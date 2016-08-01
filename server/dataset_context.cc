/** dataset_context.cc
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Context to bind a row expression to a dataset.
*/

#include "mldb/server/dataset_context.h"
#include "mldb/core/dataset.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/function_collection.h"
#include "mldb/server/dataset_collection.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/sql/sql_utils.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* SQL EXPRESSION MLDB SCOPE                                                 */
/*****************************************************************************/

SqlExpressionMldbScope::
SqlExpressionMldbScope(const MldbServer * mldb)
    : mldb(const_cast<MldbServer *>(mldb))
{
    ExcAssert(mldb);
}

BoundFunction
SqlExpressionMldbScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    // User functions don't live in table scope
    if (tableName.empty()) {
        // 1.  Try to get a function entity
        auto fn = mldb->functions->tryGetExistingEntity(functionName.rawString());

        if (fn) {
            // We found one.  Now wrap it up as a normal function.
            if (args.size() > 1)
                throw HttpReturnException(400, "User function " + functionName
                                          + " expected a single row { } argument");

            std::shared_ptr<FunctionApplier> applier;

            if (args.empty()) {
                applier.reset
                    (fn->bind(argScope,
                              std::make_shared<RowValueInfo>(std::vector<KnownColumn>()))
                     .release());
            }
            else {
                if (!args[0].info->isRow()) {
                    throw HttpReturnException(400, "User function " + functionName
                                              + " expects a row argument ({ }), "
                                              + "got " + args[0].expr->print() );
                }
                applier.reset(fn->bind(argScope, ExpressionValueInfo::toRow(args[0].info))
                              .release());
            }
        
            auto exec = [=] (const std::vector<ExpressionValue> & args,
                             const SqlRowScope & scope)
                -> ExpressionValue
                {
                    if (args.empty()) {
                        return applier->apply(ExpressionValue());
                    }
                    else {
                        return applier->apply(args[0]);
                    }
                };

            return BoundFunction(exec, applier->info.output);
        }
    }

    return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                          argScope);
}

std::shared_ptr<Dataset>
SqlExpressionMldbScope::
doGetDataset(const Utf8String & datasetName)
{
    return mldb->datasets->getExistingEntity(datasetName.rawString());
}

std::shared_ptr<Dataset>
SqlExpressionMldbScope::
doGetDatasetFromConfig(const Any & datasetConfig)
{
    return obtainDataset(mldb, datasetConfig.convert<PolyConfig>());
}

// defined in table_expression_operations.cc
BoundTableExpression
bindDataset(std::shared_ptr<Dataset> dataset, Utf8String asName);

TableOperations
SqlExpressionMldbScope::
doGetTable(const Utf8String & tableName)
{
    return bindDataset(doGetDataset(tableName), Utf8String()).table;
}

MldbServer *
SqlExpressionMldbScope::
getMldbServer() const
{
    return mldb;
}

ColumnGetter
SqlExpressionMldbScope::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{
    throw HttpReturnException(
        400,
        "Cannot read column \"" + columnName.toUtf8String()
        + "\" with no FROM clause.");
}

GetAllColumnsOutput
SqlExpressionMldbScope::
doGetAllColumns(const Utf8String & tableName,
                function<ColumnName (const ColumnName &)> keep)
{
    throw HttpReturnException(400, "Cannot use wildcards with no FROM clause.");
}

/*****************************************************************************/
/* ROW EXPRESSION DATASET CONTEXT                                            */
/*****************************************************************************/

RowName
SqlExpressionDatasetScope::RowScope::
getRowName() const
{
    if (rowName) return *rowName;
    else {
        ExcAssert(row);
        return row->rowName;
    }
}

const RowName &
SqlExpressionDatasetScope::RowScope::
getRowName(RowName & storage) const
{
    if (rowName) return *rowName;
    else return row->rowName;
}

RowHash
SqlExpressionDatasetScope::RowScope::
getRowHash() const
{
    if (rowName) return *rowName;
    else return row->rowHash;
}

const ExpressionValue &
SqlExpressionDatasetScope::RowScope::
getColumn(const ColumnName & columnName,
          const VariableFilter & filter,
          ExpressionValue & storage,
          ssize_t knownOffset) const
{
    if (expr) {
        const ExpressionValue * val
            = expr->tryGetNestedColumn(columnName, storage, filter);
        if (!val)
            return storage = ExpressionValue::null(Date::negativeInfinity());
        else return *val;
    }
    else {
        const ExpressionValue * fromOutput
            = searchRow(row->columns, columnName, filter, storage);
        if (fromOutput)
            return *fromOutput;
        
        return storage = std::move(ExpressionValue::null(Date::negativeInfinity()));
    }
}

ExpressionValue
SqlExpressionDatasetScope::RowScope::
getColumnCount() const
{
    ML::Lightweight_Hash_Set<ColumnHash> columns;
    Date ts = Date::negativeInfinity();

    if (row) {
        
        for (auto & c: row->columns) {
            columns.insert(std::get<0>(c));
            ts.setMax(std::get<2>(c));
        }
    }
    else {
        return ExpressionValue(expr->getUniqueAtomCount(),
                               expr->getEffectiveTimestamp());
    }
    
    return ExpressionValue(columns.size(), ts);
}
        
const ExpressionValue &
SqlExpressionDatasetScope::RowScope::
getFilteredValue(const VariableFilter & filter,
                 ExpressionValue & storage) const
{
    if (row) {
        // TODO: if one day we are able to prove that this is
        // the only expression that touches the row, we could
        // move it into place
        ExpressionValue val(row->columns);
        return storage = val.getFilteredDestructive(filter);
    }
    else {
        return expr->getFiltered(filter, storage);
    }
}

ExpressionValue
SqlExpressionDatasetScope::RowScope::
getReshaped(const std::unordered_map<ColumnHash, ColumnName> & index,
            const VariableFilter & filter) const
{
    RowValue result;

    if (row) {
        for (auto & c: row->columns) {
            auto it = index.find(std::get<0>(c));
            if (it == index.end()) {
                continue;
            }
            
            result.emplace_back(it->second, std::get<1>(c), std::get<2>(c));
        }
    }
    else {
        auto onAtom = [&] (const Path & columnName,
                           const Path & prefix,
                           const CellValue & val,
                           Date ts)
            {
                ColumnName newColumnName = prefix + columnName;
                auto it = index.find(newColumnName);
                if (it == index.end())
                    return true;
                result.emplace_back(it->second, val, ts);
                return true;
            };

        expr->forEachAtom(onAtom);
    }

    ExpressionValue val(std::move(result));
    return  val.getFilteredDestructive(filter);
}

SqlExpressionDatasetScope::
SqlExpressionDatasetScope(std::shared_ptr<Dataset> dataset, const Utf8String& alias)
    : SqlExpressionMldbScope(dataset->server), dataset(*dataset), alias(alias)
{
     dataset->getChildAliases(childaliases);
}

SqlExpressionDatasetScope::
SqlExpressionDatasetScope(const Dataset & dataset, const Utf8String& alias)
    : SqlExpressionMldbScope(dataset.server), dataset(dataset), alias(alias)
{
    dataset.getChildAliases(childaliases);
}

SqlExpressionDatasetScope::
SqlExpressionDatasetScope(const BoundTableExpression& boundDataset)
    : SqlExpressionMldbScope(boundDataset.dataset->server),
      dataset(*boundDataset.dataset),
      alias(boundDataset.asName)
{
    boundDataset.dataset->getChildAliases(childaliases);
}

ColumnGetter
SqlExpressionDatasetScope::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{   
    ColumnName simplified;
    if (tableName.empty() && columnName.size() > 1) {
        if (!alias.empty() && columnName.startsWith(alias)) {
            simplified = columnName.removePrefix();
        }
    }
    if (simplified.empty())
        simplified = columnName;

    //cerr << "doGetColumn: " << tableName << " " << columnName << endl;
    //cerr << columnName.size() << endl;
    //cerr << "alias " << alias << endl;
    //for (auto & c: childaliases)
    //    cerr << "  child " << c << endl;
    //cerr << "simplified = " << simplified << endl;

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = context.as<RowScope>();
                return row.getColumn(simplified, filter, storage);
            },
            std::make_shared<AtomValueInfo>()};
}


BoundFunction
SqlExpressionDatasetScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    // First, let the dataset either override or implement the function
    // itself.
    auto fnoverride = dataset.overrideFunction(tableName, functionName, argScope);
    if (fnoverride)
        return fnoverride;

    if (functionName == "rowName") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowScope>();
                    return ExpressionValue(row.getRowName().toUtf8String(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
                };
    }

    if (functionName == "rowPath") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowScope>();
                    return ExpressionValue(CellValue(row.getRowName()),
                                           Date::negativeInfinity());
                },
                std::make_shared<PathValueInfo>()
                };
    }

    if (functionName == "rowHash") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowScope>();
                    return ExpressionValue(row.getRowHash().hash(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Uint64ValueInfo>()
                };
    }

    // Look for a derived function.  We do it here so that it's only done
    // if the function can't be found higher up.
    auto fnderived
        = getDatasetDerivedFunction(tableName, functionName, args, argScope,
                                    *this, "row");

    if (fnderived)
        return fnderived;

    /* columnCount function: return number of columns with explicit values set
       in the current row.
    */
    if (functionName == "columnCount") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowScope>();
                    return row.getColumnCount();
                },
                std::make_shared<Uint64ValueInfo>()};
    }

    return SqlExpressionMldbScope
        ::doGetFunction(tableName, functionName, args, argScope);
}

ColumnGetter
SqlExpressionDatasetScope::
doGetBoundParameter(const Utf8String & paramName)
{
    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                ExcAssert(canIgnoreIfExactlyOneValue(filter));

                auto & row = context.as<RowScope>();
                if (!row.params || !*row.params)
                    throw HttpReturnException(400, "Bound parameters requested but none passed");
                return storage = std::move((*row.params)(paramName));
            },
            std::make_shared<AnyValueInfo>()};
}

GetAllColumnsOutput
SqlExpressionDatasetScope::
doGetAllColumns(const Utf8String & tableName,
                std::function<ColumnName (const ColumnName &)> keep)
{
    if (!tableName.empty()
        && std::find(childaliases.begin(), childaliases.end(), tableName)
            == childaliases.end()
        && tableName != alias)
        throw HttpReturnException(400, "Unknown dataset " + tableName);

    auto columns = dataset.getMatrixView()->getColumnNames();

    auto filterColumnName = [&] (const ColumnName & inputColumnName)
        -> ColumnName
    {
        if (!tableName.empty() && !childaliases.empty()) {
            // We're in a join.  The columns will all be prefixed with their
            // child table name, but we don't want to use that.
            // eg, in x inner join y, we will have variables like
            // x.a and y.b, but when we select them we want them to be
            // like a and b.
            if (!inputColumnName.startsWith(tableName)) {
                return ColumnName();
            }
            // Otherwise, check if we need it
            ColumnName result = keep(inputColumnName);
            return std::move(result);
        }

        return keep(inputColumnName);
    };

    std::unordered_map<ColumnHash, ColumnName> index;
    std::vector<KnownColumn> columnsWithInfo;
    bool allWereKept = true;
    bool noneWereRenamed = true;

    vector<ColumnName> columnsNeedingInfo;

    for (auto & columnName: columns) {
        ColumnName outputName(filterColumnName(columnName));

        if (outputName == ColumnName()) {
            allWereKept = false;
            continue;
        }

        if (outputName != columnName)
            noneWereRenamed = false;
        columnsNeedingInfo.push_back(columnName);

        index[columnName] = outputName;

        // Ask the dataset to describe this column later, null ptr for now
        columnsWithInfo.emplace_back(outputName, nullptr,
                                     COLUMN_IS_DENSE);

        // Change the name to the output name
        //columnsWithInfo.back().columnName = outputName;
    }

    auto allInfo = dataset.getKnownColumnInfos(columnsNeedingInfo);

    // Now put in the value info
    for (unsigned i = 0;  i < allInfo.size();  ++i) {
        ColumnName outputName = columnsWithInfo[i].columnName;
        columnsWithInfo[i] = allInfo[i];
        columnsWithInfo[i].columnName = std::move(outputName);
    }

    std::function<ExpressionValue (const SqlRowScope &, const VariableFilter &)> exec;

    if (allWereKept && noneWereRenamed) {
        // We can pass right through; we have a select *

        exec = [=] (const SqlRowScope & context,
                    const VariableFilter & filter) -> ExpressionValue
            {
                ExpressionValue storage;
                auto & row = context.as<RowScope>();
                return row.getFilteredValue(filter, storage);
            };
    }
    else {
        // Some are excluded or renamed; we need to go one by one
        exec = [=] (const SqlRowScope & context, const VariableFilter & filter)
            {
                auto & row = context.as<RowScope>();
                return row.getReshaped(index, filter);
            };

    }
    
    GetAllColumnsOutput result;
    result.exec = exec;
    result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                 SCHEMA_CLOSED);
    return result;
}

GenerateRowsWhereFunction
SqlExpressionDatasetScope::
doCreateRowsWhereGenerator(const SqlExpression & where,
                           ssize_t offset,
                           ssize_t limit)
{
    auto res = dataset.generateRowsWhere(*this, alias, where, offset, limit);
    if (!res)
        throw HttpReturnException(500, "Dataset returned null generator",
                                  "datasetType", ML::type_name(dataset));
    return res;
}

ColumnFunction
SqlExpressionDatasetScope::
doGetColumnFunction(const Utf8String & functionName)
{
    if (functionName == "columnName") {
        return {[=] (const ColumnName & columnName,
                     const std::vector<ExpressionValue> & args)
                {
                    return ExpressionValue(columnName.toUtf8String(),
                                           Date::negativeInfinity());
                }};
    }

    if (functionName == "columnHash") {
        return {[=] (const ColumnName & columnName,
                     const std::vector<ExpressionValue> & args)
                {
                    return ExpressionValue(columnName.hash(),
                                           Date::negativeInfinity());
                }};
    }

    if (functionName == "rowCount") {
        return {[=] (const ColumnName & columnName,
                     const std::vector<ExpressionValue> & args)
                {
                    return ExpressionValue
                        (dataset.getColumnIndex()->getColumnRowCount(columnName),
                         Date::notADate());
                }};
    }

    return nullptr;
}

ColumnName
SqlExpressionDatasetScope::
doResolveTableName(const ColumnName & fullColumnName, Utf8String &tableName) const
{
    if (!childaliases.empty()) {
        for (auto & a: childaliases) {
            if (fullColumnName.startsWith(a)) {
                tableName = a;
                return fullColumnName.removePrefix();
            }
        }
    }
    else {
        if (!alias.empty() && fullColumnName.startsWith(alias)) {
            tableName = alias;
            return fullColumnName.removePrefix();
        }
    }
    tableName = Utf8String();
    return fullColumnName;
}   


/*****************************************************************************/
/* ROW EXPRESSION ORDER BY CONTEXT                                           */
/*****************************************************************************/

ColumnGetter
SqlExpressionOrderByScope::
doGetColumn(const Utf8String & tableName, const ColumnName & columnName)
{
    /** An order by clause can read through both what was selected and what
        was in the underlying row.  So we first look in what was selected,
        and then fall back to the underlying row.
    */

    auto innerGetVariable
        = ReadThroughBindingScope::doGetColumn(tableName, columnName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = context.as<RowScope>();
                
                const ExpressionValue * fromOutput
                    = searchRow(row.output.columns, columnName.front(),
                                filter, storage);
                if (fromOutput) {
                    if (columnName.size() == 1) {
                        return *fromOutput;
                    }
                    else {
                        ColumnName tail = columnName.removePrefix();
                        auto value = fromOutput->getNestedColumn(tail, filter);
                        if (!value.empty())
                            return storage = std::move(value);
                    }
                }
                
                return innerGetVariable(context, storage);
            },
            std::make_shared<AtomValueInfo>()};
}

} // namespace MLDB
} // namespace Datacratic

