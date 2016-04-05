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
/* ROW EXPRESSION MLDB CONTEXT                                               */
/*****************************************************************************/

SqlExpressionMldbContext::
SqlExpressionMldbContext(const MldbServer * mldb)
    : mldb(const_cast<MldbServer *>(mldb))
{
    ExcAssert(mldb);
}

BoundFunction
SqlExpressionMldbContext::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                          argScope);
}

std::shared_ptr<Function>
SqlExpressionMldbContext::
doGetFunctionEntity(const Utf8String & functionName)
{
    return mldb->functions->getExistingEntity(functionName.rawString());
}

std::shared_ptr<Dataset>
SqlExpressionMldbContext::
doGetDataset(const Utf8String & datasetName)
{
    return mldb->datasets->getExistingEntity(datasetName.rawString());
}

std::shared_ptr<Dataset>
SqlExpressionMldbContext::
doGetDatasetFromConfig(const Any & datasetConfig)
{
    return obtainDataset(mldb, datasetConfig.convert<PolyConfig>());
}

// defined in table_expression_operations.cc
BoundTableExpression
bindDataset(std::shared_ptr<Dataset> dataset, Utf8String asName);

TableOperations
SqlExpressionMldbContext::
doGetTable(const Utf8String & tableName)
{
    return bindDataset(doGetDataset(tableName), Utf8String()).table;
}

MldbServer *
SqlExpressionMldbContext::
getMldbServer() const
{
    return mldb;
}



/*****************************************************************************/
/* ROW EXPRESSION DATASET CONTEXT                                            */
/*****************************************************************************/

SqlExpressionDatasetContext::
SqlExpressionDatasetContext(std::shared_ptr<Dataset> dataset, const Utf8String& alias)
    : SqlExpressionMldbContext(dataset->server), dataset(*dataset), alias(alias)
{
     dataset->getChildAliases(childaliases);
}

SqlExpressionDatasetContext::
SqlExpressionDatasetContext(const Dataset & dataset, const Utf8String& alias)
    : SqlExpressionMldbContext(dataset.server), dataset(dataset), alias(alias)
{
    dataset.getChildAliases(childaliases);
}

SqlExpressionDatasetContext::
SqlExpressionDatasetContext(const BoundTableExpression& boundDataset)
: SqlExpressionMldbContext(boundDataset.dataset->server), dataset(*boundDataset.dataset), alias(boundDataset.asName)
{
    boundDataset.dataset->getChildAliases(childaliases);
}

#if 0
Utf8String 
SqlExpressionDatasetContext::
resolveTableName(const Utf8String& variable, Utf8String& resolvedTableName) const;
{
    if (variable.empty())
        return Utf8String();

    auto it = variable.begin();
    if (*it == '"') {
        //identifier starts in quote 
        //we want to remove the quotes if they are extraneous (now that we have context)
        //and we want to remove the quotes around the variable's name (if any)
        Utf8String quoteString = "\"";
        ++it;
        it = std::search(it, variable.end(), quoteString.begin(), quoteString.end());
        if (it != variable.end()) {
            auto next = it;
            next ++;
            if (next != variable.end() && *next == '.') {
                //Found something like "text".
                //which is an explicit dataset name
                //remove the quotes around the table name if there is no ambiguity (i.e, a "." inside)      
                Utf8String tableName(variable.begin(), next);          
                if (tableName.find(".") == tableName.end()) 
                    tableName = removeQuotes(tableName);

                //remove the quote aroud the variable's name
                next++;
                if (next != variable.end()) {
                    Utf8String shortVariableName(next, variable.end());
                    shortVariableName = tableName + "." + removeQuotes(shortVariableName);
                    resolvedTableName = std::move(tableName);
                    return shortVariableName;
                }            
            }
        }       
    }
    else {


        Utf8String dotString = ".";

        do  {
            it = std::search(it, variable.end(), dotString.begin(), dotString.end());

            if (it != variable.end()) {

                //check if this portion of the identifier correspond to a dataset name within this context
                Utf8String tableName(variable.begin(), it);
                for (auto& datasetName: childaliases) {

                    if (tableName == datasetName) {
                        resolvedTableName = std::move(tableName);
                        if (datasetName.find('.') != datasetName.end()) {
                            //we need to enclose it in quotes to resolve any potential ambiguity
                            Utf8String quotedVariableName(++it, variable.end());
                            quotedVariableName = "\"" + tableName + "\"" + "." + removeQuotes(quotedVariableName);
                            return quotedVariableName;
                        }
                        else {
                            //keep it unquoted 
                            return variable;
                        }
                    }
                }

                ++it;
            } 
        } while (it != variable.end());
    }

    return variable;
}
#endif

Utf8String 
SqlExpressionDatasetContext::
resolveTableName(const Utf8String& variable) const
{
    Utf8String resolvedTableName;
    return resolveTableName(variable, resolvedTableName);
}

ColumnGetter
SqlExpressionDatasetContext::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{   
    Utf8String simplifiedVariableName;
    
    if (!childaliases.empty())
        simplifiedVariableName = resolveTableName(columnName);
    else
        simplifiedVariableName
            = removeTableName(alias, columnName).toSimpleName();

    ColumnName columnName(simplifiedVariableName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = context.as<RowContext>();

                const ExpressionValue * fromOutput
                    = searchRow(row.row.columns, columnName, filter, storage);
                if (fromOutput)
                    return *fromOutput;

                return storage = std::move(ExpressionValue::null(Date::negativeInfinity()));
            },
            std::make_shared<AtomValueInfo>()};
}

BoundFunction
SqlExpressionDatasetContext::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{

    Utf8String resolvedTableName = tableName;
    Utf8String resolvedFunctionName = functionName;

    // TO RESOLVE BEFORE MERGE
#if 0
    //Dont call "resolveTableName" for function because it puts quotes to resolve variables ambiguity
    if (tableName.empty()) {
        resolvedFunctionName = removeTableName(alias, functionName).toSimpleName();
        if (resolvedFunctionName != functionName)
            resolvedTableName = alias;
    }
#endif

    // First, let the dataset either override or implement the function
    // itself.
    auto fnoverride = dataset.overrideFunction(resolvedTableName, resolvedFunctionName, argScope);
    if (fnoverride)
        return fnoverride;

    if (resolvedFunctionName == "rowName") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowContext>();
                    return ExpressionValue(row.row.rowName.toUtf8String(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()
                };
    }

    if (resolvedFunctionName == "rowHash") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowContext>();
                    return ExpressionValue(row.row.rowHash,
                                           Date::negativeInfinity());
                },
                std::make_shared<Uint64ValueInfo>()
                };
    }

    /* columnCount function: return number of columns with explicit values set
       in the current row.
    */
    if (resolvedFunctionName == "columnCount") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & row = context.as<RowContext>();
                    ML::Lightweight_Hash_Set<ColumnHash> columns;
                    Date ts = Date::negativeInfinity();
                    
                    for (auto & c: row.row.columns) {
                        columns.insert(std::get<0>(c));
                        ts.setMax(std::get<2>(c));
                    }
                    
                    return ExpressionValue(columns.size(), ts);
                },
                std::make_shared<Uint64ValueInfo>()};
    }

    return SqlBindingScope::doGetFunction(resolvedTableName, resolvedFunctionName, args, argScope);
}

ColumnGetter
SqlExpressionDatasetContext::
doGetBoundParameter(const Utf8String & paramName)
{
    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                ExcAssert(canIgnoreIfExactlyOneValue(filter));

                auto & row = context.as<RowContext>();
                if (!row.params || !*row.params)
                    throw HttpReturnException(400, "Bound parameters requested but none passed");
                return storage = std::move((*row.params)(paramName));
            },
            std::make_shared<AnyValueInfo>()};
}

GetAllColumnsOutput
SqlExpressionDatasetContext::
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
        if (!tableName.empty() && !childaliases.empty()
            && !inputColumnName.startsWith(tableName)) {
            return ColumnName();
        }

        return keep(inputColumnName);
    };

    std::unordered_map<ColumnHash, ColumnName> index;
    std::vector<KnownColumn> columnsWithInfo;
    bool allWereKept = true;
    bool noneWereRenamed = true;

    vector<ColumnName> columnsNeedingInfo;

    for (auto & columnName: columns) {
        ColumnName outputName(filterColumnName(columnName.toUtf8String()));
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

    std::function<ExpressionValue (const SqlRowScope &)> exec;

    if (allWereKept && noneWereRenamed) {
        // We can pass right through; we have a select *

        exec = [=] (const SqlRowScope & context) -> ExpressionValue
            {
                auto & row = context.as<RowContext>();

                // TODO: if one day we are able to prove that this is
                // the only expression that touches the row, we could
                // move it into place
                return row.row.columns;
            };
    }
    else {
        // Some are excluded or renamed; we need to go one by one
        exec = [=] (const SqlRowScope & context)
            {
                auto & row = context.as<RowContext>();

                RowValue result;

                for (auto & c: row.row.columns) {
                    auto it = index.find(std::get<0>(c));
                    if (it == index.end()) {
                        continue;
                    }
                
                    result.emplace_back(it->second, std::get<1>(c), std::get<2>(c));
                }

                return std::move(result);
            };

    }
    
    GetAllColumnsOutput result;
    result.exec = exec;
    result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                 SCHEMA_CLOSED);
    return result;
}

GenerateRowsWhereFunction
SqlExpressionDatasetContext::
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
SqlExpressionDatasetContext::
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

Utf8String 
SqlExpressionDatasetContext::
doResolveTableName(const Utf8String & fullVariableName, Utf8String &tableName) const
{
    if (!childaliases.empty()) {
        return resolveTableName(fullVariableName, tableName);
    }
    else {
        Utf8String simplifiedVariableName
            = removeTableName(alias, fullVariableName).toSimpleName();
        if (simplifiedVariableName != fullVariableName)
            tableName = alias;
        return std::move(simplifiedVariableName);
    }
}   

/*****************************************************************************/
/* ROW EXPRESSION ORDER BY CONTEXT                                           */
/*****************************************************************************/

ColumnGetter
SqlExpressionOrderByContext::
doGetColumn(const Utf8String & tableName, const ColumnName & columnName)
{
    /** An order by clause can read through both what was selected and what
        was in the underlying row.  So we first look in what was selected,
        and then fall back to the underlying row.
    */

    ColumnName columnName(columnName);

    auto innerGetVariable
        = ReadThroughBindingContext::doGetColumn(tableName, columnName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = context.as<RowContext>();
                
                const ExpressionValue * fromOutput
                    = searchRow(row.output.columns, columnName, filter, storage);
                if (fromOutput)
                    return *fromOutput;
                
                return innerGetVariable(context, storage);
            },
            std::make_shared<AtomValueInfo>()};
}

} // namespace MLDB
} // namespace Datacratic

