/** table_expression_operations.cc
    Jeremy Barnes, 27 July, 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "table_expression_operations.h"
#include "mldb/builtin/sub_dataset.h"
#include "mldb/http/http_exception.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/types/value_description.h"
#include <functional>

using namespace std;
using namespace std::placeholders;

namespace MLDB {

/** Create a bound table expression that implements the binding of
    the given dataset.
*/
BoundTableExpression
bindDataset(std::shared_ptr<Dataset> dataset, Utf8String asName)
{
    BoundTableExpression result;
    result.dataset = dataset;
    result.asName = asName;

    // Allow us to query row information from the dataset
    result.table.getRowInfo = [=] () { return dataset->getRowInfo(); };

    // Allow the dataset to override functions
    result.table.getFunction = [=] (SqlBindingScope & context,
                                    const Utf8String & tableName,
                                    const Utf8String & functionName,
                                    const std::vector<std::shared_ptr<ExpressionValueInfo> > & args)
        -> BoundFunction 
        {
            return dataset->overrideFunction(tableName, functionName, context);
        };

    // Allow the dataset to run queries
    result.table.runQuery = [=] (const SqlBindingScope & context,
                                 const SelectExpression & select,
                                 const WhenExpression & when,
                                 const SqlExpression & where,
                                 const OrderByExpression & orderBy,
                                 ssize_t offset,
                                 ssize_t limit,
                                 const ProgressFunc & onProgress)
        -> BasicRowGenerator
        {
            return dataset->queryBasic(context, select, when, where, orderBy,
                                       offset, limit);
        };

    result.table.getChildAliases = [=] ()
        {
            std::vector<Utf8String> aliases;
            dataset->getChildAliases(aliases);
            return aliases;
        };

    return result;
}

/*****************************************************************************/
/* NAMED DATASET EXPRESSION                                                  */
/*****************************************************************************/

NamedDatasetExpression::
NamedDatasetExpression(const Utf8String& asName) : asName(asName)
{

}

/*****************************************************************************/
/* DATASET EXPRESSION                                                        */
/*****************************************************************************/

DatasetExpression::
DatasetExpression(Utf8String datasetName, Utf8String asName)
    : NamedDatasetExpression(asName), datasetName(datasetName)
{
}

DatasetExpression::
DatasetExpression(Any config, Utf8String asName)
    : NamedDatasetExpression(asName), config(std::move(config))
{
}

DatasetExpression::
~DatasetExpression()
{
}

BoundTableExpression
DatasetExpression::
bind(SqlBindingScope & context, const ProgressFunc & onProgress) const
{
    if (!config.empty()) {
        return bindDataset(context.doGetDatasetFromConfig(config), asName);
    }
    else {
        return bindDataset(context.doGetDataset(datasetName), asName.empty() ? datasetName : asName);
    }
}
    
Utf8String
DatasetExpression::
print() const
{
    return "dataset(" + datasetName + "," + asName + ")";
}

std::string
DatasetExpression::
getType() const
{
    return "dataset";
}

void
DatasetExpression::
printJson(JsonPrintingContext & context)
{
    if (!config.empty())
        context.writeJson(jsonEncode(config));
    else context.writeStringUtf8(surface);
}

Utf8String
DatasetExpression::
getOperation() const
{
    return Utf8String();
}

std::set<Utf8String>
DatasetExpression::
getTableNames() const
{
    return { asName };
}

UnboundEntities
DatasetExpression::
getUnbound() const
{
    UnboundEntities result;

    // Our table is bound, so it's not an unbound entity.  In fact, since we
    // have an instantiated dataset, nothing is unbound.

    return result;
}


/*****************************************************************************/
/* JOIN EXPRESSION                                                           */
/*****************************************************************************/

JoinExpression::
JoinExpression(std::shared_ptr<TableExpression> left,
               std::shared_ptr<TableExpression> right,
               std::shared_ptr<SqlExpression> on,
               JoinQualification qualification)
    : left(std::move(left)), right(std::move(right)), on(std::move(on)), qualification(qualification)
{
    ExcAssert(this->left);
    ExcAssert(this->right);
}

JoinExpression::
~JoinExpression()
{
}

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library
std::shared_ptr<Dataset>
(*createJoinedDatasetFn) (SqlBindingScope &,
                          std::shared_ptr<TableExpression>,
                          BoundTableExpression,
                          std::shared_ptr<TableExpression>,
                          BoundTableExpression,
                          std::shared_ptr<SqlExpression>,
                          JoinQualification);

BoundTableExpression
JoinExpression::
bind(SqlBindingScope & scope, const ProgressFunc & onProgress) const
{
    BoundTableExpression boundLeft = left->bind(scope, onProgress);
    BoundTableExpression boundRight = right->bind(scope, onProgress);

    if (boundLeft.dataset && boundRight.dataset) {
        auto ds = createJoinedDatasetFn(scope,
                                        left,
                                        std::move(boundLeft),
                                        right,
                                        std::move(boundRight),
                                        on, qualification);
        return bindDataset(ds, Utf8String());
    }
    else {
        if (boundLeft.asName.empty()
            && boundLeft.table.getChildAliases().empty()) {
            throw HttpReturnException
                (400, "Tables in joins that don't have a natural name like a "
                 "dataset name must have an AS expression (consider replacing '"
                 + left->surface + "' with '" + left->surface + " AS lhs'");
        }

        if (boundRight.asName.empty()
            && boundRight.table.getChildAliases().empty()) {
            throw HttpReturnException
                (400, "Tables in joins that don't have a natural name like a "
                 "dataset name must have an AS expression (consider replacing '"
                 + right->surface + "' with '" + right->surface + " AS rhs'");
        }

        // Use the new executor for the join
        auto pipeline = 
            PipelineElement::root(scope)
            ->join(left, std::move(boundLeft),
                   right, std::move(boundRight),
                   on, qualification, SelectExpression::STAR)
            ->bind();
   
        BoundTableExpression result;

        // Allow us to query row information from the dataset
        result.table.getRowInfo = [=] () -> std::shared_ptr<RowValueInfo>
            {
                return ExpressionValueInfo::toRow(pipeline->outputScope()->outputInfo().back());
            };
    
        // Allow the dataset to override functions
        result.table.getFunction = [=] (SqlBindingScope & context,
                                        const Utf8String & tableName,
                                        const Utf8String & functionName,
                                        const std::vector<std::shared_ptr<ExpressionValueInfo> > & args)
            -> BoundFunction 
            {
                return BoundFunction();
            };

        // Allow the dataset to run queries
        result.table.runQuery = [=] (const SqlBindingScope & context,
                                     const SelectExpression & select,
                                     const WhenExpression & when,
                                     const SqlExpression & where_,
                                     const OrderByExpression & orderBy,
                                     ssize_t offset,
                                     ssize_t limit,
                                     const ProgressFunc & onProgress)
            -> BasicRowGenerator
            {
                // Joins are detected in the outer query logic and intercepted.
                // As a result, counter-intuitively, this function is currently
                // never called.  The commented-out partial implementation is
                // kept as a starting point for whenever we use the table
                // operations for more than implementing joins, and we will need
                // to use it.
                throw HttpReturnException
                (500, "Internal logic error: joins should not require runQuery");
#if 0
                // Copy the where expression
                std::shared_ptr<SqlExpression> where = where_.shallowCopy();

                auto getRowName
                    = SqlExpression::parse("rowPath()")->bind(...);

                auto exec = [=] (ssize_t numToGenerate,
                                 SqlRowScope & rowScope,
                                 const BoundParameters & params)
                -> std::vector<NamedRowValue>
                {
                    std::vector<NamedRowValue> result;

                    auto gotElement = [&] (std::shared_ptr<PipelineResults> & res)
                        -> bool
                    {
                        auto rowName = ...;

                        // extract the rowName and the row
                        // ...
                        return true;
                    };

                    pipeline->start(params)->takeAll(gotElement);
                    
                    return result;
                };

                BasicRowGenerator result(exec, "join generator on-demand");
                return result;
#endif
            };

        result.table.getChildAliases = [=] ()
            {
                return pipeline->outputScope()->getTableNames();
            };

        return result;
    }
}

Utf8String
JoinExpression::
print() const
{
    Utf8String result = "join(";

    if (qualification == JOIN_LEFT)
        result += "LEFT,";
    else if (qualification == JOIN_RIGHT)
        result += "RIGHT,";
    else if (qualification == JOIN_FULL)
        result += "FULL,";

    result += left->print() + "," + right->print();
    if (on)
        result += "," + on->print();
    result += ")";
    return result;
}

std::string
JoinExpression::
getType() const
{
    return "join";
}

Utf8String
JoinExpression::
getOperation() const
{
    return Utf8String();
}

std::set<Utf8String>
JoinExpression::
getTableNames() const
{
    std::set<Utf8String> l = left->getTableNames();
    std::set<Utf8String> r = right->getTableNames();
    l.insert(r.begin(), r.end());
    return l;
}

UnboundEntities
JoinExpression::
getUnbound() const
{
    UnboundEntities leftUnbound = left->getUnbound();
    UnboundEntities rightUnbound = right->getUnbound();
    UnboundEntities onUnbound;
    if (on) {
        onUnbound = on->getUnbound();
    }

    auto tables = getTableNames();

    UnboundEntities result;
    result.merge(leftUnbound);
    result.merge(rightUnbound);
    result.mergeFiltered(onUnbound, tables);

    std::vector<ColumnPath> toRemove;
    for (auto & v: result.vars) {
        for (auto & t: tables) {
            if (v.first.startsWith(t)) {
                toRemove.push_back(v.first);
            }
        }
    }
    for (auto & r: toRemove)
        result.vars.erase(r);

    return result;
}


/*****************************************************************************/
/* SELECT SUBTABLE EXPRESSION                                                */
/*****************************************************************************/

/** Used when doing a select inside a FROM clause **/

SelectSubtableExpression::
SelectSubtableExpression(SelectStatement statement,
                         Utf8String asName)
    : NamedDatasetExpression(asName), statement(std::move(statement))
{

}

SelectSubtableExpression::
~SelectSubtableExpression()
{
}

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library
std::shared_ptr<Dataset> (*createSubDatasetFn) (MldbServer *, 
                                                const SubDatasetConfig &,
                                                const ProgressFunc &);

BoundTableExpression
SelectSubtableExpression::
bind(SqlBindingScope & context, const ProgressFunc & onProgress) const
{
    SubDatasetConfig config;
    config.statement = statement;
    auto ds = createSubDatasetFn(context.getMldbServer(), config, onProgress);

    return bindDataset(ds, asName);
}

Utf8String
SelectSubtableExpression::
print() const
{
    return "select(" + statement.select.print() + "," + asName + ")";
}

std::string
SelectSubtableExpression::
getType() const
{
    return "select";
}

Utf8String
SelectSubtableExpression::
getOperation() const
{
    return Utf8String();
}

std::set<Utf8String>
SelectSubtableExpression::
getTableNames() const
{
    return { asName };
}

UnboundEntities
SelectSubtableExpression::
getUnbound() const
{
    return statement.getUnbound();
}


/*****************************************************************************/
/* DATASET EXPRESSION                                                        */
/*****************************************************************************/

NoTable::
~NoTable()
{
}

BoundTableExpression
NoTable::
bind(SqlBindingScope & context, const ProgressFunc & onProgress) const
{
    return BoundTableExpression();
}
    
Utf8String
NoTable::
print() const
{
    return Utf8String("null");
}

std::string
NoTable::
getType() const
{
    return "null";
}

void
NoTable::
printJson(JsonPrintingContext & context)
{
    context.writeNull();
}

Utf8String
NoTable::
getOperation() const
{
    return Utf8String();
}

std::set<Utf8String>
NoTable::
getTableNames() const
{
    return {};
}

UnboundEntities
NoTable::
getUnbound() const
{
    UnboundEntities result;
    return result;
}

/*****************************************************************************/
/* DATASET FUNCTION EXPRESSION                                               */
/*****************************************************************************/

DatasetFunctionExpression::
DatasetFunctionExpression(Utf8String functionName, 
                          std::vector<std::shared_ptr<TableExpression>> & args,
                          std::shared_ptr<SqlExpression> options)
    : NamedDatasetExpression(""), functionName(functionName),
      args(args), options(options)
{
    setDatasetAlias(print());
}

DatasetFunctionExpression::
~DatasetFunctionExpression()
{
}

BoundTableExpression
DatasetFunctionExpression::
bind(SqlBindingScope & context, const ProgressFunc & onProgress) const
{
    std::vector<BoundTableExpression> boundArgs;
    
    size_t steps = args.size();
    ProgressState joinState(100);
    auto stepProgress = [&](uint step, const ProgressState & state) {
        joinState = (100 / steps * state.count / *state.total) + (100 / steps * step);
        return onProgress(joinState);
    };

    for (uint i = 0; i < steps; ++i) {
        auto & arg = args[i];
        auto & combinedProgress = onProgress ? std::bind(stepProgress, i, _1) : onProgress;
        boundArgs.push_back(arg->bind(context, combinedProgress));
    }

    ExpressionValue expValOptions;
    if (options) {
        expValOptions = options->constantValue();
    }

    auto fn = context.doGetDatasetFunction(functionName, boundArgs, expValOptions, asName, onProgress);

    if (!fn)
        throw HttpReturnException(400, "could not bind dataset function " + functionName);

    return fn;
}

Utf8String
DatasetFunctionExpression::
print() const
{
    Utf8String output = functionName + "(";
    for (auto arg : args)
        output += arg->print() + ",";

    output += ")" ;

    if (asName != "")
        output += " AS " + asName ;

    return output;
}

std::string
DatasetFunctionExpression::
getType() const
{
    return "datasetFunction";
}

Utf8String
DatasetFunctionExpression::
getOperation() const
{
    return Utf8String();
}

std::set<Utf8String>
DatasetFunctionExpression::
getTableNames() const
{
    return { asName };
}

UnboundEntities
DatasetFunctionExpression::
getUnbound() const
{
    UnboundEntities result;
    for (auto & a: args) {
        result.merge(a->getUnbound());
    }
    return result;
}

/*****************************************************************************/
/* ROW TABLE EXPRESSION                                                      */
/*****************************************************************************/

RowTableExpression::
RowTableExpression(std::shared_ptr<SqlExpression> expr,
                   Utf8String asName,
                   Style style)
    : expr(std::move(expr)), asName(std::move(asName)), style(style)
{
    ExcAssert(this->expr);
}

RowTableExpression::
~RowTableExpression()
{
}

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library
std::vector<NamedRowValue>
(*querySubDatasetFn) (MldbServer * server,
                      std::vector<NamedRowValue> rows,
                      const SelectExpression & select,
                      const WhenExpression & when,
                      const SqlExpression & where,
                      const OrderByExpression & orderBy,
                      const TupleExpression & groupBy,
                      const std::shared_ptr<SqlExpression> having,
                      const std::shared_ptr<SqlExpression> named,
                      uint64_t offset,
                      int64_t limit,
                      const Utf8String & tableAlias,
                      bool allowMultiThreading) = nullptr;

BoundTableExpression
RowTableExpression::
bind(SqlBindingScope & context, const ProgressFunc & onProgress) const
{
    ExcAssert(querySubDatasetFn);

    MldbServer * server = context.getMldbServer();
    ExcAssert(server);

    auto boundExpr = expr->bind(context);

    // infer value expression from row, especially if there is only
    // one column available and thus its value type is simple
    auto inputInfo = ExpressionValueInfo::toRow(boundExpr.info);

    std::shared_ptr<ExpressionValueInfo> valueInfo;
    
    switch (style) {
    case COLUMNS:
        if (inputInfo->getSchemaCompleteness() == SCHEMA_CLOSED) {
            // Try to infer a type for the column column.  This is done by
            // getting a type that covers (can represent) the types of all
            // of the columns in the dataset.
            auto knownColumns = inputInfo->getKnownColumns();
            if (!knownColumns.empty()) {
                valueInfo = knownColumns[0].valueInfo;
                for (size_t i = 1;  i < knownColumns.size();  ++i) {
                    valueInfo = ExpressionValueInfo
                        ::getCovering(valueInfo, knownColumns[i].valueInfo);
                }
            }
        }
        if (!valueInfo)
            valueInfo.reset(new AnyValueInfo());
        break;
    case ATOMS:
        if (inputInfo->getSchemaCompleteness() == SCHEMA_CLOSED) {
            // Try to infer a type for the column column
            auto knownAtoms = inputInfo->getKnownAtoms();
            if (!knownAtoms.empty()) {
                valueInfo = knownAtoms[0].valueInfo;
                for (size_t i = 1;  i < knownAtoms.size();  ++i) {
                    valueInfo = ExpressionValueInfo
                        ::getCovering(valueInfo, knownAtoms[i].valueInfo);
                }
            }
        }
        if (!valueInfo)
            valueInfo.reset(new AtomValueInfo());
        break;
    default:
        throw HttpReturnException(500, "Invalid row_dataset style");
    }
    
    std::vector<KnownColumn> knownColumns;
    knownColumns.emplace_back(ColumnPath("value"),
                              valueInfo,
                              COLUMN_IS_DENSE);
    knownColumns.emplace_back(ColumnPath("column"),
                              std::make_shared<PathValueInfo>(),
                              COLUMN_IS_DENSE);
    auto info = std::make_shared<RowValueInfo>(knownColumns);

    BoundTableExpression result;
    result.dataset = nullptr;
    result.asName = asName;

    // Allow us to query row information from the dataset
    result.table.getRowInfo = [=] () { return info; };
    
    // Allow the dataset to override functions
    result.table.getFunction = [=] (SqlBindingScope & context,
                                    const Utf8String & tableName,
                                    const Utf8String & functionName,
                                    const std::vector<std::shared_ptr<ExpressionValueInfo> > & args)
        -> BoundFunction 
        {
            return BoundFunction();
        };

    static const PathElement valueName("value");
    static const PathElement columnNameName("column");

    // Allow the dataset to run queries
    result.table.runQuery = [=] (const SqlBindingScope & context,
                                 const SelectExpression & select,
                                 const WhenExpression & when,
                                 const SqlExpression & where_,
                                 const OrderByExpression & orderBy,
                                 ssize_t offset,
                                 ssize_t limit,
                                 const ProgressFunc & onProgress)
        -> BasicRowGenerator
        {
            // Copy the where expression
            std::shared_ptr<SqlExpression> where = where_.shallowCopy();

            auto exec = [=] (ssize_t numToGenerate,
                             SqlRowScope & rowScope,
                             const BoundParameters & params)
                -> std::vector<NamedRowValue>
            {
                // 1.  Get the row
                ExpressionValue row = boundExpr(rowScope, GET_LATEST);

                if (!row.isRow()) {
                    throw HttpReturnException
                        (400, "Argument to row_dataset must be a row, not a "
                         "scalar or NULL (got " + jsonEncodeStr(row) + ")");
                }

                // 2.  Put it in a sub dataset
                std::vector<NamedRowValue> rows;
                rows.reserve(row.rowLength());
                int n = 0;

                if (style == ATOMS) {
                    auto onAtom = [&] (Path & columnName,
                                       CellValue & val,
                                       Date ts)
                        {
                            NamedRowValue row;
                            row.rowHash = row.rowName = ColumnPath(to_string(n++));

                            row.columns.emplace_back(columnNameName,
                                                     ExpressionValue(CellValue(std::move(columnName)), ts));
                            row.columns.emplace_back(valueName, ExpressionValue(std::move(val), ts));

                            rows.emplace_back(std::move(row));

                            return true;
                        };

                    row.forEachAtomDestructive(onAtom);
                }
                else { // style == ROWS
                    auto onExpression = [&] (PathElement & columnName,
                                             ExpressionValue & val)
                    {
                        NamedRowValue row;
                        row.rowHash = row.rowName = ColumnPath(to_string(n++));

                        row.columns.emplace_back
                            (columnNameName,
                             ExpressionValue(CellValue(std::move(columnName)),
                                             val.getEffectiveTimestamp()));
                        
                        row.columns.emplace_back(valueName, std::move(val));

                        rows.emplace_back(std::move(row));

                        return true;
                    };

                    row.forEachColumnDestructive(onExpression);
                }

                return querySubDatasetFn(server, std::move(rows),
                                         select, when, *where, orderBy,
                                         TupleExpression(),
                                         SqlExpression::TRUE,
                                         SqlExpression::parse("rowPath()"),
                                         offset, limit, "" /* dataset alias */,
                                         false /* allow multithreading */);
                };

            BasicRowGenerator result(exec, "row table expression generator");
            return result;
        };

    result.table.getChildAliases = [=] ()
        {
            std::vector<Utf8String> aliases;
            return aliases;
        };

    return result;
}
    
Utf8String
RowTableExpression::
print() const
{
    return "row_table(" + expr->print() + ")";
}

void
RowTableExpression::
printJson(JsonPrintingContext & context)
{
    context.writeStringUtf8(surface);
}

std::string
RowTableExpression::
getType() const
{
    return "row_table";
}

Utf8String
RowTableExpression::
getOperation() const
{
    return expr->print();
}

std::set<Utf8String>
RowTableExpression::
getTableNames() const
{
    return { asName };
}

UnboundEntities
RowTableExpression::
getUnbound() const
{
    return expr->getUnbound();
}



} // namespace MLDB

