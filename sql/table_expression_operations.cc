/** table_expression_operations.cc
    Jeremy Barnes, 27 July, 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

*/

#include "table_expression_operations.h"
#include "mldb/builtin/joined_dataset.h"
#include "mldb/builtin/sub_dataset.h"
#include "mldb/http/http_exception.h"

using namespace std;

namespace Datacratic {
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
                                 ssize_t limit)
        -> BasicRowGenerator
        {
            return dataset->queryBasic(context, select, when, where, orderBy,
                                       offset, limit);
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
bind(SqlBindingScope & context) const
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
std::shared_ptr<Dataset> (*createJoinedDatasetFn) (MldbServer *, const JoinedDatasetConfig &);

BoundTableExpression
JoinExpression::
bind(SqlBindingScope & context) const
{
    JoinedDatasetConfig config;
    config.left = left;
    config.right = right;
    config.on = on;

    config.qualification = qualification;
    auto ds = createJoinedDatasetFn(context.getMldbServer(), config);

    return bindDataset(ds, Utf8String());
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

    std::vector<ColumnName> toRemove;
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
std::shared_ptr<Dataset> (*createSubDatasetFn) (MldbServer *, const SubDatasetConfig &);

BoundTableExpression
SelectSubtableExpression::
bind(SqlBindingScope & context) const
{
    SubDatasetConfig config;
    config.statement = statement;
    auto ds = createSubDatasetFn(context.getMldbServer(), config);

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
bind(SqlBindingScope & context) const
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
bind(SqlBindingScope & context) const
{
    std::vector<BoundTableExpression> boundArgs;
    for (auto arg : args)
        boundArgs.push_back(arg->bind(context));

    ExpressionValue expValOptions;
    if (options) {
        expValOptions = options->constantValue();
    }

    auto fn = context.doGetDatasetFunction(functionName, boundArgs, expValOptions, asName);

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
        cerr << "getting unbound for arg " << a->print() << endl;
        result.merge(a->getUnbound());
    }

    cerr << "unbound is " << jsonEncode(result) << endl;

    return result;
}

/*****************************************************************************/
/* ROW TABLE EXPRESSION                                                      */
/*****************************************************************************/

RowTableExpression::
RowTableExpression(std::shared_ptr<SqlExpression> expr,
                   Utf8String asName)
    : expr(std::move(expr)), asName(std::move(asName))
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
                      std::vector<MatrixNamedRow> rows,
                      const SelectExpression & select,
                      const WhenExpression & when,
                      const SqlExpression & where,
                      const OrderByExpression & orderBy,
                      const TupleExpression & groupBy,
                      const SqlExpression & having,
                      const SqlExpression & named,
                      uint64_t offset,
                      int64_t limit,
                      const Utf8String & tableAlias,
                      bool allowMultiThreading) = nullptr;

BoundTableExpression
RowTableExpression::
bind(SqlBindingScope & context) const
{
    ExcAssert(querySubDatasetFn);

    MldbServer * server = context.getMldbServer();
    ExcAssert(server);

    auto boundExpr = expr->bind(context);

    // TODO: infer value expression from row, especially if there is only
    // one column available and thus its value type is simple

    std::vector<KnownColumn> knownColumns;
    knownColumns.emplace_back(ColumnName("value"),
                              std::make_shared<AnyValueInfo>(),
                              COLUMN_IS_DENSE);
    knownColumns.emplace_back(ColumnName("column"),
                              std::make_shared<AnyValueInfo>(),
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

    static const ColumnName valueName("value");
    static const ColumnName columnNameName("column");

    // Allow the dataset to run queries
    result.table.runQuery = [=] (const SqlBindingScope & context,
                                 const SelectExpression & select,
                                 const WhenExpression & when,
                                 const SqlExpression & where_,
                                 const OrderByExpression & orderBy,
                                 ssize_t offset,
                                 ssize_t limit)
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
                
                // 2.  Put it in a sub dataset
                std::vector<MatrixNamedRow> rows;
                rows.reserve(row.rowLength());
                int n = 0;

                auto onColumn = [&] (const Coords & columnName,
                                     const Coords & prefix,
                                     const CellValue & cell,
                                     Date ts)
                {
                    MatrixNamedRow row;
                    row.rowHash = row.rowName = ColumnName(to_string(n++));
                    row.columns.emplace_back(columnNameName,
                                             (prefix + columnName)
                                                 .toUtf8String(),
                                             ts);
                    row.columns.emplace_back(valueName,
                                             cell,
                                             ts);
                    rows.emplace_back(std::move(row));

                    return true;
                };

                row.forEachAtom(onColumn);

                return querySubDatasetFn(server, std::move(rows),
                                         select, when, *where, orderBy,
                                         TupleExpression(),
                                         *SqlExpression::TRUE,
                                         *SqlExpression::parse("rowName()"),
                                         offset, limit, "" /* dataset alias */,
                                         false /* allow multithreading */);
            };

            BasicRowGenerator result(exec, "row table expression generator");
            return result;
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
    context.writeStringUtf8(print());
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
} // namespace Datacratic
