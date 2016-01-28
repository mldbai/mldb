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
                                 ssize_t limit,
                                 bool allowParallel)
        -> BasicRowGenerator
        {
            return dataset->queryBasic(context, select, when, where, orderBy,
                                       offset, limit, allowParallel);
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

    // HACK: fix them up until we've resolved the . for scope issue
    std::vector<std::pair<Utf8String, Utf8String> > prefixedNames;
    for (auto & t: tables) {
        prefixedNames.emplace_back(t, t + ".");
    }

    std::vector<Utf8String> toRemove;
    for (auto & v: result.vars) {
        for (auto & p: prefixedNames) {
            if (v.first.startsWith(p.second)) {
                toRemove.push_back(v.first);
                
                // Don't add it back anywhere... since we know about the
                // table, it's a resolved variable.
                //Utf8String newName(v.first);
                //newName.replace(0, p.second.length(), Utf8String());
                //result.tables[p.first].vars[newName].merge(v.second);
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
    UnboundEntities result;
    throw HttpReturnException(500, "getUnbound() for SelectSubtableExpression: not done");
    return result;
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
    throw HttpReturnException(500, "getUnbound() for DatasetFunctionExpression: not done");
    return result;
}



} // namespace MLDB
} // namespace Datacratic
