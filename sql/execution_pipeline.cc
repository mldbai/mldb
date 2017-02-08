/** execution_pipeline.cc                                          -*- C++ -*-
    Jeremy Barnes, 27 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "execution_pipeline.h"
#include "execution_pipeline_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include <algorithm>


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* PIPELINE RESULTS                                                          */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(PipelineResults);

PipelineResultsDescription::
PipelineResultsDescription()
{
    addField("values", &PipelineResults::values,
             "Values in the pipeline results up to this point");
    //addField("group", &PipelineResults::group,
    //         "Group of values in this result");
}


/*****************************************************************************/
/* LEXICAL SCOPE                                                             */
/*****************************************************************************/

LexicalScope::
~LexicalScope()
{
}

std::set<Utf8String>
LexicalScope::
tableNames() const
{
    Utf8String a = as();
    if (!a.empty())
        return { a };
    else return { };
}


/*****************************************************************************/
/* PIPELINE EXPRESSION SCOPE                                                 */
/*****************************************************************************/

PipelineExpressionScope::
PipelineExpressionScope(std::shared_ptr<SqlBindingScope> outerScope)
    : outerScope_(outerScope)
{

}

PipelineExpressionScope::
~PipelineExpressionScope()
{
}

std::shared_ptr<PipelineExpressionScope>
PipelineExpressionScope::
tableScope(std::shared_ptr<LexicalScope> table)
{
    auto result = std::make_shared<PipelineExpressionScope>(*this);

    TableEntry entry(table, numOutputFields());
    Utf8String asName = table->as();

    //Note for JB: In which case do we need more than the one default table?
    //Not using the latest causes issues
    result->defaultTables.resize(0);
    result->defaultTables.emplace_back(entry);

    if (!asName.empty())
        result->tables[asName] = entry;
    result->parent_ = shared_from_this();

    auto outputAdded = table->outputAdded(); 

    result->outputInfo_.insert(result->outputInfo_.end(),
                               std::make_move_iterator(outputAdded.begin()),
                               std::make_move_iterator(outputAdded.end()));

    //cerr << "table scope for " << MLDB::type_name(*table) << " goes from "
    //     << entry.fieldOffset << " to " << result->numOutputFields()
    //     << endl;

    return result;
}

std::shared_ptr<PipelineExpressionScope>
PipelineExpressionScope::
parameterScope(GetParamInfo getParamInfo,
               std::vector<std::shared_ptr<ExpressionValueInfo> > outputAdded) const
{
    auto result = std::make_shared<PipelineExpressionScope>(*this);
    result->getParamInfo_ = std::move(getParamInfo);
    result->parent_ = shared_from_this();
    result->outputInfo_.insert(result->outputInfo_.end(),
                               std::make_move_iterator(outputAdded.begin()),
                               std::make_move_iterator(outputAdded.end()));

    //cerr << "parameter scope goes from "
    //     << numOutputFields() << " to " << result->numOutputFields()
    //     << endl;
    return result;
}

std::shared_ptr<PipelineExpressionScope>
PipelineExpressionScope::
selectScope(std::vector<std::shared_ptr<ExpressionValueInfo> > outputAdded) const
{
    auto result = std::make_shared<PipelineExpressionScope>(*this);
    result->parent_ = shared_from_this();
    result->outputInfo_.insert(result->outputInfo_.end(),
                               std::make_move_iterator(outputAdded.begin()),
                               std::make_move_iterator(outputAdded.end()));

    //cerr << "select scope goes from "
    //     << numOutputFields() << " to " << result->numOutputFields()
    //     << endl;

    return result;
}

ColumnGetter
PipelineExpressionScope::
doGetColumn(const Utf8String & tableName, const ColumnPath & columnName)
{
    //cerr << "doGetColumn with tableName " << tableName
    //     << " and variable name " << columnName << endl;

    //if table is explicitly provided
    if (!tableName.empty()) {
        //look in the table scope
        auto it = tables.find(tableName);
        if (it != tables.end()) {
            return it->second.doGetColumn(columnName);
        }
    }
    else {
        //if we have a matching table name from the path
        if (columnName.size() > 1) {
            auto it = tables.find(columnName[0].toUtf8String());
            if (it != tables.end()) {
                return it->second.doGetColumn(columnName.removePrefix());
            }
        }

         //check default table as last resort
        if (tableName.empty() && !defaultTables.empty())
            return defaultTables.back().doGetColumn(columnName);

    }

    // Otherwise, look for it in the enclosing scope
    return outerScope_->doGetColumn(tableName, columnName);
}

GetAllColumnsOutput 
PipelineExpressionScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep)
{
    if (tableName.empty()) {
        if (defaultTables.empty())
            throw HttpReturnException(500, "Get variable without table name with no default table in scope");
        return defaultTables.back().doGetAllColumns(tableName, keep);
    }
    else {
        // Otherwise, look in the table scope
        auto it = tables.find(tableName);
        if (it != tables.end()) {
            return it->second.doGetAllColumns(tableName, keep);
        }

        // look in the default table childs
        for (auto& t : defaultTables) {
            auto tableNames = t.tableNames();
             auto it = tableNames.find(tableName);
             if (it != tableNames.end()) {
                return t.doGetAllColumns(tableName, keep);
             }
        }
    }        

    return outerScope_->doGetAllColumns(tableName, keep);
}

BoundFunction
PipelineExpressionScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    if (tableName.empty()) {
        for (const TableEntry & t: defaultTables) {
            BoundFunction r = t.doGetFunction(functionName, args, argScope);
            if (r)
                return r;
        }

        for (auto & t: tables) {
            if (functionName.startsWith(t.first)) {
                Utf8String suffix = functionName;
                suffix.removePrefix(t.first);
                return t.second.doGetFunction(suffix, args, argScope);
            }
        }
    }
    else {
        // Otherwise, look in the table scope
        auto it = tables.find(tableName);
        if (it != tables.end()) {
            return it->second.doGetFunction(functionName, args, argScope);
        }
    }        

    // Look for a derived function
    auto fnderived
        = getDatasetDerivedFunction(tableName, functionName, args, argScope,
                                    *this, "row");

    if (fnderived)
        return fnderived;

    return outerScope_->doGetFunction(tableName, functionName, args, argScope);
}

ColumnFunction
PipelineExpressionScope::
doGetColumnFunction(const Utf8String & functionName)
{
    return outerScope_->doGetColumnFunction(functionName);
}

ColumnGetter
PipelineExpressionScope::
doGetBoundParameter(const Utf8String & paramName)
{
    //cerr << "doGetBoundParameter for " << paramName << endl;
    if (!getParamInfo_)
        return ColumnGetter();

    auto info = getParamInfo_(paramName);
    if (!info)
        return ColumnGetter();

    auto exec = [=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            auto & row = rowScope.as<PipelineResults>();
            return storage = row.getParam(paramName);
        };
        
    return { exec, info };
}

ColumnPath
PipelineExpressionScope::
doResolveTableName(const ColumnPath & fullColumnName,
                   Utf8String &tableName) const
{
    for (auto & t: tables) {
        if (fullColumnName.startsWith(t.first)) {
            tableName = t.first;
            return fullColumnName.removePrefix();
        }
    }

    for (auto & t: defaultTables) {
        for (auto & t2: t.tableNames()) {
            if (fullColumnName.startsWith(t2)) {
                tableName = t2;
                return fullColumnName.removePrefix();
            }
        }
    }

    return fullColumnName;
}

std::vector<Utf8String>
PipelineExpressionScope::
getTableNames() const
{
    std::vector<Utf8String> result;
    for (auto & t: defaultTables) {
        if (!t.scope)
            continue;
        std::set<Utf8String> n = t.scope->tableNames();
        result.insert(result.end(), n.begin(), n.end());
    }

    for (auto & t: tables) {
        result.push_back(t.first);
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()),
                 result.end());

    return result;
}

MldbServer * 
PipelineExpressionScope::
getMldbServer() const
{
    return outerScope_->getMldbServer();
}

std::shared_ptr<Dataset>
PipelineExpressionScope::
doGetDataset(const Utf8String & datasetName)
{
    return outerScope_->doGetDataset(datasetName);
}

std::shared_ptr<Dataset>
PipelineExpressionScope::
doGetDatasetFromConfig(const Any & datasetConfig)
{
     return outerScope_->doGetDatasetFromConfig(datasetConfig);
}

PipelineExpressionScope::TableEntry::
TableEntry(std::shared_ptr<LexicalScope> scope,
           int fieldOffset)
    : scope(scope), fieldOffset(fieldOffset)
{
}

ColumnGetter
PipelineExpressionScope::TableEntry::
doGetColumn(const ColumnPath & columnName) const
{
    return scope->doGetColumn(columnName, fieldOffset);
}
    
GetAllColumnsOutput
PipelineExpressionScope::TableEntry::
doGetAllColumns(const Utf8String & tableName, const ColumnFilter& keep) const
{
    return scope->doGetAllColumns(tableName, keep, fieldOffset);
}

BoundFunction
PipelineExpressionScope::TableEntry::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope) const
{
    return scope->doGetFunction(functionName, args, fieldOffset, argScope);
}

std::set<Utf8String>
PipelineExpressionScope::TableEntry::
tableNames() const
{
    return scope->tableNames();
}

/*****************************************************************************/
/* ELEMENT EXECUTOR                                                          */
/*****************************************************************************/

bool
ElementExecutor::
takeAll(std::function<bool (std::shared_ptr<PipelineResults> &)> onResult)
{
    std::shared_ptr<PipelineResults> res;
    while ((res = take()))
        if (!onResult(res))
            return false;
    return true;
}

/*****************************************************************************/
/* PIPELINE ELEMENT                                                          */
/*****************************************************************************/

std::shared_ptr<PipelineElement>
PipelineElement::
root(std::shared_ptr<SqlBindingScope> scope)
{
    return std::make_shared<RootElement>(scope);
}

std::shared_ptr<PipelineElement>
PipelineElement::
root(SqlBindingScope & scope)
{
    return std::make_shared<RootElement>(ML::make_unowned_sp(scope));
}
    
std::shared_ptr<PipelineElement>
PipelineElement::
root()
{
    return std::make_shared<RootElement>(std::make_shared<SqlBindingScope>());
}

std::shared_ptr<PipelineElement>
PipelineElement::
params(std::function<std::shared_ptr<ExpressionValueInfo> (const Utf8String & name)> getParamInfo)
{
    return std::make_shared<ParamsElement>(shared_from_this(), std::move(getParamInfo));
}

std::shared_ptr<PipelineElement>
PipelineElement::
from(std::shared_ptr<TableExpression> from,
     WhenExpression when,
     SelectExpression select,
     std::shared_ptr<SqlExpression> where,
     OrderByExpression orderBy,
     GetParamInfo getParamInfo)
{
    //The FromElement needs the GetParamInfo in case it is a sub select (e.g., "select * from (select $param)")
    //In which case the GetParamInfo is needed to create the sub-pipeline.

    return std::make_shared<FromElement>(shared_from_this(), from, 
                                         BoundTableExpression(),
                                         when,
                                         select, where, orderBy, getParamInfo);
}

std::shared_ptr<PipelineElement>
PipelineElement::
from(std::shared_ptr<TableExpression> from,
     BoundTableExpression boundFrom,
     WhenExpression when,
     SelectExpression select,
     std::shared_ptr<SqlExpression> where,
     OrderByExpression orderBy)
{
    return std::make_shared<FromElement>(shared_from_this(), from, boundFrom,
                                         when, select, where, orderBy);
}

std::shared_ptr<PipelineElement>
PipelineElement::
join(std::shared_ptr<TableExpression> left,
     std::shared_ptr<TableExpression> right,
     std::shared_ptr<SqlExpression> on,
     JoinQualification joinQualification,
     SelectExpression select,
     std::shared_ptr<SqlExpression> where,
     OrderByExpression orderBy)
{
    return std::make_shared<JoinElement>(shared_from_this(),
                                         std::move(left),
                                         BoundTableExpression(),
                                         std::move(right),
                                         BoundTableExpression(),
                                         std::move(on),
                                         joinQualification,
                                         std::move(select),
                                         std::move(where),
                                         std::move(orderBy));
}

std::shared_ptr<PipelineElement>
PipelineElement::
join(std::shared_ptr<TableExpression> left,
     BoundTableExpression boundLeft,
     std::shared_ptr<TableExpression> right,
     BoundTableExpression boundRight,
     std::shared_ptr<SqlExpression> on,
     JoinQualification joinQualification,
     SelectExpression select,
     std::shared_ptr<SqlExpression> where,
     OrderByExpression orderBy)
{
    return std::make_shared<JoinElement>(shared_from_this(),
                                         std::move(left),
                                         std::move(boundLeft),
                                         std::move(right),
                                         std::move(boundRight),
                                         std::move(on),
                                         joinQualification,
                                         std::move(select),
                                         std::move(where),
                                         std::move(orderBy));
}

std::shared_ptr<PipelineElement>
PipelineElement::
where(std::shared_ptr<SqlExpression> where)
{
    return std::make_shared<FilterWhereElement>(shared_from_this(), where);
}

std::shared_ptr<PipelineElement>
PipelineElement::
select(SelectExpression select)
{
    return std::make_shared<SelectElement>(shared_from_this(), select);
}

std::shared_ptr<PipelineElement>
PipelineElement::
select(std::shared_ptr<SqlExpression> select)
{
    if (!select)
        return shared_from_this();
    return std::make_shared<SelectElement>(shared_from_this(), select);
}

std::shared_ptr<PipelineElement>
PipelineElement::
select(const OrderByExpression & orderBy)
{
    std::shared_ptr<PipelineElement> result = shared_from_this();
    for (auto & c: orderBy.clauses) {
        result = result->select(c.first);
    }
    return result;
}

std::shared_ptr<PipelineElement>
PipelineElement::
select(const TupleExpression & tup)
{
    std::shared_ptr<PipelineElement> result = shared_from_this();
    for (auto & c: tup.clauses) {
        result = result->select(c);
    }
    return result;
}

std::shared_ptr<PipelineElement>
PipelineElement::
sort(OrderByExpression orderBy)
{
    return std::make_shared<OrderByElement>(shared_from_this(), orderBy);
}

std::shared_ptr<PipelineElement>
PipelineElement::
partition(int numElements)
{
    return std::make_shared<PartitionElement>(shared_from_this(), numElements);
}

std::shared_ptr<PipelineElement>
PipelineElement::
statement(const SelectStatement& stm, GetParamInfo getParamInfo)
{
    auto root = shared_from_this();

    bool hasGroupBy = !stm.groupBy.empty();
    std::vector< std::shared_ptr<SqlExpression> > aggregators
        = stm.select.findAggregators(hasGroupBy);

    auto groupBy = stm.groupBy;

    if (!hasGroupBy && !aggregators.empty()) {
        //if we have no group by but aggregators, make a universal group
        groupBy.clauses.emplace_back(SqlExpression::parse("1"));
        hasGroupBy = true;
    }

    if (hasGroupBy) {
        return root
            ->params(getParamInfo)
            ->from(stm.from, stm.when,
                   SelectExpression::STAR, stm.where,
                   OrderByExpression(), getParamInfo)
            ->where(stm.where)
            ->select(groupBy)
            ->sort(groupBy)
            ->partition(groupBy.clauses.size())
            ->where(stm.having)
            ->select(stm.orderBy)
            ->sort(stm.orderBy)
            ->select(stm.rowName)  // second last element is rowname
            ->select(stm.select);
    }
    else {
        if (stm.from && stm.from->getType() != "null") {
            return root
            ->params(getParamInfo)
            ->from(stm.from, stm.when,
                   SelectExpression::STAR, stm.where,
                   OrderByExpression(), getParamInfo)
            ->where(stm.where)
            ->select(stm.orderBy)
            ->sort(stm.orderBy)
            ->select(stm.rowName)  // second last element is rowname
            ->select(stm.select);
        }
        else {
            return root
            ->params(getParamInfo)
            ->from(stm.from, stm.when,
                   SelectExpression::STAR, stm.where,
                   OrderByExpression(), getParamInfo)
            ->select(stm.rowName)  // second last element is rowname
            ->select(stm.select); // only select 1 value
        }
    }
}

} // namespace MLDB

