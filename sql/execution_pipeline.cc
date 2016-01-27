/** execution_pipeline.cc                                          -*- C++ -*-
    Jeremy Barnes, 27 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "execution_pipeline.h"
#include "execution_pipeline_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/jml/utils/smart_ptr_utils.h"

using namespace std;


namespace Datacratic {
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
PipelineExpressionScope(std::shared_ptr<SqlBindingScope> context)
    : ReadThroughBindingContext(*context),
      context_(std::move(context))
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

    TableEntry entry(table, outputInfo_.size());
    Utf8String asName = table->as();
    result->defaultTables.emplace_back(entry);
    if (!asName.empty())
        result->tables[asName] = entry;
    result->parent_ = shared_from_this();

    auto outputAdded = table->outputAdded(); 

    result->outputInfo_.insert(result->outputInfo_.end(),
                               std::make_move_iterator(outputAdded.begin()),
                               std::make_move_iterator(outputAdded.end()));

    //cerr << "table scope for " << ML::type_name(*table) << " goes from "
    //     << entry.fieldOffset << " to " << result->outputInfo_.size()
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
    return result;
}

VariableGetter
PipelineExpressionScope::
doGetVariable(const Utf8String & tableName, const Utf8String & variableName)
{
    //cerr << "doGetVariable with tableName " << tableName
    //     << " and variable name " << variableName << endl;
        
    if (tableName.empty()) {
        if (defaultTables.empty()) {

            cerr << "tables in scope: ";
            for (auto & t: tables)
                cerr << t.first;
            cerr << endl;
            throw HttpReturnException(500, "Get variable without table name with no default table in scope");
        }
        return defaultTables.back().doGetVariable(variableName);
    }
    else {
        // Otherwise, look in the table scope
        auto it = tables.find(tableName);
        if (it != tables.end()) {
            return it->second.doGetVariable(variableName);
        }
    }        

    // Otherwise, look for it in the enclosing scope
    return ReadThroughBindingContext::doGetVariable(tableName, variableName);
}

GetAllColumnsOutput 
PipelineExpressionScope::
doGetAllColumns(const Utf8String & tableName,
                std::function<Utf8String (const Utf8String &)> keep)
{
    if (tableName.empty()) {
        if (defaultTables.empty())
            throw HttpReturnException(500, "Get variable without table name with no default table in scope");
        return defaultTables.back().doGetAllColumns(keep);
    }
    else {
        // Otherwise, look in the table scope
        auto it = tables.find(tableName);
        if (it != tables.end()) {
            return it->second.doGetAllColumns(keep);
        }
    }        

    return ReadThroughBindingContext::doGetAllColumns(tableName, keep);
}

BoundFunction
PipelineExpressionScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args)
{
    if (tableName.empty()) {
        for (const TableEntry & t: defaultTables) {
            BoundFunction r = t.doGetFunction(functionName, args);
            if (r)
                return r;
        }
    }
    else {
        // Otherwise, look in the table scope
        auto it = tables.find(tableName);
        if (it != tables.end()) {
            return it->second.doGetFunction(functionName, args);
        }
    }        

    return ReadThroughBindingContext::doGetFunction(tableName, functionName, args);
}

ColumnFunction
PipelineExpressionScope::
doGetColumnFunction(const Utf8String & functionName)
{
    return ReadThroughBindingContext::doGetColumnFunction(functionName);
}

VariableGetter
PipelineExpressionScope::
doGetBoundParameter(const Utf8String & paramName)
{
    //cerr << "doGetBoundParameter for " << paramName << endl;
    if (!getParamInfo_)
        return VariableGetter();

    auto info = getParamInfo_(paramName);
    if (!info)
        return VariableGetter();

    auto exec = [=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            auto & row = static_cast<const PipelineResults &>(rowScope);
            return storage = std::move(row.getParam(paramName));
        };
        
    return { exec, info };
}

PipelineExpressionScope::TableEntry::
TableEntry(std::shared_ptr<LexicalScope> scope,
           int fieldOffset)
    : scope(scope), fieldOffset(fieldOffset)
{
}

VariableGetter
PipelineExpressionScope::TableEntry::
doGetVariable(const Utf8String & variableName) const
{
    return scope->doGetVariable(variableName, fieldOffset);
}
    
GetAllColumnsOutput
PipelineExpressionScope::TableEntry::
doGetAllColumns(std::function<Utf8String (const Utf8String &)> keep) const
{
    return scope->doGetAllColumns(keep, fieldOffset);
}

BoundFunction
PipelineExpressionScope::TableEntry::
doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args) const
{
    return scope->doGetFunction(functionName, args, fieldOffset);
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
     OrderByExpression orderBy)
{
    return std::make_shared<FromElement>(shared_from_this(), from, when,
                                         select, where, orderBy);
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
                                         std::move(right),
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


} // namespace MLDB
} // namespace Datacratic
