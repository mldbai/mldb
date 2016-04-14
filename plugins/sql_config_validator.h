/** sql_config_validator.h                                         -*- C++ -*-
    Guy Dumais, 18 December 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Several templates to validate constraints on SQL statements in entity
    configs.
*/

#include "mldb/sql/sql_expression.h"
#include "types/optional.h"

#pragma once

namespace Datacratic {

namespace MLDB {


// VALIDATION HELPERS
template<typename ConfigType, 
    typename FieldType,
    template<typename> class Validator1>
std::function<void (ConfigType *, JsonParsingContext &)>
validate(FieldType ConfigType::* field, const char * name)
{
    return [=](ConfigType * cfg, JsonParsingContext & context)
        {
            Validator1<FieldType>()(cfg->*field, name);
        };
}

// consider using a variadic parameter
template<typename ConfigType,
    typename FieldType,
    template<typename> class Validator1,
    template<typename> class Validator2>
std::function<void (ConfigType *, JsonParsingContext & context)>
validate(FieldType ConfigType::* field, const char * name)
{
     return [=](ConfigType * cfg, JsonParsingContext & context)
        {
            Validator1<FieldType>()(cfg->*field, name);
            Validator2<FieldType>()(cfg->*field, name);
        };
}

// consider using a variadic parameter
template<typename ConfigType,
    typename FieldType,
    template<typename> class Validator1,
    template<typename> class Validator2,
    template<typename> class Validator3>
std::function<void (ConfigType *, JsonParsingContext & context)>
validate(FieldType ConfigType::* field, const char * name)
{
     return [=](ConfigType * cfg, JsonParsingContext & context)
        {
            Validator1<FieldType>()(cfg->*field, name);
            Validator2<FieldType>()(cfg->*field, name);
            Validator3<FieldType>()(cfg->*field, name);
        };
}

// really consider using a variadic parameter
template<typename ConfigType,
    typename FieldType,
    template<typename> class Validator1,
    template<typename> class Validator2,
    template<typename> class Validator3,
    template<typename> class Validator4>
std::function<void (ConfigType *, JsonParsingContext & context)>
validate(FieldType ConfigType::* field, const char * name)
{
     return [=](ConfigType * cfg, JsonParsingContext & context)
        {
            Validator1<FieldType>()(cfg->*field, name);
            Validator2<FieldType>()(cfg->*field, name);
            Validator3<FieldType>()(cfg->*field, name);
            Validator4<FieldType>()(cfg->*field, name);
        };
}

// one can chain validation of several fields it this way
// chain(validator1, chain(validator2, validator3))
template <typename ConfigType> 
std::function<void (ConfigType *, JsonParsingContext &)>
chain(const std::function<void (ConfigType *, JsonParsingContext &)> & validator1,
      const std::function<void (ConfigType *, JsonParsingContext &)> & validator2) {
    return [=](ConfigType * config, JsonParsingContext & context) {
            validator1(config, context);
            validator2(config, context);
        };
}

/** 
 *  Accept any select statement with empty GROUP BY/HAVING clause.
 *  FieldType must contain a SelectStatement named stm.
 */
template<typename FieldType> struct NoGroupByHaving 
{
    void operator()(const FieldType & query, const char * name)
    {
        if (query.stm) {
            if (!query.stm->groupBy.empty()) {
                throw ML::Exception("cannot train %s with a groupBy clause", name);
            }
            else if (!query.stm->having->isConstantTrue()) {
                throw ML::Exception("cannot train %s with a having clause", name);
            }
        }
    }
};

template<typename FieldType> struct NoGroupByHaving<Optional<FieldType> >
{
    void operator()(const Optional<FieldType> & query, const char * name)
    {
        if (query) NoGroupByHaving<FieldType>()(*query, name);
    }
};

/** 
  *  Must contain a FROM clause
 */
template<typename FieldType> struct MustContainFrom
{
    void operator()(const FieldType & query, const char * name)
    {
        if (!query.stm || !query.stm->from || query.stm->from->surface.empty())
            throw ML::Exception("%s must contain a FROM clause", name);
    }
};

template<typename FieldType> struct MustContainFrom<Optional<FieldType> >
{
    void operator()(const Optional<FieldType> & query, const char * name)
    {
        if (query) MustContainFrom<FieldType>()(*query, name);
    }
};

/**
 *  Accept simple select expressions like column1, column2, wildcard expressions
 *  and column expressions but reject operations on columns like sum(column1, column2).
 *  FieldType must contain a SelectStatement named stm.
 */
template<typename FieldType> struct PlainColumnSelect
{
    void operator()(const FieldType & query, const char * name)
    {
        auto getWildcard = [] (const std::shared_ptr<SqlRowExpression> expression) 
            -> std::shared_ptr<const WildcardExpression>
            {
                return std::dynamic_pointer_cast<const WildcardExpression>(expression);
            };

        auto getColumnExpression = [] (const std::shared_ptr<SqlRowExpression> expression)
            -> std::shared_ptr<const SelectColumnExpression>
            {
                return std::dynamic_pointer_cast<const SelectColumnExpression>(expression);
            };

        auto getComputedColumn = [] (const std::shared_ptr<SqlRowExpression> expression)
            -> std::shared_ptr<const ComputedColumn>
            {
                return std::dynamic_pointer_cast<const ComputedColumn>(expression);
            };

        auto getReadVariable = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const ReadColumnExpression>
            {
                return std::dynamic_pointer_cast<const ReadColumnExpression>(expression);
            };

        auto getWithinExpression = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const SelectWithinExpression>
            {
                return std::dynamic_pointer_cast<const SelectWithinExpression>(expression);
            };

        auto getIsTypeExpression = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const IsTypeExpression>
            {
                return std::dynamic_pointer_cast<const IsTypeExpression>(expression);
            };

        auto getComparisonExpression = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const ComparisonExpression>
            {
                return std::dynamic_pointer_cast<const ComparisonExpression>(expression);
            };

        auto getBooleanExpression = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const BooleanOperatorExpression>
            {
                return std::dynamic_pointer_cast<const BooleanOperatorExpression>(expression);
            };

        auto getFunctionCallExpression = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const FunctionCallExpression>
            {
                return std::dynamic_pointer_cast<const FunctionCallExpression>(expression);
            };

        auto getConstantExpression = [] (const std::shared_ptr<SqlExpression> expression) 
            -> std::shared_ptr<const ConstantExpression>
            {
                return std::dynamic_pointer_cast<const ConstantExpression>(expression);
            };

        if (query.stm) {
            auto & select = query.stm->select;
            for (const auto & clause : select.clauses) {

                auto wildcard = getWildcard(clause);
                if (wildcard)
                    continue;

                auto columnExpression = getColumnExpression(clause);
                if (columnExpression)
                    continue;

                auto computedVariable = getComputedColumn(clause);
                if (computedVariable) {
                    auto readVariable = getReadVariable(computedVariable->expression);
                    if (readVariable)
                        continue;
                    // {x, y}
                    auto withinExpression = getWithinExpression(computedVariable->expression);
                    if (withinExpression)
                        continue;
                    // x is not null
                    auto isTypeExpression = getIsTypeExpression(computedVariable->expression);
                    if (isTypeExpression)
                        continue;
                    // x = 'true'
                    auto comparisonExpression = getComparisonExpression(computedVariable->expression);
                    if (comparisonExpression)
                        continue;
                    // NOT x 
                    auto booleanExpression = getBooleanExpression(computedVariable->expression);
                    if (booleanExpression)
                        continue;
                    // function(args)[extract]
                    auto functionCallExpression = getFunctionCallExpression(computedVariable->expression);
                    if (functionCallExpression)
                        continue;
                     // 1.0
                    auto constantExpression = getConstantExpression(computedVariable->expression);
                    if (constantExpression)
                        continue;
                }

                throw ML::Exception(std::string(name) + 
                                    " training only accept wildcard and column names at " + 
                                    clause->surface.rawString());
            }
        }
    }
};

template<typename FieldType> struct PlainColumnSelect<Optional<FieldType> >
{
    void operator()(const Optional<FieldType> & query, const char * name)
    {
        if (query) PlainColumnSelect<FieldType>()(*query, name);
    }
};

inline bool containsNamedSubSelect(const InputQuery& query, const Utf8String& name) 
{

    auto getComputedColumn = [] (const std::shared_ptr<SqlRowExpression> expression)
        -> std::shared_ptr<const ComputedColumn>
        {
            return std::dynamic_pointer_cast<const ComputedColumn>(expression);
        };

    if (query.stm) {
        auto & select = query.stm->select;
        for (const auto & clause : select.clauses) {
            
            auto computedVariable = getComputedColumn(clause);
            if (computedVariable
                && computedVariable->alias.size() == 1
                && computedVariable->alias[0] ==  name)
                return true;
        }
    }
    return false;
}

/**
 *  Ensure the select contains a row named "features" and a scalar named "label".
 *  FieldType must contain a SelectStatement named stm.
 */
template<typename FieldType> struct FeaturesLabelSelect
{
    void operator()(const FieldType & query, const char * name)
    {
        if (!containsNamedSubSelect(query, "features") ||
            !containsNamedSubSelect(query, "label") )
            throw ML::Exception("%s training expect a row named 'features' and a scalar named 'label'", name);
    }
};

template<typename FieldType> struct FeaturesLabelSelect<Optional<FieldType> >
{
    void operator()(const Optional<FieldType> & query, const char * name)
    {
        if (query) FeaturesLabelSelect<FieldType>()(*query, name);
    }
};

/**
 *  Ensure the select contains a scalar named "score" and a scalar named "label".
 *  FieldType must contain a SelectStatement named stm.
 */
template<typename FieldType> struct ScoreLabelSelect
{
    void operator()(const FieldType & query, const char * name)
    {
        if (!containsNamedSubSelect(query, "score") ||
            !containsNamedSubSelect(query, "label") )
            throw ML::Exception("%s training expect a scalar named 'score' and a scalar named 'label'", name);
    }
};

} // namespace MLDB
} // namespace Datacratic
