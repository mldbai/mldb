/** sql_config_validator.h                                         -*- C++ -*-
    Guy Dumais, 18 December 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Several templates to validate constraints on SQL statements and other 
    parts of entity configs.
*/

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "types/optional.h"
#include "mldb/arch/demangle.h"

#pragma once

namespace Datacratic {

namespace MLDB {


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

template<typename ConfigType,
         typename FieldType, // either InputQuery or Optional<InputQuery>
         typename Constraint>
std::function<void (ConfigType *, JsonParsingContext &)>
validateQuery(FieldType ConfigType::* field, const Constraint & constraint)
{
    return [=](ConfigType * cfg, JsonParsingContext & context) {
        constraint(cfg->*field, ConfigType::name);
    };
}

template<typename ConfigType,
         typename FieldType, // either InputQuery or Optional<InputQuery>
         typename Constraint,
         typename... Constraints>
std::function<void (ConfigType *, JsonParsingContext & context)>
validateQuery(FieldType ConfigType::* field, const Constraint & constraint, Constraints&& ...constraints)
{
    // workaround of a gcc 4.8 limitation
    // parameter packs cannot be expended in lambda function
    // see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=55914
    auto validator = [=](ConfigType * cfg, JsonParsingContext & context) {
        constraint.operator()(cfg->*field, ConfigType::name);
    };

    return chain<ConfigType>(validator, validateQuery(field, constraints...));
}

struct QueryValidator {
    virtual void operator()(const InputQuery & query, const std::string & name) const = 0;
    void operator()(const Optional<InputQuery> & query, const std::string & name) const;
};

/**
 *  Accept any select statement with empty GROUP BY/HAVING clause.
 *  FieldType must contain a SelectStatement named stm.
 */
struct NoGroupByHaving : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Accept any select statement with empty WHERE clause.
 *  FieldType must contain a SelectStatement named stm.
 */
struct NoWhere : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Accept any select statement with empty LIMIT clause.
 *  FieldType must contain a SelectStatement named stm.
 */
struct NoLimit : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Accept any select statement with empty OFFSET clause.
 *  FieldType must contain a SelectStatement named stm.
 */
struct NoOffset : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
  *  Must contain a FROM clause
 */
struct MustContainFrom : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Check the presence of a NamedColumnExpression with name `features` that contains 
 *  only simple row expressions of the form: {column1, column2}, {prefix*}, {*} and 
 *  {* EXCLUDING(column)}.  Basically, any expression that select columns without altering
 *  their values.
 */
struct PlainColumnSelect : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Check the presence of simple row expressions of the form: {column1, column2}, 
 *  {prefix*}, {*} and {* EXCLUDING(column)}.  Basically, any expression that select columns 
 *  without altering their values are accepted.
 */
struct PlainColumnSubSelect : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

inline bool containsNamedSubSelect(const InputQuery& query, const Utf8String& name) 
{

    auto getNamedColumnExpression = [] (const std::shared_ptr<SqlRowExpression> expression)
        -> std::shared_ptr<const NamedColumnExpression>
        {
            return std::dynamic_pointer_cast<const NamedColumnExpression>(expression);
        };

    if (query.stm) {
        auto & select = query.stm->select;
        for (const auto & clause : select.clauses) {
            auto namedColumn = getNamedColumnExpression(clause);
            if (namedColumn
                && namedColumn->alias.size() == 1
                && namedColumn->alias[0] ==  name)
                return true;
        }
    }
    return false;
}

/**
 *  Ensure the select contains a row named "features" and a scalar named "label".
 *  FieldType must contain a SelectStatement named stm.
 */
struct FeaturesLabelSelect : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Ensure the select contains a scalar named "score" and a scalar named "label".
 *  FieldType must contain a SelectStatement named stm.
 */
struct ScoreLabelSelect : public QueryValidator
{
    using QueryValidator::operator();
    void operator()(const InputQuery & query, const std::string & name) const;
};

/**
 *  Make sure that if a functionName is specified, a valid modelFileUrl
 *  is also specified.
 */
template<typename ConfigType>
std::function<void (ConfigType *, JsonParsingContext &)>
validateFunction()
{
    return [](ConfigType * cfg, JsonParsingContext & context) {
        if (!cfg->functionName.empty() &&
            !cfg->modelFileUrl.valid()) {
                throw ML::Exception(std::string(ConfigType::name) + " requires a valid "
                                    "modelFileUrl when specifying a functionName. "
                                    "modelFileUrl '" + cfg->modelFileUrl.toString()
                                    + "' is invalid.");
        }
    };
}

} // namespace MLDB
} // namespace Datacratic
