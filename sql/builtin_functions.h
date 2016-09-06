/** builtin_functions.h                                             -*- C++ -*-
    Francois Maillet, 21 janvier 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#pragma once

#include "sql/expression_value.h"
#include "sql/sql_expression.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/http/http_exception.h"
#include <memory>
#include <vector>

namespace Datacratic {
namespace MLDB {

// Empty string to avoid construction of temporary object
extern const Utf8String NO_FUNCTION_NAME;

inline void checkArgsSize(size_t number, size_t expected,
                          const Utf8String & fctName_=NO_FUNCTION_NAME)
{
    if (number != expected) {
        auto fctName = fctName_;
        if (!fctName.empty()) {
            fctName = "function " + fctName + " ";
        }
        if (expected != 1)
            throw HttpReturnException(400, fctName + "expected " + to_string(expected) + " arguments, got " + to_string(number));
        else
            throw HttpReturnException(400, fctName + "expected " + to_string(expected) + " argument, got " + to_string(number));
    }
}

inline void checkArgsSize(size_t number, size_t minArgs, size_t maxArgs,
                          const Utf8String & fctName_=NO_FUNCTION_NAME)
{
    if (minArgs == maxArgs) {
        checkArgsSize(number, minArgs, fctName_);
        return;
    }
    if (number < minArgs || number > maxArgs) {
        auto fctName = fctName_;
        if (!fctName.empty()) {
            fctName = "function " + fctName + " ";
        }
        throw HttpReturnException
            (400, fctName + "expected between "
             + std::to_string(minArgs) + " and "
             + std::to_string(maxArgs) + " arguments, got "
             + std::to_string(number));
    }
}

// Calculate the effective timstamps for an expression involving two
// operands.
inline Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2)
{
    return std::max(v1.getEffectiveTimestamp(),
                    v2.getEffectiveTimestamp());
}

inline Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2,
                   const ExpressionValue & v3)
{
    return std::max(std::max(v1.getEffectiveTimestamp(),
                             v2.getEffectiveTimestamp()),
                    v3.getEffectiveTimestamp());
}

inline Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2,
                   const ExpressionValue & v3,
                   const ExpressionValue & v4)
{
    return std::max(std::max(std::max(v1.getEffectiveTimestamp(),
                                      v2.getEffectiveTimestamp()),
                             v3.getEffectiveTimestamp()),
                    v4.getEffectiveTimestamp());
}

namespace Builtins {

void
unpackJson(RowValue & row,
           const std::string & id,
           const Json::Value & val,
           const Date & ts);

typedef BoundFunction (*BuiltinFunction) (const std::vector<BoundSqlExpression> &);

struct RegisterBuiltin {
    template<typename... Names>
    RegisterBuiltin(const BuiltinFunction & function, Names&&... names)
    {
        doRegister(function, std::forward<Names>(names)...);
    }

    void doRegister(const BuiltinFunction & function)
    {
    }

    template<typename... Names>
    void doRegister(const BuiltinFunction & function, std::string name,
                    Names&&... names)
    {
        auto fn = [=] (const Utf8String & str,
                       const std::vector<BoundSqlExpression> & args,
                       SqlBindingScope & scope)
            -> BoundFunction
            {
                try {
                    BoundFunction result = function(args);
                    auto fn = result.exec;
                    result.exec = [=] (const std::vector<ExpressionValue> & args,
                                       const SqlRowScope & scope)
                    -> ExpressionValue
                    {
                        try {
                            return fn(args, scope);
                        } JML_CATCH_ALL {
                            rethrowHttpException(-1, "Executing builtin function "
                                                 + str + ": " + ML::getExceptionString(),
                                                 "functionName", str,
                                                 "functionArgs", args);
                        }
                    };

                    return result;
                } JML_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin function "
                                         + str + ": " + ML::getExceptionString(),
                                         "functionName", str,
                                         "functionArgs", args);
                }
            };
        handles.push_back(registerFunction(Utf8String(name), fn));
        doRegister(function, std::forward<Names>(names)...);
    }

    std::vector<std::shared_ptr<void> > handles;
};


/*****************************************************************************/
/* SQL BUILTIN                                                               */
/*****************************************************************************/

/** Allows a builtin function to be defined in SQL.

    Example:

    DEF_SQL_BUILTIN(sincos, 2, "[sin($1), cos($1)]");

    This will add a builtin function called sincos that is essentially a
    macro for the given implementation.
*/

struct SqlBuiltin {
    SqlBuiltin(const std::string & name,
               const Utf8String & expr,
               size_t arity);

    BoundFunction bind(const std::vector<BoundSqlExpression> & args,
                       SqlBindingScope & scope) const;

    Utf8String functionName;
    size_t arity;
    std::shared_ptr<SqlExpression> parsed;
    std::shared_ptr<void> handle;
};

#define DEF_SQL_BUILTIN(op, arity, expr) \
    static Datacratic::MLDB::Builtins::SqlBuiltin register_##op(#op, expr, arity);

} // namespace Builtins
} // namespace MLDB
} // namespace Datacratic
