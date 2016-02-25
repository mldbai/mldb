/** builtin_functions.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Builtin functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "sql_expression.h"
#include "tokenize.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/stats/distribution_simd.h"
#include "mldb/jml/utils/csv.h"
#include "mldb/types/vector_description.h"
#include "mldb/ml/confidence_intervals.h"
#include "mldb/jml/math/xdiv.h"
#include "mldb/base/hash.h"
#include "mldb/base/parse_context.h"
#include <boost/lexical_cast.hpp>

#include <boost/regex/icu.hpp>

using namespace std;


namespace Datacratic {
namespace MLDB {
namespace Builtins {

namespace {
    void checkArgsSize(size_t number, size_t expected)
    {
        if (number != expected)
        {
            if (expected != 1)
                throw HttpReturnException(400, "expected " + to_string(expected) + " arguments, got " + to_string(number));
            else
                throw HttpReturnException(400, "expected " + to_string(expected) + " argument, got " + to_string(number));
        }
    }    
}

// Calculate the effective timstamps for an expression involving two
// operands.
static Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2)
{
    return std::max(v1.getEffectiveTimestamp(),
                    v2.getEffectiveTimestamp());
}

typedef BoundFunction (*BuiltinFunction) (const std::vector<BoundSqlExpression> &);
typedef ValuedBoundFunction (*ValuedBuiltinFunction) (const std::vector<BoundSqlExpression> &);

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
#if 0
                    std::vector<BoundSqlExpression> boundArgs;
                    for (auto& arg : args) {
                        boundArgs.emplace_back(std::move(arg->bind(scope)));
                    }
#endif                    
                    BoundFunction result = std::move(function(args));
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

    template<typename... Names>
    RegisterBuiltin(const ValuedBuiltinFunction & function, Names&&... names)
    {
        doRegister(function, std::forward<Names>(names)...);
    }

    void doRegister(const ValuedBuiltinFunction & function)
    {
    }

    template<typename... Names>
    void doRegister(const ValuedBuiltinFunction & function, std::string name,
                    Names&&... names)
    {
        auto fn = [=] (const Utf8String & str,
                       const std::vector<std::shared_ptr<SqlExpression> > & args,
                       SqlBindingScope & scope)
            -> BoundFunction
            {
                try {
                    std::vector<BoundSqlExpression> boundArgs;
                    for (auto& arg : args) {
                        boundArgs.emplace_back(std::move(arg->bind(scope)));
                    }

                    ValuedBoundFunction valuedBoundFunction = std::move(function(boundArgs));
                    
                    auto fn = [=] (const std::vector<BoundSqlExpression> & args,
                                   const SqlRowScope & scope)
                    -> ExpressionValue
                    {
                        try {
                            std::vector<ExpressionValue> evaluatedArgs;
                            for (auto& arg : boundArgs)
                                evaluatedArgs.emplace_back(arg(scope, GET_LATEST));
                            
                            return valuedBoundFunction(evaluatedArgs, scope);
                        } JML_CATCH_ALL {
                            rethrowHttpException(-1, "Executing builtin function "
                                                 + str + ": " + ML::getExceptionString(),
                                                 "functionName", str,
                                                 "functionArgs", args);
                        }
                    };
                    
                    return BoundFunction({fn, valuedBoundFunction.resultInfo});
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
/* UNARY SCALARS                                                             */
/*****************************************************************************/

typedef CellValue (*UnaryScalarFunction) (const CellValue & arg);

/// Register a builtin function that operates on unary scalars with a
/// signature (Atom) -> Atom, to work on scalars, rows or
/// embeddings.

struct RegisterBuiltinUnaryScalar {
    template<typename... Names>
    RegisterBuiltinUnaryScalar(const UnaryScalarFunction & function,
                               std::shared_ptr<ExpressionValueInfo> info,
                               Names&&... names)
    {
        doRegister(function, std::move(info), std::forward<Names>(names)...);
    }

    void doRegister(const UnaryScalarFunction & function)
    {
    }

    typedef ExpressionValue (*Wrapper) (UnaryScalarFunction,
                                        const std::vector<ExpressionValue> & args,
                                        const SqlRowScope & scope);

    static BoundFunction wrap(const Utf8String & functionName,
                              UnaryScalarFunction fn,
                              std::shared_ptr<ExpressionValueInfo> resultInfo,
                              Wrapper wrapper)
    {
        BoundFunction result;
        result.exec =  [=] (const std::vector<ExpressionValue> & args,
                            const SqlRowScope & scope)
            -> ExpressionValue
            {
                try {
#if 0
                     std::vector<ExpressionValue> evaluatedArgs;
                     for (auto& arg : args)
                         evaluatedArgs.emplace_back(arg(scope, GET_LATEST));
#endif
                    return wrapper(fn, args, scope);
                } JML_CATCH_ALL {
                    rethrowHttpException(-1, "Executing builtin function "
                                         + functionName
                                         + ": " + ML::getExceptionString(),
                                         "functionName", functionName,
                                         "functionArgs", args);
                }
            };
        result.resultInfo = resultInfo;
        return result;
    }
    
    static ExpressionValue
    applyScalar(UnaryScalarFunction fn,
                const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope)
    {
        return ExpressionValue(fn(args[0].getAtom()),
                               args[0].getEffectiveTimestamp());
    }

    static ExpressionValue
    applyEmbedding(UnaryScalarFunction fn,
                   const std::vector<ExpressionValue> & args,
                   const SqlRowScope & scope)
    {
        std::vector<CellValue> vals1 = args[0].getEmbeddingCell();
        for (auto & v: vals1) {
            v = fn(v);
        }
        
        return ExpressionValue(std::move(vals1),
                               args[0].getEffectiveTimestamp(),
                               args[0].getEmbeddingShape());
    }

    static ExpressionValue
    applyRow(UnaryScalarFunction fn,
             const std::vector<ExpressionValue> & args,
             const SqlRowScope & scope)
    {
        RowValue output;
        auto onVal = [&] (ColumnName columnName,
                          const ColumnName & prefix,
                          const CellValue & val,
                          Date ts)
            {
                output.emplace_back(std::move(columnName),
                                    fn(val),
                                    ts);
                return true;
            };
        args[0].forEachAtom(onVal);
            
        return std::move(output);
    }
    
    static ExpressionValue
    applyUnknown(UnaryScalarFunction fn,
                 const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope)
    {
        if (args[0].isAtom())
            return applyScalar(fn, args, scope);
        else if (args[0].isEmbedding())
            return applyEmbedding(fn, args, scope);
        else if (args[0].isRow())
            return applyRow(fn, args, scope);
        throw HttpReturnException(500, "applyRow unary scalar for unknown value",
                                  "value", args[0]);
    }
    
    static BoundFunction
    bindScalar(const Utf8String & functionName,
               UnaryScalarFunction fn,
               std::shared_ptr<ExpressionValueInfo> info,
               const std::vector<BoundSqlExpression> & args,
               const SqlBindingScope & scope)
    {
        return wrap(functionName, fn, std::move(info), applyScalar);
    }

    static BoundFunction
    bindEmbedding(const Utf8String & functionName,
                  UnaryScalarFunction fn,
                  std::shared_ptr<ExpressionValueInfo> info,
                  const std::vector<BoundSqlExpression> & args,
                  const SqlBindingScope & scope)
    {
        auto resultInfo
            = std::make_shared<EmbeddingValueInfo>
            (args[0].info->getEmbeddingShape(), info->getEmbeddingType());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyEmbedding);
    }
    
    static BoundFunction
    bindRow(const Utf8String & functionName,
            UnaryScalarFunction fn,
            std::shared_ptr<ExpressionValueInfo> info,
            const std::vector<BoundSqlExpression> & args,
            const SqlBindingScope & scope)
    {
        auto resultInfo
            = std::make_shared<EmbeddingValueInfo>
            (args[0].info->getEmbeddingShape(), info->getEmbeddingType());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyRow);
    }
    
    static BoundFunction
    bindUnknown(const Utf8String & functionName,
                UnaryScalarFunction fn,
                std::shared_ptr<ExpressionValueInfo> info,
                const std::vector<BoundSqlExpression> & args,
                const SqlBindingScope & scope)
    {
        auto resultInfo
            = std::make_shared<AnyValueInfo>();
        return wrap(functionName, fn, std::move(resultInfo),
                    applyUnknown);
    }
    
    template<typename... Names>
    void doRegister(const UnaryScalarFunction & function,
                    std::shared_ptr<ExpressionValueInfo> info,
                    std::string name,
                    Names&&... names)
    {
        auto fn = [=] (const Utf8String & functionName,
                       const std::vector<BoundSqlExpression> & args,
                       SqlBindingScope & scope)
            -> BoundFunction
            {
                try {
                    checkArgsSize(args.size(), 1);
#if 0
                    std::vector<BoundSqlExpression> boundArgs;
                    for (auto& arg : args) {
                        boundArgs.emplace_back(std::move(arg->bind(scope)));
                    }
#endif
                    if (args[0].info->isScalar())
                        return bindScalar(functionName, function,
                                          std::move(info), args,
                                          scope);
                    else if (args[0].info->isEmbedding()) {
                        return bindEmbedding(functionName, function,
                                             std::move(info), args,
                                             scope);
                    }
                    else if (args[0].info->isRow()) {
                        return bindRow(functionName, function,
                                       std::move(info), args,
                                       scope);
                    }
                    else {
                        return bindUnknown(functionName, function,
                                           std::move(info), args,
                                           scope);
                    }
                } JML_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin function "
                                         + functionName + ": "
                                         + ML::getExceptionString(),
                                         "functionName", functionName,
                                         "functionArgs", args);
                }
                
                ExcAssert(false); // silence bad compiler escape analysis
            };
        handles.push_back(registerFunction(Utf8String(name), fn));
        doRegister(function, std::forward<Names>(names)...);
    }
    
    std::vector<std::shared_ptr<void> > handles;
};


/*****************************************************************************/
/* NUMERIC UNARY SCALARS                                                     */
/*****************************************************************************/

template<typename Op>
struct RegisterBuiltinUnaryNumericScalar: public RegisterBuiltinUnaryScalar {

    static CellValue call(const CellValue & v)
    {
        if (v.empty())
            return v;
        return Op::call(v.toDouble());
    }

    template<typename... Names>
    RegisterBuiltinUnaryNumericScalar(Names&&... names)
        : RegisterBuiltinUnaryScalar(&call,
                                     std::make_shared<Float64ValueInfo>(),
                                     std::forward<Names>(names)...)
    {
    }
};

#define WRAP_UNARY_MATH_OP(name, op)             \
    struct name##Op { static double call(double v) { return op(v); } }; \
    RegisterBuiltinUnaryNumericScalar<name##Op> register##name(#name);


/*****************************************************************************/
/* BINARY SCALARS                                                            */
/*****************************************************************************/

typedef CellValue (*BinaryScalarFunction) (const CellValue & arg1, const CellValue & arg2);

/// Register a builtin function that operates on binary scalars with a
/// signature (Atom, Atom) -> Atom, to work on scalars, rows or
/// embeddings.

struct RegisterBuiltinBinaryScalar {
    template<typename... Names>
    RegisterBuiltinBinaryScalar(const BinaryScalarFunction & function,
                                std::shared_ptr<ExpressionValueInfo> info,
                                Names&&... names)
    {
        doRegister(function, std::move(info), std::forward<Names>(names)...);
    }

    void doRegister(const BinaryScalarFunction & function)
    {
    }

    typedef ExpressionValue (*Wrapper) (BinaryScalarFunction,
                                        const std::vector<ExpressionValue> & args,
                                        const SqlRowScope & scope);

    static BoundFunction wrap(const Utf8String & functionName,
                              BinaryScalarFunction fn,
                              std::shared_ptr<ExpressionValueInfo> resultInfo,
                              Wrapper wrapper)
    {
        BoundFunction result;
        result.exec =  [=] (const std::vector<ExpressionValue> & args,
                            const SqlRowScope & scope)
            -> ExpressionValue
            {
                try {
#if 0
                     std::vector<ExpressionValue> evaluatedArgs;
                     for (auto& arg : args)
                         evaluatedArgs.emplace_back(arg(scope, GET_LATEST));
#endif
                    return wrapper(fn, args, scope);
                } JML_CATCH_ALL {
                    rethrowHttpException(-1, "Executing builtin function "
                                         + functionName
                                         + ": " + ML::getExceptionString(),
                                         "functionName", functionName,
                                         "functionArgs", args);
                }
            };
        result.resultInfo = resultInfo;
        return result;
    }
    
    static ExpressionValue
    applyScalarScalar(BinaryScalarFunction fn,
                      const std::vector<ExpressionValue> & args,
                      const SqlRowScope & scope)
    {
        Date ts = calcTs(args[0], args[1]);
        return ExpressionValue(fn(args[0].getAtom(),
                                  args[1].getAtom()),
                               ts);
    }

    static ExpressionValue
    applyScalarEmbedding(BinaryScalarFunction fn,
                         const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
    {
        auto ts = calcTs(args[0], args[1]);
        CellValue v1 = args[0].getAtom();

        std::vector<CellValue> vals2 = args[1].getEmbeddingCell();
        for (auto & v: vals2) {
            v = fn(v, v);
        }
        
        return ExpressionValue(std::move(vals2), ts,
                               args[1].getEmbeddingShape());
    }

    static ExpressionValue
    applyEmbeddingScalar(BinaryScalarFunction fn,
                         const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
    {
        auto ts = calcTs(args[0], args[1]);
        CellValue v2 = args[1].getAtom();

        std::vector<CellValue> vals1 = args[0].getEmbeddingCell();
        for (auto & v: vals1) {
            v = fn(v, v2);
        }
        
        return ExpressionValue(std::move(vals1), ts,
                               args[0].getEmbeddingShape());
    }

    static ExpressionValue
    applyScalarRow(BinaryScalarFunction fn,
                   const std::vector<ExpressionValue> & args,
                   const SqlRowScope & scope)
    {
        Date lts = args[0].getEffectiveTimestamp();
        const CellValue & v1 = args[0].getAtom();

        RowValue output;

        auto onVal = [&] (ColumnName columnName,
                          const ColumnName & prefix,
                          const CellValue & val,
                          Date ts)
            {
                output.emplace_back(std::move(columnName),
                                    fn(v1, val),
                                    std::max(lts, ts));
                return true;
            };
        args[1].forEachAtom(onVal);

        return std::move(output);
    }

    static ExpressionValue
    applyRowScalar(BinaryScalarFunction fn,
                   const std::vector<ExpressionValue> & args,
                   const SqlRowScope & scope)
    {
        Date rts = args[1].getEffectiveTimestamp();
        const CellValue & v2 = args[1].getAtom();

        RowValue output;

        auto onVal = [&] (ColumnName columnName,
                          const ColumnName & prefix,
                          const CellValue & val,
                          Date ts)
            {
                output.emplace_back(std::move(columnName),
                                    fn(val, v2),
                                    std::max(rts, ts));
                return true;
            };
        args[0].forEachAtom(onVal);

        return std::move(output);
    }

    static ExpressionValue
    applyEmbeddingEmbedding(BinaryScalarFunction fn,
                            const std::vector<ExpressionValue> & args,
                            const SqlRowScope & scope)
    {
        auto ts = calcTs(args[0], args[1]);

        std::vector<CellValue> vals1 = args[0].getEmbeddingCell();
        std::vector<CellValue> vals2 = args[1].getEmbeddingCell();

        if (vals1.size() != vals2.size())
            throw HttpReturnException(400, "Attempt to apply function to "
                                      "incompatibly sized embeddings");
        for (size_t i = 0;  i < vals1.size();  ++i)
            vals1[i] = fn(vals1[i], vals2[i]);
        
        return ExpressionValue(std::move(vals1), ts,
                               args[0].getEmbeddingShape());
    }

    static ExpressionValue
    applyRowRow(BinaryScalarFunction fn,
                const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope)
    {
        throw HttpReturnException(500, "Row to row functions not done");
#if 0
        RowValue v1, v2;
        args[0].appendToRow(Coord(), v1);
        args[1].appendToRow(Coord(), v2);

        RowValue output;
        output.reserve(std::max(v1.size(), v2.size()));
        if (v1.size() == v2.size()) {
            for (size_t i = 0;  i < v1.size();  ++i) {
                const ColumnName & n1 = std::get<0>(v1[i]);
                const ColumnName & n2 = std::get<1>(v2[i]);

                if (n1 != n2)
                    break;
            }
                
        }
#endif
    }

    static ExpressionValue
    applyUnknown(BinaryScalarFunction fn,
                 const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope)
    {
        if (args[0].isAtom()) {
            if (args[1].isAtom())
                return applyScalarScalar(fn, args, scope);
            else if (args[1].isEmbedding())
                return applyScalarEmbedding(fn, args, scope);
            else if (args[1].isRow())
                return applyScalarRow(fn, args, scope);
        }
        else if (args[0].isEmbedding()) {
            if (args[1].isAtom())
                return applyEmbeddingScalar(fn, args, scope);
            else if (args[1].isEmbedding())
                return applyEmbeddingEmbedding(fn, args, scope);
        }
        else if (args[0].isRow()) {
            if (args[1].isAtom())
                return applyRowScalar(fn, args, scope);
            else if (args[1].isRow())
                return applyRowRow(fn, args, scope);
        }

        throw HttpReturnException(400, "Incompatible binary arguments");
    }

    static BoundFunction
    bindScalarScalar(const Utf8String & functionName,
                     BinaryScalarFunction fn,
                     std::shared_ptr<ExpressionValueInfo> info,
                     const std::vector<BoundSqlExpression> & args,
                     const SqlBindingScope & scope)
    {
        return wrap(functionName, fn, std::move(info), applyScalarScalar);
    }

    static BoundFunction
    bindScalarEmbedding(const Utf8String & functionName,
                        BinaryScalarFunction fn,
                        std::shared_ptr<ExpressionValueInfo> info,
                        const std::vector<BoundSqlExpression> & args,
                        const SqlBindingScope & scope)
    {
        auto resultInfo
            = std::make_shared<EmbeddingValueInfo>
            (args[0].info->getEmbeddingShape(),
             info->getEmbeddingType());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyScalarEmbedding);
    }
    
    static BoundFunction
    bindEmbeddingScalar(const Utf8String & functionName,
                        BinaryScalarFunction fn,
                        std::shared_ptr<ExpressionValueInfo> info,
                        const std::vector<BoundSqlExpression> & args,
                        const SqlBindingScope & scope)
    {
        auto resultInfo
            = std::make_shared<EmbeddingValueInfo>
            (args[1].info->getEmbeddingShape(),
             info->getEmbeddingType());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyEmbeddingScalar);
    }
    
    static BoundFunction
    bindEmbeddingEmbedding(const Utf8String & functionName,
                           BinaryScalarFunction fn,
                           std::shared_ptr<ExpressionValueInfo> info,
                           const std::vector<BoundSqlExpression> & args,
                           const SqlBindingScope & scope)
    {
        auto resultInfo
            = std::make_shared<EmbeddingValueInfo>
            (args[0].info->getEmbeddingShape(),
             info->getEmbeddingType());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyEmbeddingEmbedding);
    }
    
    static BoundFunction
    bindScalarRow(const Utf8String & functionName,
                  BinaryScalarFunction fn,
                  std::shared_ptr<ExpressionValueInfo> info,
                  const std::vector<BoundSqlExpression> & args,
                  const SqlBindingScope & scope)
    {
        auto cols = args[1].info->getKnownColumns();
        for (auto & c: cols) {
            c.valueInfo = info;
        }
        auto resultInfo
            = std::make_shared<RowValueInfo>
                (std::move(cols),
                 args[1].info->getSchemaCompleteness());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyScalarRow);
    }
    
    static BoundFunction
    bindRowScalar(const Utf8String & functionName,
                  BinaryScalarFunction fn,
                  std::shared_ptr<ExpressionValueInfo> info,
                  const std::vector<BoundSqlExpression> & args,
                  const SqlBindingScope & scope)
    {
        auto cols = args[0].info->getKnownColumns();
        for (auto & c: cols) {
            c.valueInfo = info;
        }
        auto resultInfo
            = std::make_shared<RowValueInfo>
                (std::move(cols),
                 args[0].info->getSchemaCompleteness());
        return wrap(functionName, fn, std::move(resultInfo),
                    applyRowScalar);
    }
    
    static BoundFunction
    bindRowRow(const Utf8String & functionName,
               BinaryScalarFunction fn,
               std::shared_ptr<ExpressionValueInfo> info,
               const std::vector<BoundSqlExpression> & args,
               const SqlBindingScope & scope)
    {
        throw HttpReturnException(400, "binary function bindRowRow");
    }

    static BoundFunction
    bindUnknown(const Utf8String & functionName,
                BinaryScalarFunction fn,
                std::shared_ptr<ExpressionValueInfo> info,
                const std::vector<BoundSqlExpression> & args,
                const SqlBindingScope & scope)
    {
        auto resultInfo = std::make_shared<AnyValueInfo>();
        return wrap(functionName, fn, std::move(resultInfo),
                    applyUnknown);
    }
    
    template<typename... Names>
    void doRegister(const BinaryScalarFunction & function,
                    std::shared_ptr<ExpressionValueInfo> info,
                    std::string name,
                    Names&&... names)
    {
        auto fn = [=] (const Utf8String & functionName,
                       const std::vector<BoundSqlExpression> & args,
                       SqlBindingScope & scope)
            -> BoundFunction
            {
                try {
                    checkArgsSize(args.size(), 2);
#if 0
                    std::vector<BoundSqlExpression> boundArgs;
                    for (auto& arg : args) {
                        boundArgs.emplace_back(std::move(arg->bind(scope)));
                    }
#endif
                    // Simple case... scalar to scalar
                    if (args[0].info->isScalar()
                        && args[1].info->isScalar()) {
                        return bindScalarScalar(functionName, function,
                                                std::move(info), args,
                                                scope);
                    }
                    else if (args[0].info->isScalar()) {
                        if (args[1].info->isEmbedding()) {
                            return bindScalarEmbedding(functionName, function,
                                                       std::move(info), args,
                                                       scope);
                        }
                        else if (args[1].info->isRow()) {
                            return bindScalarRow(functionName, function,
                                                 std::move(info), args,
                                                 scope);
                        }
                    }
                    else if (args[1].info->isScalar()) {
                        if (args[0].info->isEmbedding()) {
                            return bindEmbeddingScalar(functionName, function,
                                                       std::move(info), args,
                                                       scope);
                        }
                        else if (args[0].info->isRow()) {
                            return bindRowScalar(functionName, function,
                                                 std::move(info), args,
                                                 scope);
                        }
                    }
                    else {
                        if (args[0].info->isEmbedding()) {
                            return bindEmbeddingEmbedding(functionName, function,
                                                          std::move(info), args,
                                                          scope);
                        }
                        else if (args[0].info->isRow()) {
                            return bindEmbeddingEmbedding(functionName, function,
                                                          std::move(info), args,
                                                          scope);
                        }
                    }

                    return bindUnknown(functionName, function, std::move(info),
                                       args, scope);
                } JML_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin function "
                                         + functionName + ": "
                                         + ML::getExceptionString(),
                                         "functionName", functionName,
                                         "functionArgs", args);
                }
                
                ExcAssert(false); // silence bad compiler escape analysis
            };
    
        handles.push_back(registerFunction(Utf8String(name), fn));
        doRegister(function, std::forward<Names>(names)...);
    }
    
    std::vector<std::shared_ptr<void> > handles;
};

static Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2,
                   const ExpressionValue & v3)
{
    return std::max({ v1.getEffectiveTimestamp(),
                v2.getEffectiveTimestamp(),
                v3.getEffectiveTimestamp()
                });
}

typedef double (*DoubleBinaryFunction)(double, double);

ExpressionValue binaryFunction(const std::vector<ExpressionValue> & args,
                               DoubleBinaryFunction func)
{
    ExcAssertEqual(args.size(), 2);
    const auto v1 = args[0];
    const auto v2 = args[1];
    return ExpressionValue(func(v1.toDouble(), v2.toDouble()), calcTs(v1, v2));
}

ExpressionValue replaceIf(const std::vector<ExpressionValue> & args,
                          std::function<bool(double)> ifFunc)
{
    ExcAssertEqual(args.size(), 2);
    ExcAssert(args[1].isNumber());
       
    if(args[0].isArray() || args[0].isObject()) {
        RowValue rtnRow;
      
        auto onAtom = [&] (const ColumnName & columnName,
                           const ColumnName & prefix,
                           const CellValue & val,
                           Date ts)
            {
                if(!val.isNumber() || !ifFunc(val.toDouble())) { 
                    rtnRow.push_back(make_tuple(columnName, val, ts));
                } 
                else {
                    rtnRow.push_back(make_tuple(columnName, args[1].getAtom(), ts));
                }
                return true;
            };
       
        args[0].forEachAtom(onAtom);
        return ExpressionValue(std::move(rtnRow));
    } 
    else {
        if(!args[0].isNumber() || !ifFunc(args[0].toDouble()))
            return args[0];

        return args[1];
    }
}
       
       
BoundFunction replaceIfNaN(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                return replaceIf(args, [](double d) { return std::isnan(d); });
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerReplaceNaN(replaceIfNaN, "replace_nan", "replaceNan");

BoundFunction replaceIfInf(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                return replaceIf(args, [](double d) { return std::isinf(d); });
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerReplaceInf(replaceIfInf, "replace_inf", "replaceInf");

CellValue pow(const CellValue & v1, const CellValue & v2)
{
    return ::pow(v1.toDouble(), v2.toDouble());
}

static RegisterBuiltinBinaryScalar
registerPow(pow, std::make_shared<Float64ValueInfo>(), "pow");
static RegisterBuiltinBinaryScalar
registerPower(pow, std::make_shared<Float64ValueInfo>(), "power");

CellValue mod(const CellValue & v1, const CellValue & v2)
{
    return v1.toInt() % v2.toInt();
}

static RegisterBuiltinBinaryScalar
registerMod(mod, std::make_shared<IntegerValueInfo>(), "mod");

double ln(double v)
{
    if (v <= 0)
        throw HttpReturnException(400, "ln function supports positive numbers only");
    
    return std::log(v);
}

double sqrt(double v)
{
    if (v < 0)
        throw HttpReturnException(400, "sqrt function supports positive numbers only");
    
    return std::sqrt(v);
}

WRAP_UNARY_MATH_OP(ceil, std::ceil);
WRAP_UNARY_MATH_OP(floor, std::floor);
WRAP_UNARY_MATH_OP(exp, std::exp);
WRAP_UNARY_MATH_OP(abs, std::abs);
WRAP_UNARY_MATH_OP(sin, std::sin);
WRAP_UNARY_MATH_OP(cos, std::cos);
WRAP_UNARY_MATH_OP(tan, std::tan);
WRAP_UNARY_MATH_OP(asin, std::asin);
WRAP_UNARY_MATH_OP(acos, std::acos);
WRAP_UNARY_MATH_OP(atan, std::atan);
WRAP_UNARY_MATH_OP(ln, Builtins::ln);
WRAP_UNARY_MATH_OP(sqrt, Builtins::sqrt);

CellValue quantize(const CellValue & x, const CellValue & q)
{
    double v1 = x.toDouble();
    double v2 = q.toDouble();
    double ratio = ::round(v1 / v2);
    return ratio * v2;
}

static RegisterBuiltinBinaryScalar
registerQuantize(quantize, std::make_shared<Float64ValueInfo>(), "quantize");

#ifdef THIS_MUST_BE_CLARIFIED_FIRST
BoundFunction cardinality(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                double v = args[0].toDouble();
                return ExpressionValue(std::sqrt(v),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerCardinality(cardinality, "cardinality");
#endif

const float confidence = 0.8;
const float two_sided_alpha = (1-confidence) / 2;
ConfidenceIntervals cI(two_sided_alpha, "wilson");

BoundFunction binomial_ub_80(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                ExcAssert(args[0].isInteger());
                int64_t trials = args[0].toInt();
                int64_t successes = args[1].toInt();
                return ExpressionValue(cI.binomialUpperBound(trials, successes),
                                       calcTs(args[0], args[1]));
            },
            std::make_shared<Float64ValueInfo>()};
}

BoundFunction binomial_lb_80(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                ExcAssert(args[0].isInteger());
                ExcAssert(args[1].isInteger());
                int64_t trials = args[0].toInt();
                int64_t successes = args[1].toInt();
                return ExpressionValue(cI.binomialLowerBound(trials, successes),
                                       calcTs(args[0], args[1]));
            },
            std::make_shared<Float64ValueInfo>()};
}
    
static RegisterBuiltin registerBinUb80(binomial_ub_80, "binomial_ub_80");
static RegisterBuiltin registerBinLb80(binomial_lb_80, "binomial_lb_80");

BoundFunction implicit_cast(const std::vector<BoundSqlExpression> & args)
{
    /* Take any string values, and those that can be numbers are numbers,
       and those that have an empty string are null.
    */

    checkArgsSize(args.size(), 1);
        
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                if (!args[0].isString()) {
                    return args[0];
                }
                else return ExpressionValue(CellValue::parse(args[0].toUtf8String()),
                                            args[0].getEffectiveTimestamp());
            },
            std::make_shared<AtomValueInfo>()};
}

static RegisterBuiltin registerImplicitCast(implicit_cast, "implicit_cast");

BoundFunction regex_replace(const std::vector<BoundSqlExpression> & args)
{ 
    // regex_replace(string, regex, replacement)
    checkArgsSize(args.size(), 3);

    Utf8String regexStr = args[1].constantValue().toUtf8String();

    boost::u32regex regex = boost::make_u32regex(regexStr.rawData());

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 3);

                if (args[0].empty() || args[2].empty())
                    return ExpressionValue::null(calcTs(args[0], args[1], args[2]));

                std::basic_string<char32_t> matchStr = args[0].toWideString();
                std::basic_string<char32_t> replacementStr = args[2].toWideString();

                std::basic_string<int32_t>
                    matchStr2(matchStr.begin(), matchStr.end());
                std::basic_string<int32_t>
                    replacementStr2(replacementStr.begin(), replacementStr.end());

                auto result = boost::u32regex_replace(matchStr2, regex, replacementStr2);
                std::basic_string<char32_t> result2(result.begin(), result.end());

                return ExpressionValue(result2, calcTs(args[0], args[1], args[2]));
            },
            std::make_shared<Utf8StringValueInfo>()};
}

static RegisterBuiltin registerRegexReplace(regex_replace, "regex_replace");

BoundFunction regex_match(const std::vector<BoundSqlExpression> & args)
{ 
    // regex_match(string, regex)
    checkArgsSize(args.size(), 2);

    Utf8String regexStr = args[1].constantValue().toUtf8String();

    boost::u32regex regex = boost::make_u32regex(regexStr.rawData());

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                // TODO: should be able to pass utf-8 string directly in

                ExcAssertEqual(args.size(), 2);

                if (args[0].empty())
                    return ExpressionValue::null(calcTs(args[0], args[1]));
                    
                std::basic_string<char32_t> matchStr = args[0].toWideString();

                auto result = boost::u32regex_match(matchStr.begin(), matchStr.end(),
                                                    regex);
                return ExpressionValue(result, calcTs(args[0], args[1]));
            },
            std::make_shared<BooleanValueInfo>()};
}

static RegisterBuiltin registerRegexMatch(regex_match, "regex_match");

BoundFunction regex_search(const std::vector<BoundSqlExpression> & args)
{ 
    // regex_search(string, regex)
    checkArgsSize(args.size(), 2);

    Utf8String regexStr = args[1].constantValue().toUtf8String();

    boost::u32regex regex = boost::make_u32regex(regexStr.rawData());

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                // TODO: should be able to pass utf-8 string directly in

                ExcAssertEqual(args.size(), 2);

                if (args[0].empty())
                    return ExpressionValue::null(calcTs(args[0], args[1]));

                std::basic_string<char32_t> searchStr = args[0].toWideString();

                auto result
                    = boost::u32regex_search(searchStr.begin(), searchStr.end(),
                                             regex);
                return ExpressionValue(result, calcTs(args[0], args[1]));
            },
            std::make_shared<BooleanValueInfo>()};
}

static RegisterBuiltin registerRegexSearch(regex_search, "regex_search");

BoundFunction when(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value

    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                auto ts = args[0].getEffectiveTimestamp();
                return ExpressionValue(ts, ts);
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerWhen(when, "when");
#if 0
BoundFunction min_timestamp(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value


    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<BoundSqlExpression> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                auto val = args[0](scope, GET_ALL);
                return ExpressionValue(val.getMinTimestamp(),
                                       val.getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>()};
}
#endif

BoundFunction min_timestamp(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value


    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                auto val = args[0];
                return ExpressionValue(val.getMinTimestamp(),
                                       val.getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin register_min_timestamp(min_timestamp, "min_timestamp");

BoundFunction max_timestamp(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value

    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                return ExpressionValue(args[0].getMaxTimestamp(),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin register_max_timestamp(max_timestamp, "max_timestamp");

BoundFunction toTimestamp(const std::vector<BoundSqlExpression> & args)
{
    // Return a timestamp coerced from the expression
    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                return ExpressionValue(args[0].coerceToTimestamp(),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerToTimestamp(toTimestamp, "to_timestamp");

BoundFunction at(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    checkArgsSize(args.size(), 2);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                ExpressionValue result = args[0];
                result.setEffectiveTimestamp(args[1].coerceToTimestamp().toTimestamp());
                return result;
            },
            args[0].info};
}

static RegisterBuiltin registerAt(at, "at");

BoundFunction now(const std::vector<BoundSqlExpression> & args)
{
    // Return the current time as a timestamp

    checkArgsSize(args.size(), 0);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 0);
                ExpressionValue result(Date::now(), Date::negativeInfinity());
                return result;
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerNow(now, "now");
#if 0
BoundFunction temporal_min(const std::vector<BoundSqlExpression> & args)
{
    return {[] (const std::vector<BoundSqlExpression> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                return args[0](scope, GET_EARLIEST);
            },
            args[0].info};
}

static RegisterBuiltin registerTempMin(temporal_min, "temporal_min");
#endif
BoundFunction date_part(const std::vector<BoundSqlExpression> & args)
{
    // extract the requested part of a timestamp

    if (args.size() < 2 || args.size() > 3)
        throw HttpReturnException(400, "takes between two and three arguments, got " + to_string(args.size()));

    std::string timeUnitStr = args[0].constantValue().toString();

    TimeUnit timeUnit = ParseTimeUnit(timeUnitStr);

    bool constantTimezone(false);
    int constantMinute(0);
    if (args.size() == 3 && args[2].metadata.isConstant) {
        const auto& constantValue = args[2].constantValue();
        if (!constantValue.isString()) {
            throw HttpReturnException(400, "date_part expected a string as third argument, got " + constantValue.coerceToString().toUtf8String());
        }

        Iso8601Parser timeZoneParser(constantValue.coerceToString().toString());

        constantMinute = timeZoneParser.expectTimezone();
        constantTimezone = true;
    }

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssert(args.size() >= 2);
                ExcAssert(args.size() <= 3);

                Date date = args[1].coerceToTimestamp().toTimestamp();

                if (args.size() == 3) {
                    if (constantTimezone) {
                        date.addMinutes(constantMinute);
                    }
                    else {
                        const ExpressionValue& timezoneoffsetEV = args[2];
                        if (!timezoneoffsetEV.isString()) {
                            throw HttpReturnException(400, "date_part expected a string as third argument, got " + timezoneoffsetEV.coerceToString().toUtf8String());
                        }

                        Iso8601Parser timeZoneParser(timezoneoffsetEV.toString());

                        int timezoneOffset = timeZoneParser.expectTimezone();
                        date.addMinutes(timezoneOffset);
                    }      
                }

                int value = date.get(timeUnit);

                ExpressionValue result(value, Date::negativeInfinity());
                return result;
            },
            std::make_shared<IntegerValueInfo>()};
}

static RegisterBuiltin registerDate_Part(date_part, "date_part");

BoundFunction date_trunc(const std::vector<BoundSqlExpression> & args)
{
    // extract the requested part of a timestamp

    if (args.size() < 2 || args.size() > 3)
        throw HttpReturnException(400, "takes between two and three arguments, got " + to_string(args.size()));

    std::string timeUnitStr = args[0].constantValue().toString();

    TimeUnit timeUnit = ParseTimeUnit(timeUnitStr);

    bool constantTimezone(false);
    int constantMinute(0);
    if (args.size() == 3 && args[2].metadata.isConstant) {
        const auto& constantValue = args[2].constantValue();
        if (!constantValue.isString()) {
            throw HttpReturnException(400, "date_trunc expected a string as third argument, got " + constantValue.coerceToString().toUtf8String());
        }

        Iso8601Parser timeZoneParser(constantValue.coerceToString().toString());

        constantMinute = timeZoneParser.expectTimezone();
        constantTimezone = true;
    }

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssert(args.size() >= 2);
                ExcAssert(args.size() <= 3);

                Date date = args[1].coerceToTimestamp().toTimestamp();

                if (args.size() == 3) {
                    if (constantTimezone) {
                        date.addMinutes(constantMinute);
                    }
                    else {
                        const ExpressionValue& timezoneoffsetEV = args[2];
                        if (!timezoneoffsetEV.isString()) {
                            throw HttpReturnException(400, "date_trunc expected a string as third argument, got " + timezoneoffsetEV.coerceToString().toUtf8String());
                        }

                        Iso8601Parser timeZoneParser(timezoneoffsetEV.toString());

                        int timezoneOffset = timeZoneParser.expectTimezone();
                        date.addMinutes(timezoneOffset);
                    }     
                }

                Date value = date.trunc(timeUnit);

                ExpressionValue result(value, Date::negativeInfinity());
                return result;
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerdate_trunc(date_trunc, "date_trunc");

void normalize(ML::distribution<float>& val, double p)
{
    if (p == 0) {
        double n = (val != 0).count();
        val /= n;
    }
    else if (p == INFINITY) {
        val /= val.max();
    }
    else if (p <= 0.0 || !isfinite(p))
        throw HttpReturnException(500, "Invalid power for normalize() function",
                                  "p", p);
    else if (p == 2) {
        val /= val.two_norm();
    }
    else if (p == 1) {
        val /= val.total();
    }
    else {
        double total = 0.0;
        for (float f: val) {
            total += powf(f, p);
        }
        total = std::pow(total, 1.0 / p);

        val /= total;
    }
}

BoundFunction normalize(const std::vector<BoundSqlExpression> & args)
{
    // Get the current row as an embedding, and return a normalized version
    // of it.

    checkArgsSize(args.size(), 2);

    // TODO: improve performance by getting the embedding directly

    // As an input we get an embedding, which should have a fixed
    // number of values.  Check that's the case.
    auto vectorInfo = args[0].info;

    if (!vectorInfo->isScalar())
    {
        ssize_t numDims = -1; //if its a row we dont know the number of dimensions the embedding is going to have

        if (vectorInfo->isEmbedding())
        {
            return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                    // Get it as an embedding
                    ML::distribution<float> val = args.at(0).getEmbedding();
                    Date ts = args.at(0).getEffectiveTimestamp();
                    double p = args.at(1).toDouble();

                    normalize(val, p);

                    ExpressionValue result(std::move(val),
                                           ts,
                                           args.at(0).getEmbeddingShape());

                    return std::move(result);
         
            },
                    std::make_shared<EmbeddingValueInfo>
                        (vectorInfo->getEmbeddingShape(), ST_FLOAT32)};
        }    
        else
        {
            if (vectorInfo->isRow() && (args[0].info->getSchemaCompleteness() == SCHEMA_OPEN))
                throw HttpReturnException(500, "Can't normalize a row with unknown columns"); 
        
            auto columnNames = std::make_shared<std::vector<ColumnName> >();

            std::vector<KnownColumn> columns = args[0].info->getKnownColumns();
            for (auto & c: columns)
               columnNames->emplace_back(c.columnName);

             return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                // Get it as an embedding
                ML::distribution<float> val = args[0].getEmbedding();
                Date ts = args[0].getEffectiveTimestamp();
                double p = args[1].toDouble();

                normalize(val, p);

                ExpressionValue result(std::move(val),
                                       columnNames,
                                       ts);

                 return std::move(result);
            },
                     std::make_shared<EmbeddingValueInfo>(numDims)};
    
        }
    }
    else
    {
        throw HttpReturnException(500, "Can't normalize something that's not a row or embedding");
    }
        
}

static RegisterBuiltin registerNormalize(normalize, "normalize");

BoundFunction norm(const std::vector<BoundSqlExpression> & args)
{
    // Get the current row as an embedding, and return a normalized version
    // of it.

    checkArgsSize(args.size(), 2);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                // Get it as an embedding
                ML::distribution<float> val = args[0].getEmbedding();
                Date ts = args[0].getEffectiveTimestamp();

                double p = args[1].toDouble();

                if (p == 0.0) {
                    return ExpressionValue((val != 0.0).count(), ts);
                }
                else if (p == INFINITY) {
                    return ExpressionValue(val.max(), ts);
                }
                else if (p <= 0.0 || !isfinite(p))
                    throw HttpReturnException(500, "Invalid power for norm() function",
                                              "p", p);
                else if (p == 1) {
                    return ExpressionValue(val.total(), ts);
                }
                else if (p == 2) {
                    return ExpressionValue(val.two_norm(), ts);
                }
                else {
                    double total = 0.0;
                    for (float f: val) {
                        total += powf(f, p);
                    }
                    total = std::pow(total, 1.0 / p);

                    return ExpressionValue(total, ts);
                }
            },
            std::make_shared<Float64ValueInfo>()};
        
}
static RegisterBuiltin registerNorm(norm, "norm");



BoundFunction parse_json(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() > 2 || args.size() < 1)
        throw HttpReturnException(400, " takes 1 or 2 argument, got " + to_string(args.size()));

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssert(args.size() > 0 && args.size() < 3);
                auto val = args[0];
                Utf8String str = val.toUtf8String();

                JsonArrayHandling encode = PARSE_ARRAYS;

                if (args.size() > 1)
                {
                    Utf8String arrays = args[1].getField("arrays").toUtf8String();
                    if (arrays == "encode")
                      encode = ENCODE_ARRAYS;
                    else if (arrays != "parse")
                      throw HttpReturnException(400, " value of 'arrays' must be 'parse' or 'encode', got: " + arrays);
                }

                StreamingJsonParsingContext parser(str.rawString(),
                                                   str.rawData(),
                                                   str.rawLength());

                if (!parser.isObject())
                    throw HttpReturnException(400, "JSON passed to parse_json must be an object",
                                              "json", str);

                return ExpressionValue::
                    parseJson(parser, val.getEffectiveTimestamp(),
                              encode);
            },
            std::make_shared<UnknownRowValueInfo>()
            };
}

static RegisterBuiltin registerJsonDecode(parse_json, "parse_json");

BoundFunction get_bound_unpack_json(const std::vector<BoundSqlExpression> & args) 
{
    // Comma separated list, first is row name, rest are row columns
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                auto val = args.at(0);
                Utf8String str = val.toUtf8String();
                Date ts = val.getEffectiveTimestamp();

                StreamingJsonParsingContext parser(str.rawString(),
                                                   str.rawData(),
                                                   str.rawLength());

                if (!parser.isObject())
                    throw HttpReturnException(400, "JSON passed to unpack_json must be an object",
                                              "json", str);
                
                return ExpressionValue::
                    parseJson(parser, ts, ENCODE_ARRAYS);
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerUnpackJson(get_bound_unpack_json, "unpack_json");

void
ParseTokenizeArguments(Utf8String& splitchar, Utf8String& quotechar,
                       int& offset, int& limit, int& min_token_length,
                       ML::distribution<float, std::vector<float> > & ngram_range,
                       ExpressionValue& values, 
                       bool check[7], const ExpressionValue::Row & argRow)
{
    auto assertArg = [&] (size_t field, const string & name)
        {
            if (check[field])
                throw HttpReturnException(400, "Argument " + name + " is specified more than once");
            check[field] = true;
        };

    for (auto& arg : argRow) {
        const ColumnName& columnName = std::get<0>(arg);
        if (columnName == ColumnName("splitchars")) {
            assertArg(0, "splitchars");
            splitchar = std::get<1>(arg).toUtf8String();
        }
        else if (columnName == ColumnName("quotechar")) {
            assertArg(1, "quotechar");
            quotechar = std::get<1>(arg).toUtf8String();
        }
        else if (columnName == ColumnName("offset")) {
            assertArg(2, "offset");
            offset = std::get<1>(arg).toInt();
        }
        else if (columnName == ColumnName("limit")) {
            assertArg(3, "limit");
            limit = std::get<1>(arg).toInt();
        }
        else if (columnName == ColumnName("value")) {
            assertArg(4, "value");
            values = std::get<1>(arg);
        }
        else if (columnName == ColumnName("min_token_length")) {
            assertArg(5, "min_token_length");
            min_token_length = std::get<1>(arg).toInt();
        }
        else if (columnName == ColumnName("ngram_range")) {
            assertArg(6, "ngram_range");
            ngram_range = std::get<1>(arg).getEmbedding(2);
        }
        else {
            throw HttpReturnException(400, "Unknown argument in tokenize", "argument", columnName);
        }
    }
}

BoundFunction tokenize(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() == 0)
        throw HttpReturnException(400, "requires at least one argument");

    if (args.size() > 2)
        throw HttpReturnException(400, "requires at most two arguments");


    // Comma separated list, first is row name, rest are row columns
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                Date ts = args[0].getEffectiveTimestamp();

                Utf8String text = args[0].toUtf8String();

                Utf8String splitchar = ",";
                Utf8String quotechar = "\"";
                int offset = 0;
                int limit = -1;
                int min_token_length = 1;
                ML::distribution<float, std::vector<float> > ngram_range = {1, 1};
                ExpressionValue values;
                bool check[] = {false, false, false, false, false, false, false};
                
                if (args.size() == 2)
                    ParseTokenizeArguments(splitchar, quotechar, offset, limit,
                                           min_token_length, ngram_range, values, 
                                           check, args.at(1).getRow());

                ML::Parse_Context pcontext(text.rawData(), text.rawData(), text.rawLength());

                std::unordered_map<Utf8String, int> bagOfWords;

                tokenize(bagOfWords, pcontext, splitchar, quotechar, offset, limit,
                        min_token_length, ngram_range);

                RowValue row;
                row.reserve(bagOfWords.size());

                auto it = bagOfWords.begin();

                while (it != bagOfWords.end()) {
                    if (check[4]) //values
                    {
                        if (!values.isAtom())
                          throw HttpReturnException(400, "requires 'value' argument be a scalar, got type " + values.getTypeAsUtf8String());

                        row.emplace_back(ColumnName(it->first),
                                     values.getAtom(),
                                     ts);
                        ++it;
                    }
                    else
                    {
                        row.emplace_back(ColumnName(it->first),
                                     it->second,
                                     ts);
                        ++it;
                    }
                                         
                }

                return ExpressionValue(std::move(row));
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerTokenize(tokenize, "tokenize");

BoundFunction token_extract(const std::vector<BoundSqlExpression> & args)
{
    // Comma separated list, first is row name, rest are row columns

    if (args.size() < 2)
        throw HttpReturnException(400, "requires at least two arguments");

    if (args.size() > 3)
        throw HttpReturnException(400, "requires at most three arguments");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                Date ts = args[0].getEffectiveTimestamp();

                Utf8String text = args[0].toUtf8String();

                Utf8String splitchar = ",";
                Utf8String quotechar = "\"";
                int offset = 0;
                int limit = 1;
                int min_token_length = 1;
                ML::distribution<float, std::vector<float> > ngram_range;
                ExpressionValue values;
                bool check[] = {false, false, false, false, false, false, false};                

                int nth = args.at(1).toInt();

                if (args.size() == 3)
                    ParseTokenizeArguments(splitchar, quotechar, offset, limit, min_token_length,
                                           ngram_range, values, check, args.at(2).getRow());

                ML::Parse_Context pcontext(text.rawData(), text.rawData(), text.rawLength());

                ExpressionValue result;

                Utf8String output = token_extract(pcontext, splitchar, quotechar, offset, limit,
                        nth, min_token_length);
        
                if (output != "")
                    result = ExpressionValue(output, ts);
             
                return std::move(result);
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerToken_extract(token_extract, "token_extract");

BoundFunction horizontal_count(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                size_t result = 0;
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Coord & columnName,
                                   const Coord & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            result += 1;
                            ts.setMax(atomTs);
                        }
                        return true;
                    };
                
                args.at(0).forEachAtom(onAtom);
                
                return ExpressionValue(result, ts);
            },
            std::make_shared<Float64ValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Count(horizontal_count, "horizontal_count");

BoundFunction horizontal_sum(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                double result = 0;
                Date ts = Date::negativeInfinity();
                
                auto onAtom = [&] (const Coord & columnName,
                                   const Coord & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            result += val.toDouble();
                            ts.setMax(atomTs);
                        }
                        return true;
                    };
                
                args.at(0).forEachAtom(onAtom);
                
                return ExpressionValue(result, ts);
            },
            std::make_shared<Float64ValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Sum(horizontal_sum, "horizontal_sum");

BoundFunction horizontal_avg(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                int64_t num_cols = 0;
                double accum = 0;
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Coord & columnName,
                                   const Coord & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            num_cols++;
                            accum += val.toDouble();
                            ts.setMax(atomTs);
                        }
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return ExpressionValue(ML::xdiv(accum, num_cols), ts);
            },
            std::make_shared<Float64ValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Avg(horizontal_avg, "horizontal_avg");

BoundFunction horizontal_min(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                double min_val = nan("");
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Coord & columnName,
                                   const Coord & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            double curr = val.toDouble();
                            if(std::isnan(min_val) || curr < min_val) {
                                ts = atomTs;
                                min_val = curr;
                            }
                        }
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return ExpressionValue(min_val, ts);
            },
            std::make_shared<Float64ValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Min(horizontal_min, "horizontal_min");

BoundFunction horizontal_max(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                double max_val = nan("");
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Coord & columnName,
                                   const Coord & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            double curr = val.toDouble();
                            if(std::isnan(max_val) || curr > max_val) {
                                ts = atomTs;
                                max_val = curr;
                            }
                        }
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return ExpressionValue(max_val, ts);
            },
            std::make_shared<Float64ValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Max(horizontal_max, "horizontal_max");

struct DiffOp {
    static ML::distribution<double> apply(ML::distribution<double> & d1,
                                          ML::distribution<double> & d2)
    {
        return d1 - d2;
    }
};

struct SumOp {
    static ML::distribution<double> apply(ML::distribution<double> & d1,
                                          ML::distribution<double> & d2)
    {
        return d1 + d2;
    }
};

struct ProductOp {
    static ML::distribution<double> apply(ML::distribution<double> & d1,
                                          ML::distribution<double> & d2)
    {
        return d1 * d2;
    }
};

struct QuotientOp {
    static ML::distribution<double> apply(ML::distribution<double> & d1,
                                          ML::distribution<double> & d2)
    {
        return d1 / d2;
    }
};

template<typename Op>
struct RegisterVectorOp {
    RegisterVectorOp(const std::string & name)
    {
        static RegisterBuiltin doRegister(&create, name);
    }

    static BoundFunction create(const std::vector<BoundSqlExpression> & args)
    {
        // Get the current row as an embedding, and return a normalized version
        // of it.

        ExcAssertEqual(args.size(), 2);

        //cerr << "vector_diff arg 0 = " << jsonEncode(args[0]) << endl;
        //cerr << "vector_diff arg 1 = " << jsonEncode(args[1]) << endl;

        return {[] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope) -> ExpressionValue
                {
                    //cerr << "val1 = " << jsonEncode(args.at(0)) << endl;
                    //cerr << "val2 = " << jsonEncode(args.at(1)) << endl;

                    // Get it as an embedding
                    const auto expr1 = args[0]; //(scope, GET_LATEST);
                    const auto expr2 = args[1]; //(scope, GET_LATEST);
                    ML::distribution<double> val1 = expr1.getEmbeddingDouble();
                    ML::distribution<double> val2 = expr2.getEmbeddingDouble();
                    Date ts = calcTs(expr1, expr2);

                    return ExpressionValue(std::move(Op::apply(val1, val2)), ts);
                },
                std::make_shared<UnknownRowValueInfo>()};
    }
};

RegisterVectorOp<DiffOp> registerVectorDiff("vector_diff");
RegisterVectorOp<SumOp> registerVectorSum("vector_sum");
RegisterVectorOp<ProductOp> registerVectorProduct("vector_product");
RegisterVectorOp<QuotientOp> registerVectorQuotient("vector_quotient");

void
ParseConcatArguments(Utf8String& separator, bool& columnValue,
                     const ExpressionValue::Row & argRow)
{
    bool check[3] = {false, false, false};
    auto assertArg = [&] (size_t field, const string & name) {
        if (check[field]) {
            throw HttpReturnException(
                400, "Argument " + name + " is specified more than once");
        }
        check[field] = true;
    };

    for (const auto &arg : argRow) {
        const ColumnName& columnName = std::get<0>(arg);
        if (columnName == ColumnName("separator")) {
            assertArg(1, "separator");
            separator = std::get<1>(arg).toUtf8String();
        }
        else if (columnName == ColumnName("columnValue")) {
            assertArg(2, "columnValue");
            columnValue = std::get<1>(arg).asBool();
        }
        else {
            throw HttpReturnException(400, "Unknown argument in concat",
                                      "argument", columnName);
        }
    }
}

BoundFunction concat(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() == 0) {
        throw HttpReturnException(
            400, "requires at least one argument");
    }

    if (args.size() > 2) {
        throw HttpReturnException(
            400, "requires at most two arguments");
    }

    Utf8String separator(",");
    bool columnValue = true;

    if (args.size() == 2) {
        SqlRowScope emptyScope;
        ParseConcatArguments(separator, columnValue,
                             args[1](emptyScope, GET_LATEST).getRow());
    }

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
        {
            Utf8String result = "";
            Date ts = Date::negativeInfinity();
            bool first = true;
            auto onAtom = [&] (const Coord & columnName,
                               const Coord & prefix,
                               const CellValue & val,
                               Date atomTs)
            {
                if (!val.empty()) {
                    if (first) {
                        first = false;
                    }
                    else {
                        result += separator;
                    }
                    result += columnValue ?
                        val.toUtf8String() : columnName.toUtf8String();
                }
                return true;
            };

            args.at(0).forEachAtom(onAtom);
            return ExpressionValue(result, ts);
        },
        std::make_shared<UnknownRowValueInfo>()
    };
}
static RegisterBuiltin registerConcat(concat, "concat");

BoundFunction base64_encode(const std::vector<BoundSqlExpression> & args)
{
    // Convert a blob into base64
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);

                Utf8String str = args[0].toUtf8String();
                return ExpressionValue(base64Encode(str.rawData(),
                                                    str.rawLength()),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<StringValueInfo>()
            };
}

static RegisterBuiltin registerBase64Encode(base64_encode, "base64_encode");

BoundFunction base64_decode(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                CellValue blob = args[0].coerceToBlob();
                return ExpressionValue(base64Decode((const char *)blob.blobData(),
                                                    blob.blobLength()),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<BlobValueInfo>()
            };
}

static RegisterBuiltin registerBase64Decode(base64_decode, "base64_decode");

BoundFunction extract_column(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() != 2)
        throw HttpReturnException(400, "extract_column function takes 2 arguments");

    // TODO: there is a better implementation if the field name is
    // a constant expression

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                auto val1 = args[0];
                auto val2 = args[1];
                Utf8String fieldName = val1.toUtf8String();
                cerr << "extracting " << jsonEncodeStr(val1)
                     << " from " << jsonEncodeStr(val2) << endl;

                return args[1].getField(fieldName);
            },
            std::make_shared<AnyValueInfo>()
            };
}

static RegisterBuiltin registerExtractColumn(extract_column, "extract_column");

BoundFunction lower(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    if (args.size() != 1)
        throw HttpReturnException(400, "lower function takes 1 argument");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                ExpressionValue result(args[0].getAtom().toUtf8String().toLower(),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<Utf8StringValueInfo>()
            };
}

static RegisterBuiltin registerLower(lower, "lower");

BoundFunction upper(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    if (args.size() != 1)
        throw HttpReturnException(400, "upper function takes 1 argument");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                ExpressionValue result(args[0].getAtom().toUtf8String().toUpper(),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<Utf8StringValueInfo>()
    };
}

static RegisterBuiltin registerUpper(upper, "upper");

BoundFunction flatten(const std::vector<BoundSqlExpression> & args)
{
    // Return the result indexed on a single dimension

    checkArgsSize(args.size(), 1);

    std::vector<ssize_t> shape = args[0].info->getEmbeddingShape();

    ssize_t outputShape = 1;
    for (auto s: shape) {
        if (s < 0) {
            outputShape = -1;
            break;
        }
        outputShape *= s;
    }
    auto st = args[0].info->getEmbeddingType();

    auto outputInfo
        = std::make_shared<EmbeddingValueInfo>(outputShape, st);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                size_t len = args[0].rowLength();
                return args[0].reshape({len});
            },
            outputInfo
        };
}

static RegisterBuiltin registerFlatten(flatten, "flatten");


} // namespace Builtins
} // namespace MLDB
} // namespace Datacratic

