/** builtin_functions.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Builtin functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "sql_expression.h"
#include "tokenize.h"
#include "regex_helper.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/stats/distribution_simd.h"
#include "mldb/jml/utils/csv.h"
#include "mldb/types/vector_description.h"
#include "mldb/ml/confidence_intervals.h"
#include "mldb/jml/math/xdiv.h"
#include "mldb/base/hash.h"
#include "mldb/base/parse_context.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/binding_contexts.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/clamp.hpp>
#include "mldb/ext/edlib/edlib/include/edlib.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/http/curl_wrapper.h"

#include <iterator>
#include <thread>
#include <mutex>
#include <atomic>

using namespace std;



namespace MLDB {

const Utf8String NO_FUNCTION_NAME;

CellValue getArg(const std::vector<ExpressionValue> & args,
                 size_t n,
                 const char * name,
                 const CellValue & def)
{
    if (args.size() > n)
        return args[n].getAtom();
    return def;
}

namespace Builtins {


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
                    return wrapper(fn, args, scope);
                } MLDB_CATCH_ALL {
                    rethrowHttpException(-1, "Executing builtin function "
                                         + functionName
                                         + ": " + getExceptionString(),
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
        auto onVal = [&] (ColumnPath columnName,
                          const ColumnPath & prefix,
                          const CellValue & val,
                          Date ts)
            {
                output.emplace_back(prefix + std::move(columnName),
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
                } MLDB_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin function "
                                         + functionName + ": "
                                         + getExceptionString(),
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
                    return wrapper(fn, args, scope);
                } MLDB_CATCH_ALL {
                    rethrowHttpException(-1, "Executing builtin function "
                                         + functionName
                                         + ": " + getExceptionString(),
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

        auto onVal = [&] (ColumnPath columnName,
                          const ColumnPath & prefix,
                          const CellValue & val,
                          Date ts)
            {
                output.emplace_back(prefix + std::move(columnName),
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

        auto onVal = [&] (ColumnPath columnName,
                          const ColumnPath & prefix,
                          const CellValue & val,
                          Date ts)
            {
                output.emplace_back(prefix + std::move(columnName),
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
        args[0].appendToRow(PathElement(), v1);
        args[1].appendToRow(PathElement(), v2);

        RowValue output;
        output.reserve(std::max(v1.size(), v2.size()));
        if (v1.size() == v2.size()) {
            for (size_t i = 0;  i < v1.size();  ++i) {
                const ColumnPath & n1 = std::get<0>(v1[i]);
                const ColumnPath & n2 = std::get<1>(v2[i]);

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
                } MLDB_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin function "
                                         + functionName + ": "
                                         + getExceptionString(),
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

typedef double (*DoubleBinaryFunction)(double, double);

ExpressionValue binaryFunction(const std::vector<ExpressionValue> & args,
                               DoubleBinaryFunction func)
{
    checkArgsSize(args.size(), 2);
    const auto v1 = args[0];
    const auto v2 = args[1];
    return ExpressionValue(func(v1.toDouble(), v2.toDouble()), calcTs(v1, v2));
}

CellValue replaceIfNan(const CellValue & v1, const CellValue & v2)
{
    if (v1.empty())
        return v1;
    if (!v1.isNumber())
        return v1;
    if (std::isnan(v1.toDouble()))
        return v2;
    return v1;
}

CellValue replaceIfInf(const CellValue & v1, const CellValue & v2)
{
    if (v1.empty())
        return v1;
    if (!v1.isNumber())
        return v1;
    if (std::isinf(v1.toDouble()))
        return v2;
    return v1;
}

CellValue replaceIfNotFinite(const CellValue & v1, const CellValue & v2)
{
    if (v1.empty())
        return v1;
    if (!v1.isNumber())
        return v1;
    if (!std::isfinite(v1.toDouble()))
        return v2;
    return v1;
}

CellValue replaceIfNull(const CellValue & v1, const CellValue & v2)
{
    if (v1.empty())
        return v2;
    return v1;
}

static RegisterBuiltinBinaryScalar
registerReplaceIfNan(replaceIfNan, std::make_shared<AtomValueInfo>(),
                     "replace_nan");

static RegisterBuiltinBinaryScalar
registerReplaceIfInf(replaceIfInf, std::make_shared<AtomValueInfo>(),
                     "replace_inf");

static RegisterBuiltinBinaryScalar
registerReplaceIfNotFinite(replaceIfNotFinite, std::make_shared<AtomValueInfo>(),
                           "replace_not_finite");

static RegisterBuiltinBinaryScalar
registerReplaceIfNull(replaceIfNull, std::make_shared<AtomValueInfo>(),
                      "replace_null");

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

CellValue atan2(const CellValue & v1, const CellValue & v2)
{
    return std::atan2(v1.toDouble(), v2.toDouble());
}

static RegisterBuiltinBinaryScalar
registerAtan2(atan2, std::make_shared<Float64ValueInfo>(), "atan2");

double ln(double v)
{
    return std::log(v);
}

// log function consistent with postgresql's
BoundFunction log(const std::vector<BoundSqlExpression> & args)
{
    // log(x) (base 10)
    if (args.size() == 1) {
        return {[] (const std::vector<ExpressionValue> & args,
                    const SqlRowScope & scope) -> ExpressionValue
                {
                    ExcAssertEqual(args.size(), 1);

                    if (args[0].empty())
                        return ExpressionValue();

                    return ExpressionValue(std::log10(args[0].toDouble()),
                                           args[0].getEffectiveTimestamp());
                },
                std::make_shared<Float64ValueInfo>()};
    // log(base, x)
    } else if (args.size() == 2) {
        return {[] (const std::vector<ExpressionValue> & args,
                    const SqlRowScope & scope) -> ExpressionValue
                {
                    ExcAssertEqual(args.size(), 2);

                    if (args[0].empty() || args[1].empty())
                        return ExpressionValue();

                    double base = args[0].toDouble();
                    double x = args[1].toDouble();
                    return ExpressionValue(std::log(x) / std::log(base),
                                           calcTs(args[0], args[1]));
                },
                std::make_shared<Float64ValueInfo>()};
    // wrong number of arguments
    } else {
        throw HttpReturnException(400,
            "the log function expected 1 or 2 arguments, got "
            + to_string(args.size()));
    }
}

static RegisterBuiltin registerLog(log, "log");

double sqrt(double v)
{
    return std::sqrt(v);
}

WRAP_UNARY_MATH_OP(ceil, std::ceil);
WRAP_UNARY_MATH_OP(floor, std::floor);
WRAP_UNARY_MATH_OP(round, std::round);
WRAP_UNARY_MATH_OP(exp, std::exp);
WRAP_UNARY_MATH_OP(abs, std::abs);
WRAP_UNARY_MATH_OP(sin, std::sin);
WRAP_UNARY_MATH_OP(cos, std::cos);
WRAP_UNARY_MATH_OP(tan, std::tan);
WRAP_UNARY_MATH_OP(asin, std::asin);
WRAP_UNARY_MATH_OP(acos, std::acos);
WRAP_UNARY_MATH_OP(atan, std::atan);
WRAP_UNARY_MATH_OP(sinh, std::sinh);
WRAP_UNARY_MATH_OP(cosh, std::cosh);
WRAP_UNARY_MATH_OP(tanh, std::tanh);
WRAP_UNARY_MATH_OP(asinh, std::asinh);
WRAP_UNARY_MATH_OP(acosh, std::acosh);
WRAP_UNARY_MATH_OP(atanh, std::atanh);
WRAP_UNARY_MATH_OP(ln, Builtins::ln);
WRAP_UNARY_MATH_OP(sqrt, Builtins::sqrt);
WRAP_UNARY_MATH_OP(isfinite, std::isfinite);
WRAP_UNARY_MATH_OP(isinf, std::isinf);
WRAP_UNARY_MATH_OP(isnan, std::isnan);

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
                checkArgsSize(args.size(), 1);
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
                checkArgsSize(args.size(), 2);
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
                checkArgsSize(args.size(), 2);
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
                checkArgsSize(args.size(), 1);
                if (!args[0].isString()) {
                    return args[0];
                }
                else return ExpressionValue(CellValue::parse(args[0].toUtf8String()),
                                            args[0].getEffectiveTimestamp());
            },
            std::make_shared<AtomValueInfo>()};
}

static RegisterBuiltin registerImplicitCast(implicit_cast, "implicit_cast");

BoundFunction remove_prefix(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 2);
                if (!args[0].isString() || !args[1].isString()) {
                     throw MLDB::Exception("The arguments passed to remove_prefix must be two strings");
                }
                else {
                    Utf8String text = args[0].toUtf8String();
                    text.removePrefix(args[1].toUtf8String());
                    return ExpressionValue(std::move(text),
                                           args[0].getEffectiveTimestamp());
                }
            },
            std::make_shared<Utf8StringValueInfo>()};
}

static RegisterBuiltin registerRemovePrefix(remove_prefix, "remove_prefix");

BoundFunction remove_suffix(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 2);
                if (!args[0].isString() || !args[1].isString()) {
                     throw MLDB::Exception("The arguments passed to remove_suffix must be two strings");
                }
                else {
                    Utf8String text = args[0].toUtf8String();
                    text.removeSuffix(args[1].toUtf8String());
                    return ExpressionValue(std::move(text),
                                           args[0].getEffectiveTimestamp());
                }
            },
            std::make_shared<Utf8StringValueInfo>()};
}

static RegisterBuiltin registerRemoveSuffix(remove_suffix, "remove_suffix");

BoundFunction regex_replace(const std::vector<BoundSqlExpression> & args)
{
    // regex_replace(string, regex, replacement)
    checkArgsSize(args.size(), 3);

    return {ApplyRegexReplace(args[1]),
            std::make_shared<Utf8StringValueInfo>()};
}

static RegisterBuiltin registerRegexReplace(regex_replace, "regex_replace");

BoundFunction regex_match(const std::vector<BoundSqlExpression> & args)
{
    // regex_match(string, regex)
    checkArgsSize(args.size(), 2);

    return {ApplyRegexMatch(args[1]),
            std::make_shared<BooleanValueInfo>()};
}

static RegisterBuiltin registerRegexMatch(regex_match, "regex_match");

BoundFunction regex_search(const std::vector<BoundSqlExpression> & args)
{
    // regex_search(string, regex)
    checkArgsSize(args.size(), 2);

    return {ApplyRegexSearch(args[1]),
            std::make_shared<BooleanValueInfo>()};
}

static RegisterBuiltin registerRegexSearch(regex_search, "regex_search");

BoundFunction earliest_timestamp(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value


    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                auto val = args[0];
                return ExpressionValue(val.getMinTimestamp(),
                                       val.getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>(),
            GET_ALL};
}

static RegisterBuiltin register_earliest_timestamp(earliest_timestamp, "earliest_timestamp");

BoundFunction latest_timestamp(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value

    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                return ExpressionValue(args[0].getMaxTimestamp(),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin register_latest_timestamp(latest_timestamp, "latest_timestamp");

BoundFunction distinct_timestamps(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);

                std::set<CellValue> results;

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty())
                            results.insert(atomTs);
                        return true;
                    };

                args[0].forEachAtom(onAtom);

                std::vector<CellValue> embedding(results.begin(), results.end());
                return ExpressionValue(std::move(embedding),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<EmbeddingValueInfo>(),
            GET_ALL};
}

static RegisterBuiltin register_distinct_timestamps(distinct_timestamps, "distinct_timestamps");

BoundFunction toTimestamp(const std::vector<BoundSqlExpression> & args)
{
    // Return a timestamp coerced from the expression
    checkArgsSize(args.size(), 1, "to_timestamp");
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
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
                checkArgsSize(args.size(), 2);
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
                checkArgsSize(args.size(), 0);
                ExpressionValue result(Date::now(), Date::negativeInfinity());
                return result;
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerNow(RegisterBuiltin::NON_DETERMINISTIC, now, "now");

BoundFunction temporal_earliest(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                return args[0];
            },
            args[0].info,
            GET_EARLIEST};
}

static RegisterBuiltin registerTempEarliest(temporal_earliest, "temporal_earliest");

BoundFunction temporal_latest(const std::vector<BoundSqlExpression> & args)
{
    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                return args[0];
            },
            args[0].info,
            GET_LATEST};
}

static RegisterBuiltin registerTempLatest(temporal_latest, "temporal_latest");

template <typename AggregatorFunc>
BoundFunction temporalAggregatorT(const std::vector<BoundSqlExpression> & args)
{
    typedef typename AggregatorFunc::value_type value_type;

    checkArgsSize(args.size(), 1);
    auto info = args[0].info;

    // What we do depends upon whether we have a scalar or row value in the
    // info.
    bool extractScalar = info->isScalar();

    auto apply = [=] (const std::vector<ExpressionValue> & args,
                      const SqlRowScope & scope) -> ExpressionValue
        {
            checkArgsSize(args.size(), 1);

            const ExpressionValue & val = args[0];

            auto applyAggregator = [&] (value_type current,
                                        const ExpressionValue & val)
            {
                auto onColumn = [&] (const ExpressionValue & val)
                {
                    current = AggregatorFunc::apply(current, val);
                    return true;
                };

                val.forEachSuperposedValue(onColumn);

                return current;
            };

            if (val.empty()) {
                return val;
            } else if (val.isAtom()) {
                return AggregatorFunc::extract
                (AggregatorFunc::apply(AggregatorFunc::init(), val));
            } else if (val.isRow()) {
                // TODO - figure out what should be the ordering of the columns in
                // the result
                std::unordered_map<PathElement, value_type> results;

                auto onColumn = [&] (const PathElement & columnName,
                                     const ExpressionValue & val)
                {
                    if (val.empty())
                        return true;

                    auto iter = results.find(columnName);
                    if (iter == results.end()) {
                        iter = results.emplace(columnName, AggregatorFunc::init()).first;
                    }
                    iter->second = applyAggregator(iter->second, val);
                    return true;
                };

                val.forEachColumn(onColumn);

                if (extractScalar) {
                    if (results.size() != 1) {
                        throw HttpReturnException
                            (500, "Problem with output determination for temporal agg",
                             "info", info,
                             "input", val);
                    }
                    
                    return AggregatorFunc::extract(results.begin()->second);
                }
                else {
                    StructValue row;
                    for (auto & result : results) {
                        row.emplace_back(result.first,
                                         AggregatorFunc::extract(result.second));
                    }
                    return std::move(row);
                }

            } else {
                throw HttpReturnException
                (500, "temporal aggregators invoked on unknown type",
                 "value", val);
            }
        };

    return {apply,
            std::make_shared<UnknownRowValueInfo>(),
            GET_ALL};
}


BoundFunction jaccard_index(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() != 2)
        throw HttpReturnException(500, "jaccard_index function takes two arguments");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                if(!args[0].isRow() || !args[1].isRow())
                    throw MLDB::Exception("The arguments passed to the jaccard_index must be two "
                        "row expressions");

                set<Path> a, b;
                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (val.empty())
                            return true;

                        a.insert(columnName);
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);
                b = std::move(a);
                args.at(1).forEachAtom(onAtom);

                if(a.size() == 0 && b.size() == 0)
                    return ExpressionValue(1, Date::now());

                vector<Path> intersect(a.size() + b.size());
                auto it=std::set_intersection(a.begin(), a.end(), b.begin(), b.end(), intersect.begin());
                ssize_t intersect_size = it-intersect.begin();

                const double union_size = a.size() + b.size() - intersect_size;
                double index = intersect_size / union_size;

                return ExpressionValue(index, Date::now());
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerJaccard_Index(jaccard_index, "jaccard_index");





namespace {

struct Min {
    typedef ExpressionValue value_type;

    static ExpressionValue init() { return ExpressionValue(); }

    static ExpressionValue apply(const ExpressionValue & left,
                                 const ExpressionValue & right)
    {
        if (left.empty())
            return right;
        if (right.empty())
            return left;
        return right < left ? right : left;
    }

    static ExpressionValue extract(ExpressionValue val)
    {
        return val;
    }
};

static RegisterBuiltin registerTempMin(temporalAggregatorT<Min>, "temporal_min");

struct Max {
    typedef ExpressionValue value_type;
    static ExpressionValue init() { return ExpressionValue(); }
    static ExpressionValue
    apply(const ExpressionValue & left,
          const ExpressionValue & right)
    {
        if (left.empty())
            return right;
        if (right.empty())
            return left;
        return right > left ? right : left;
    }
    static ExpressionValue extract(ExpressionValue val)
    {
        return val;
    }
};

static RegisterBuiltin registerTempMax(temporalAggregatorT<Max>, "temporal_max");

struct Sum {
    typedef ExpressionValue value_type;
    static ExpressionValue init() { return ExpressionValue(); }
    static ExpressionValue
    apply(const ExpressionValue & left, const ExpressionValue & right)
    {
        if (left.empty())
            return right;
        if (right.empty())
            return left;
        double value = left.toDouble() + right.toDouble();
        Date ts = left.getEffectiveTimestamp();
        ts.setMax(right.getEffectiveTimestamp());
        return ExpressionValue(value, ts);
    }

    static ExpressionValue extract(ExpressionValue val)
    {
        return val;
    }
};

static RegisterBuiltin registerTempSum(temporalAggregatorT<Sum>, "temporal_sum");

struct Avg {
    typedef std::pair<ExpressionValue, uint64_t> value_type;
    static value_type init()
    {
        return {ExpressionValue(), 0};
    }
    static value_type
    apply(const value_type & left, const ExpressionValue & right)
    {
        if (right.empty())
            return left;
        if (left.first.empty())
            return { right, 1 };
        auto sum = left.first.toDouble() + right.toDouble();
        auto count = left.second + 1;
        auto ts = left.first.getEffectiveTimestamp();
        ts.setMax(right.getEffectiveTimestamp());
        return {ExpressionValue(sum, ts), count};
    }
    static ExpressionValue extract(const value_type & val)
    {
        return ExpressionValue(val.first.toDouble() / val.second,
                               val.first.getEffectiveTimestamp());
    }
};

static RegisterBuiltin registerTempAvg(temporalAggregatorT<Avg>, "temporal_avg");

struct Count {
    typedef ExpressionValue value_type;
    static ExpressionValue init()
    {
        return ExpressionValue();
    }

    static ExpressionValue
    apply(const ExpressionValue & left, const ExpressionValue & right)
    {
        if (right.empty())
            return left;
        if (left.empty())
            return ExpressionValue(1, right.getEffectiveTimestamp());
        auto value = left.toInt();
        Date ts = left.getEffectiveTimestamp();
        ts.setMax(right.getEffectiveTimestamp());
        return ExpressionValue(++value, ts);
    }

    static ExpressionValue extract(ExpressionValue val)
    {
        return val;
    }
};

static RegisterBuiltin registerTempCount(temporalAggregatorT<Count>, "temporal_count");

} // file scope



BoundFunction date_part(const std::vector<BoundSqlExpression> & args)
{
    // extract the requested part of a timestamp

    if (args.size() < 2 || args.size() > 3)
        throw HttpReturnException(400, "takes between two and three arguments, got " + to_string(args.size()));

    std::string timeUnitStr = args[0].constantValue().toString();

    TimeUnit timeUnit = ParseTimeUnit(timeUnitStr);

    bool constantTimezone(false);
    int constantMinute(0);
    if (args.size() == 3 && args[2].info->isConst()) {
        const auto& constantValue = args[2].constantValue();
        if (!constantValue.isString()) {
            throw HttpReturnException(400, "date_part expected a string as third argument, got " +
                    constantValue.coerceToString().toUtf8String());
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
                            throw HttpReturnException(400, "date_part expected a string as third argument, got " +
                                    timezoneoffsetEV.coerceToString().toUtf8String());
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
    if (args.size() == 3 && args[2].info->isConst()) {
        const auto& constantValue = args[2].constantValue();
        if (!constantValue.isString()) {
            throw HttpReturnException(400, "date_trunc expected a string as third argument, got " +
                    constantValue.coerceToString().toUtf8String());
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

void normalize(distribution<double>& val, double p)
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

     if (!vectorInfo->isScalar()) {
         if (vectorInfo->isEmbedding()) {
             return {[=] (const std::vector<ExpressionValue> & args,
                          const SqlRowScope & scope) -> ExpressionValue
                     {
                         // Get it as an embedding
                         distribution<double> val
                             = args.at(0).getEmbeddingDouble();
                         Date ts = args.at(0).getEffectiveTimestamp();
                         double p = args.at(1).toDouble();

                         normalize(val, p);

                         ExpressionValue result(std::move(val),
                                                ts,
                                                args.at(0).getEmbeddingShape());

                         return result;

                     },
                     std::make_shared<EmbeddingValueInfo>
                         (vectorInfo->getEmbeddingShape(), ST_FLOAT32)};
         }
         else {
             if (vectorInfo->isRow()
                 && (args[0].info->getSchemaCompleteness() == SCHEMA_OPEN))
                 throw HttpReturnException
                     (500, "Can't normalize a row with unknown columns");

             auto columnNames = std::make_shared<std::vector<ColumnPath> >();

             std::vector<KnownColumn> columns = args[0].info->getKnownColumns();
             for (auto & c: columns)
                 columnNames->emplace_back(c.columnName);

             size_t numDims = -1;
             if (args[0].info->getSchemaCompleteness() == SCHEMA_CLOSED)
                 numDims = columnNames->size();
             
             return {[=] (const std::vector<ExpressionValue> & args,
                          const SqlRowScope & scope) -> ExpressionValue
                     {
                         // Get it as an embedding
                         distribution<double> val = args[0].getEmbeddingDouble();
                         Date ts = args[0].getEffectiveTimestamp();
                         double p = args[1].toDouble();

                         normalize(val, p);

                         ExpressionValue result(std::move(val), columnNames,  ts);

                         return result;
                     },
                     std::make_shared<EmbeddingValueInfo>(numDims)};
         }
     }
     else {
         throw HttpReturnException
             (500, "Can't normalize something that's not a row or embedding");
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
                distribution<double> val = args[0].getEmbeddingDouble();
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


struct ParseJsonOptions {
    bool ignoreErrors = false;
    JsonArrayHandling arrays = PARSE_ARRAYS;
};

DECLARE_STRUCTURE_DESCRIPTION(ParseJsonOptions);
DEFINE_STRUCTURE_DESCRIPTION(ParseJsonOptions);

ParseJsonOptionsDescription::
ParseJsonOptionsDescription()
{
    addAuto("ignoreErrors", &ParseJsonOptions::ignoreErrors,
            "If true, errors in the JSON are ignored and the element with "
            "an error will be silently ignored.  If false (the default), "
            "a JSON format error will lead to the function failing with "
            "an exception.");
    addAuto("arrays", &ParseJsonOptions::arrays,
            "Describes how arrays are encoded in the JSON output.  For "
            "''parse' (default), the arrays become structured values. "
            "For 'encode', "
            "arrays containing atoms are sparsified with the values "
            "representing one-hot "
            "keys and boolean true values");
}

BoundFunction parse_json(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() > 2 || args.size() < 1)
        throw HttpReturnException(400, " takes 1 or 2 argument, got " + to_string(args.size()));


    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExcAssert(args.size() > 0 && args.size() < 3);
                auto & val = args[0];

                if(val.empty())
                    return ExpressionValue::null(val.getEffectiveTimestamp());

                ParseJsonOptions options;

                if(args.size() == 2) {
                    options = args[1].extractT<ParseJsonOptions>();
                }

                try {
                    MLDB_TRACE_EXCEPTIONS(!options.ignoreErrors);

                    Utf8String str = val.toUtf8String();
                    StreamingJsonParsingContext parser(str.rawString(),
                                                       str.rawData(),
                                                       str.rawLength());

                    if (!parser.isObject() && !parser.isArray())
                        throw HttpReturnException(400, "JSON passed to parse_json must be "
                                "an object or an array; got '" + str + "'",
                                                  "json", str);

                    return ExpressionValue::
                        parseJson(parser, val.getEffectiveTimestamp(),
                                  options.arrays);
                }
                catch(std::exception & e) {
                    if(options.ignoreErrors) {
                        RowValue rv;
                        rv.emplace_back(make_tuple(Path("__parse_json_error__"),
                                                   CellValue(true),
                                                   //CellValue(e.what()),
                                                   val.getEffectiveTimestamp()));
                        return ExpressionValue(std::move(rv));
                    }

                    throw;
                }
            },
            std::make_shared<UnknownRowValueInfo>()
            };
}

static RegisterBuiltin registerJsonDecode(parse_json, "parse_json");

BoundFunction print_json(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                auto & val = args[0];
                std::string str;
                StringJsonPrintingContext context(str);
                val.extractJson(context);
                return ExpressionValue(Utf8String(std::move(str)),
                                       val.getEffectiveTimestamp());
            },
            std::make_shared<Utf8StringValueInfo>()
            };
}

static RegisterBuiltin registerPrintJson(print_json, "print_json");


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
                if (args[0].empty()) {
                    return ExpressionValue::null(Date::negativeInfinity());
                }

                Date ts = args[0].getEffectiveTimestamp();

                Utf8String text = args[0].toUtf8String();

                TokenizeOptions options;

                if (args.size() == 2) {
                    options = args[1].extractT<TokenizeOptions>();
                }

                ParseContext pcontext(text.rawData(), text.rawData(), text.rawLength());

                std::unordered_map<Utf8String, int> bagOfWords;

                tokenize(bagOfWords, pcontext, options);

                RowValue row;
                row.reserve(bagOfWords.size());

                auto it = bagOfWords.begin();

                while (it != bagOfWords.end()) {
                    if (!options.value.empty()) {
                        row.emplace_back(ColumnPath(it->first),
                                         options.value,
                                         ts);
                        ++it;
                    }
                    else
                    {
                        row.emplace_back(ColumnPath(it->first),
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

                TokenizeOptions options;

                if (args.size() == 3) {
                    options = args[2].extractT<TokenizeOptions>();
                }
                
                ParseContext pcontext(text.rawData(), text.rawData(), text.rawLength());

                ExpressionValue result;

                int nth = args.at(1).toInt();

                Utf8String output = token_extract(pcontext, nth, options);

                if (!output.empty())
                    result = ExpressionValue(std::move(output), ts);

                return result;
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerToken_extract(token_extract, "token_extract");

BoundFunction token_split(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() != 2)
        throw HttpReturnException(400, "requires two arguments");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                Date ts = args[0].getEffectiveTimestamp();

                Utf8String text = args[0].toUtf8String();

                Utf8String separator = args[1].toUtf8String();

                TokenizeOptions options;

                ParseContext pcontext(text.rawData(), text.rawData(), text.rawLength());

                auto tokens = token_split(pcontext, separator);

                std::vector<CellValue> values;

                for (auto& token : tokens) {
                    values.push_back(token);
                }

                ExpressionValue result(values, ts);

                return result;
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerToken_split(token_split, "split_part");

BoundFunction horizontal_count(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                size_t result = 0;
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
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
            std::make_shared<Uint64ValueInfo>()};
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
                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
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

BoundFunction horizontal_string_agg(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() != 1 && args.size() != 2)
        throw HttpReturnException(500, "horizontal_string_agg function takes one or two arguments");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                bool first = true;
                Utf8String result;
                Date ts = Date::negativeInfinity();
                Utf8String separator;
                if (args.size() == 2) {
                    separator = args.at(1).getAtom()
                        .coerceToString().toUtf8String();
                }                

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            if (!first)
                                result += separator;
                            first = false;
                            result += val.coerceToString().toUtf8String();
                            ts.setMax(atomTs);
                        }
                        return true;
                    };
                
                args.at(0).forEachAtom(onAtom);
                
                return ExpressionValue(result, ts);
                },
            std::make_shared<Utf8StringValueInfo>()};
}

static RegisterBuiltin registerHorizontal_String_Agg(horizontal_string_agg, "horizontal_string_agg");

BoundFunction horizontal_avg(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                int64_t num_cols = 0;
                double accum = 0;
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
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

                if(num_cols > 0)
                    return ExpressionValue(accum / num_cols, ts);

                return ExpressionValue::null(args[0].getEffectiveTimestamp());
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
                CellValue min_val;
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            if(min_val.empty() || val < min_val) {
                                ts = atomTs;
                                min_val = val;
                            }
                        }
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return ExpressionValue(min_val, ts);
            },
            std::make_shared<AnyValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Min(horizontal_min, "horizontal_min");

BoundFunction horizontal_max(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                CellValue max_val;
                Date ts = Date::negativeInfinity();

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        if (!val.empty()) {
                            if(max_val.empty() || val > max_val) {
                                ts = atomTs;
                                max_val = val;
                            }
                        }
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return ExpressionValue(max_val, ts);
            },
            std::make_shared<AnyValueInfo>()};
}
static RegisterBuiltin registerHorizontal_Max(horizontal_max, "horizontal_max");

BoundFunction horizontal_earliest(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                auto earliest = ExpressionValue::null(Date::positiveInfinity());

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        auto expr = ExpressionValue(val, atomTs);
                        if (!earliest.isEarlier(atomTs, expr))
                            earliest = std::move(expr);
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return earliest;
            },
            args[0].info};
}
static RegisterBuiltin registerHorizontal_Earliest(horizontal_earliest, "horizontal_earliest");

BoundFunction horizontal_latest(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                auto latest = ExpressionValue::null(Date::negativeInfinity());

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date atomTs)
                    {
                        auto expr = ExpressionValue(val, atomTs);
                        if (!latest.isLater(atomTs, expr))
                            latest = std::move(expr);
                        return true;
                    };

                args.at(0).forEachAtom(onAtom);

                return latest;
            },
            args[0].info};
}
static RegisterBuiltin registerHorizontal_Latest(horizontal_latest, "horizontal_latest");

struct DiffOp {
    static distribution<double> apply(distribution<double> & d1,
                                          distribution<double> & d2)
    {
        return d1 - d2;
    }
};

struct SumOp {
    static distribution<double> apply(distribution<double> & d1,
                                          distribution<double> & d2)
    {
        return d1 + d2;
    }
};

struct ProductOp {
    static distribution<double> apply(distribution<double> & d1,
                                          distribution<double> & d2)
    {
        return d1 * d2;
    }
};

struct QuotientOp {
    static distribution<double> apply(distribution<double> & d1,
                                          distribution<double> & d2)
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

        checkArgsSize(args.size(), 2);

        //cerr << "vector_diff arg 0 = " << jsonEncode(args[0]) << endl;
        //cerr << "vector_diff arg 1 = " << jsonEncode(args[1]) << endl;

        /* Here we ask the ExpressionValueInfo object to return us the
           following:

           1.  A function we use to extract two compatible embeddings from
               two different objects;
           2.  A function we use to pack the result of our expression back
               into the original structure;
           3.  The ExpressionValueInfo object we use to put it back in.
        */
        ExpressionValueInfo::GetCompatibleDoubleEmbeddingsFn extract;
        std::shared_ptr<ExpressionValueInfo> info;
        ExpressionValueInfo::ReconstituteFromDoubleEmbeddingFn reconst;

        std::tie(extract, info, reconst)
            = args[0].info->getCompatibleDoubleEmbeddings(*args[1].info);

        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope) -> ExpressionValue
                {
                    checkArgsSize(args.size(), 2);
                    distribution<double> embedding1, embedding2;
                    std::shared_ptr<const void> token;
                    Date ts;
                    std::tie(embedding1, embedding2, token, ts)
                        = extract(args[0], args[1]);

                    return reconst(Op::apply(embedding1, embedding2), token,
                                   ts);
                },
                info};
    }
};

RegisterVectorOp<DiffOp> registerVectorDiff("vector_diff");
RegisterVectorOp<SumOp> registerVectorSum("vector_sum");
RegisterVectorOp<ProductOp> registerVectorProduct("vector_product");
RegisterVectorOp<QuotientOp> registerVectorQuotient("vector_quotient");


BoundFunction base64_encode(const std::vector<BoundSqlExpression> & args)
{
    // Convert a blob into base64
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);

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
                checkArgsSize(args.size(), 1);
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
    checkArgsSize(args.size(), 2);

    // TODO: there is a better implementation if the field name is
    // a constant expression

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 2);
                auto val1 = args[0];
                auto val2 = args[1];
                Utf8String fieldName = val1.toUtf8String();
                // cerr << "extracting " << jsonEncodeStr(val1)
                //      << " from " << jsonEncodeStr(val2) << endl;
                
                return args[1].getColumn(fieldName);
            },
            std::make_shared<AnyValueInfo>()
            };
}

static RegisterBuiltin registerExtractColumn(extract_column, "extract_column");

BoundFunction lower(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
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

    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                ExpressionValue result(args[0].getAtom().toUtf8String().toUpper(),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<Utf8StringValueInfo>()
    };
}

static RegisterBuiltin registerUpper(upper, "upper");

BoundFunction length(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
     return {[] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
             {
                checkArgsSize(args.size(), 1);
                //if(!args[0].isString())
                    //throw MLDB::Exception("The parameter passed to the length "
                            //"function must be a string");

                return ExpressionValue
                    (args[0].getAtom().toUtf8String().length(), 
                     args[0].getEffectiveTimestamp());
             },
             std::make_shared<IntegerValueInfo>()
    };
}

static RegisterBuiltin registerLength(length, "length");

BoundFunction blob_length(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
     return {[] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
             {
                checkArgsSize(args.size(), 1);

                return ExpressionValue
                    (args[0].getAtom().coerceToBlob().blobLength(), 
                     args[0].getEffectiveTimestamp());
             },
             std::make_shared<IntegerValueInfo>()
    };
}

static RegisterBuiltin registerblob_length(blob_length, "blob_length");

BoundFunction levenshtein_distance(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);

     return {[] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
             {
                using namespace Edlib;

                checkArgsSize(args.size(), 2);
                if(!args[0].isString() || !args[1].isString())
                    throw MLDB::Exception("The parameters passed to the levenshtein_distance "
                            "function must be strings");

                const auto query = args[0].getAtom().toUtf8String().rawString();
                const auto target = args[1].getAtom().toUtf8String().rawString();

                // start by testing easy edge cases
                int maxSize = max(query.size(), target.size());
                int bestScore = -1;
                if(query.size() == 0 && target.size() == 0)
                    bestScore = 0;
                else if(query.size() == 0 || target.size() == 0)
                    bestScore = maxSize;

                if(bestScore != -1)
                    return ExpressionValue(bestScore,
                                           args[0].getEffectiveTimestamp());

                auto conf = edlibDefaultAlignConfig();
                conf.k = maxSize;

                EdlibAlignResult alignRes = 
                    edlibAlign(query.c_str(), query.size(),
                               target.c_str(), target.size(), conf);

                bestScore = alignRes.editDistance;
                edlibFreeAlignResult(alignRes);

                if(bestScore == -1)
                    throw MLDB::Exception("Error computing Levenshtein distance");

                return ExpressionValue(bestScore,
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<IntegerValueInfo>()
    };
}

static RegisterBuiltin registerLevenshteinDistance(levenshtein_distance, "levenshtein_distance");



BoundFunction flatten(const std::vector<BoundSqlExpression> & args)
{
    // Return the result indexed on a single dimension

    checkArgsSize(args.size(), 1);

    if (args[0].info->isEmbedding()) {
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
                    checkArgsSize(args.size(), 1);
                    size_t len = args[0].rowLength();
                    return args[0].reshape({len});
                },
                outputInfo
                    };
    }
    else {
        // Simpler but slower... extract each atom, one by one
        // TODO: infer size and type if known
        auto outputInfo
            = std::make_shared<EmbeddingValueInfo>(ST_ATOM);
        
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope) -> ExpressionValue
                {
                    checkArgsSize(args.size(), 1);

                    // If this is an embedding (but couldn't be proved statically),
                    // then do it the simple and efficient way
                    if (args[0].isEmbedding()) {
                        size_t len = args[0].getAtomCount();
                        return args[0].reshape({len});
                    }

                    std::vector<std::tuple<ColumnPath, CellValue> > vals;
                    vals.reserve(100);
                    Date tsOut = Date::negativeInfinity();
                    auto onAtom = [&] (ColumnPath col,
                                       const ColumnPath & prefix,
                                       CellValue val,
                                       Date ts)
                        {
                            vals.emplace_back(std::move(col), std::move(val));
                            ts.setMax(tsOut);
                            return true;
                        };
                    
                    args.at(0).forEachAtom(onAtom);
                    
                    std::sort(vals.begin(), vals.end());
                  
                    std::vector<CellValue> valsOut;
                    valsOut.reserve(vals.size());
                    for (auto & v: vals)
                        valsOut.emplace_back(std::move(std::get<1>(v)));
  
                    return ExpressionValue(std::move(valsOut), tsOut);
                },
                outputInfo
         };
    }
}

static RegisterBuiltin registerFlatten(flatten, "flatten");

BoundFunction reshape(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2, 3, "reshape");

    // A null first argument is OK for the empty embedding
    // this means basically n times the given value
    if (!args[0].info->couldBeEmbedding()) {
        if (args[0].info->couldBeScalar()) {
            // Shape is not a constant, so we need to evaluate after binding
            if (args.size() != 3)
                throw HttpReturnException
                    (400,"Null embedding needs third argument to reshape()");
            auto shape = args[1].info->getEmbeddingShape();
            auto st = args[2].info->isConst()
                ? valueStorageType(args[2].constantValue().getAtom())
                : ST_ATOM;
            auto outputInfo = std::make_shared<EmbeddingValueInfo>(shape, st);

            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope) -> ExpressionValue
                    {
                        checkArgsSize(args.size(), 3, "reshape");
                        if (!args[0].getAtom().empty()) {
                            throw HttpReturnException
                                (400, "Expected null first argument for scalar reshape()");
                        }

                        size_t n = 1;
                        DimsVector shape;
                        auto addDim = [&] (const Path & columnName,
                                           const Path & prefix,
                                           const CellValue & val,
                                           Date ts)
                            {
                                shape.push_back(val.toInt());
                                n += shape.back();
                                return true;
                            };

                        args[1].forEachAtom(addDim);

                        std::vector<CellValue> vals(n, args[2].getAtom());
                        return ExpressionValue(vals,
                                               calcTs(args[0], args[1], args[2]),
                                               shape);
                    },
                    outputInfo
                        };
            
        }
        throw HttpReturnException(400, "requires an embedding as first argument, got " + jsonEncodeStr(args[0].info));
    }

    if (!args[1].info->couldBeEmbedding())
        throw HttpReturnException(400, "requires an embedding as second argument");

    if (args[1].info->isConst()) {
        //Dont know the type without evaluating second arg;
        auto embeddingFormat = args[1].constantValue();

        DimsVector shape;

        auto addDim = [&] (const Path & columnName,
                           const Path & prefix,
                           const CellValue & val,
                           Date ts)
            {
                shape.push_back(val.toInt());
                return true;
            };

        embeddingFormat.forEachAtom(addDim);

        auto st = args[0].info->getEmbeddingType();

        auto outputInfo = EmbeddingValueInfo::fromShape(shape, st);

        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope) -> ExpressionValue
                {
                    checkArgsSize(args.size(), 2, 3, "reshape");

                    if (args.size() == 2) {
                        return args[0].reshape(shape);
                    }
                    else {
                        return args[0].reshape(shape, args[2]);
                    }
                },
                outputInfo
           };
    }
    else {
        // Shape is not a constant, so we need to evaluate after binding
        auto shape = args[1].info->getEmbeddingShape();
        auto st = args[0].info->getEmbeddingType();

        auto outputInfo = std::make_shared<EmbeddingValueInfo>(shape, st);

        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope) -> ExpressionValue
                {
                    checkArgsSize(args.size(), 2, 3, "reshape");

                    DimsVector shape;
                    auto addDim = [&] (const Path & columnName,
                                       const Path & prefix,
                                       const CellValue & val,
                                       Date ts)
                        {
                            shape.push_back(val.toInt());
                            return true;
                        };

                    args[1].forEachAtom(addDim);

                    if (args.size() == 2) {
                        return args[0].reshape(shape);
                    }
                    else {
                        return args[0].reshape(shape, args[2]);
                    }
                },
                outputInfo
           };
    }
}

static RegisterBuiltin registerReshape(reshape, "reshape");

    // Calculate the output shape of the concatenated values
template<typename Sizes>
Sizes calcShape(const std::vector<Sizes> & shapes)
{
    if (shapes.empty())
        return Sizes();
    Sizes result = shapes[0];
    for (size_t i = 1;  i < shapes.size();  ++i) {
        const Sizes & shape = shapes[i];
        if (shape.size() != result.size()) {
            throw HttpReturnException
                (400, "Attempt to concat vectors with different shapes");
        }
        if (result[0] == -1 || shape[0] == -1)
            result[0] = -1;
        else result[0] += shape[0];
    }
    
    return result;
};
    


BoundFunction concat(const std::vector<BoundSqlExpression> & args)
{
    if (args.empty()) {
        auto exec = [=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope) -> ExpressionValue
            {
                return ExpressionValue::null(Date::negativeInfinity());
            };
        return {
            exec,
            std::make_shared<EmptyValueInfo>()
        };
    }
    
    DimsVector shape;

    std::vector<std::vector<ssize_t> > knownShapes;
    StorageType st = args[0].info->getEmbeddingType();

    for (auto & a: args) {
        if (!a.info->couldBeEmbedding())
            throw HttpReturnException(400, "concat requires embeddings");
        auto sh = a.info->getEmbeddingShape();
        knownShapes.emplace_back(sh);
        st = coveringStorageType(st, a.info->getEmbeddingType());
    }

    auto outShape = calcShape(knownShapes);

    auto outputInfo = std::make_shared<EmbeddingValueInfo>(outShape, st);
    
    auto exec = [=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
        -> ExpressionValue
        {
            ExcAssert(!args.empty());

            Date ts = args[0].getEffectiveTimestamp();
            std::vector<DimsVector> shapes;
            shapes.reserve(args.size());
            auto st = args[0].getEmbeddingType();
            for (auto & a: args) {
                shapes.emplace_back(a.getEmbeddingShape());
                st = coveringStorageType(st, a.getEmbeddingType());
                ts.setMax(a.getEffectiveTimestamp());
            }
            
            DimsVector shape = calcShape(shapes);

            auto outBuffer = allocateStorageBuffer(shape, st);

            // Go argument by argument, copying elements in
            char * data = (char *)outBuffer.get();
            for (auto & a: args) {
                size_t n = a.getAtomCount();
                a.convertEmbedding(data, n, st);
                size_t bytes = storageBufferBytes(n, st);
                data += bytes;
            }

            return ExpressionValue::embedding(ts, outBuffer, st, shape);
        };

    return {
        exec,
        outputInfo
     };
}

static RegisterBuiltin registerConcat(concat, "concat");

BoundFunction shape(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    if (!args[0].info->couldBeEmbedding())
        throw HttpReturnException(400, "requires an array as first argument");
     
    auto outputInfo
        = std::make_shared<EmbeddingValueInfo>(-1, ST_INT32);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);

                DimsVector shape = args[0].getEmbeddingShape();
                std::vector<int> shapeValues;
                shapeValues.reserve(shape.size());

                for (auto s : shape)
                    shapeValues.push_back(s);

                return ExpressionValue(shapeValues, args[0].getEffectiveTimestamp());
            },
            outputInfo
    }; 
}

static RegisterBuiltin registerShape(shape, "shape");

BoundFunction static_type(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    auto outputInfo = valueInfoForType<std::shared_ptr<ExpressionValueInfo> >();
    Date ts = Date::negativeInfinity();  // it has always had this type

    auto argInfo = args[0].info;
    ExcAssert(argInfo);

    ExpressionValue result(jsonEncode(argInfo), ts);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                return result;
            },
            outputInfo
        };
}

static RegisterBuiltin registerStaticType(static_type, "static_type");

BoundFunction static_expression_info(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    auto outputInfo = valueInfoForType<BoundSqlExpression>();
    Date ts = Date::negativeInfinity();  // it has always had this type

    auto argInfo = args[0].info;
    ExcAssert(argInfo);

    ExpressionValue result(jsonEncode(args[0]), ts);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                return result;
            },
            outputInfo
        };
}

static RegisterBuiltin registerStaticExpressionInfo
    (static_expression_info, "static_expression_info");

BoundFunction static_known_columns(const std::vector<BoundSqlExpression> & args)
{
    // Return the result indexed on a single dimension

    checkArgsSize(args.size(), 1);

    auto outputInfo = std::make_shared<UnknownRowValueInfo>();
    Date ts = Date::negativeInfinity();  // it has always had these columns

    auto argInfo = args[0].info;
    ExcAssert(argInfo);

    auto cols = argInfo->getKnownColumns();

    ExpressionValue result(jsonEncode(cols), ts);


    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                return result;
            },
            outputInfo
        };
}            

static RegisterBuiltin
registerStaticKnownColumns(static_known_columns, "static_known_columns");


// Test function that fails due to a std::bad_alloc on memory allocation
BoundFunction fail_memory_allocation(const std::vector<BoundSqlExpression> & args)
{
    // Return the result indexed on a single dimension
    auto outputInfo = std::make_shared<UnknownRowValueInfo>();

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
        {
            throw std::bad_alloc();
        },
        outputInfo
    };
}

static RegisterBuiltin
registerFailMemoryAllocation(fail_memory_allocation, "_fail_memory_allocation");

BoundFunction clamp(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 3);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                if (args[0].empty())
                    return ExpressionValue();

                CellValue lower = args[1].getAtom();
                CellValue upper = args[2].getAtom();

                Date limitsTs = std::max(args[1].getEffectiveTimestamp(), args[2].getEffectiveTimestamp());

                auto doAtom = [&] (const CellValue& val) {
                    if (val.empty()) {
                        return val;
                    }
                    else if (val.isNaN()) {
                        if (lower.empty())
                            return val;
                        else
                            return lower;
                    }
                    else if (upper.empty()) {
                        return std::max(val, lower);
                    }
                    else {
                        CellValue clamped = boost::algorithm::clamp(val, lower, upper);
                        return clamped;
                    }
                };

                if (args[0].isAtom()) {
                    return ExpressionValue(doAtom(args[0].getAtom()), std::max(args[0].getEffectiveTimestamp(), limitsTs));
                }
                else {
                    std::vector<std::tuple<PathElement, ExpressionValue> > vals;
                    auto exec = [&] (const PathElement & columnName,
                                     const ExpressionValue & val) {

                        vals.emplace_back(columnName, ExpressionValue(doAtom(val.getAtom()), std::max(val.getEffectiveTimestamp(), limitsTs)));
                        return true;
                    } ;

                    checkArgsSize(args.size(), 3);
                    args[0].forEachColumn(exec);

                    return ExpressionValue(vals);
                }
            },
            args[0].info
        };
}

static RegisterBuiltin registerClamp(clamp, "clamp");

BoundFunction parse_path(const std::vector<BoundSqlExpression> & args)
{
    // Parse a string, and return a structured path
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {

                checkArgsSize(args.size(), 1);
                ExpressionValue result(CellValue(Path::parse(args[0].getAtom().toUtf8String())),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<PathValueInfo>()
    };
}

static RegisterBuiltin registerParsePath(parse_path, "parse_path");

BoundFunction stringify_path(const std::vector<BoundSqlExpression> & args)
{
    // Return an escaped string from a path
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {

                checkArgsSize(args.size(), 1);
                ExpressionValue result(args[0].coerceToPath().toUtf8String(),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<Utf8StringValueInfo>()
    };
}

static RegisterBuiltin registerStringifyPath(stringify_path, "stringify_path");

BoundFunction flatten_path(const std::vector<BoundSqlExpression> & args)
{
    // Return an escaped string from a path
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {

                checkArgsSize(args.size(), 1);
                ExpressionValue result(Path(PathElement(args[0].coerceToPath().toUtf8String())),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<PathValueInfo>()
    };
}

static RegisterBuiltin registerFlattenPath(flatten_path, "flatten_path");

BoundFunction unflatten_path(const std::vector<BoundSqlExpression> & args)
{
    // Return an escaped string from a path
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                Path path = args[0].coerceToPath();
                if (path.size() != 1) {
                    throw HttpReturnException
                        (400, "Attempt to pass non-flattened "
                         "path with length " + std::to_string(path.size())
                         + " to unflatten_path().  Must have length of one.",
                         "path", path);
                }
                ExpressionValue result(Path::parse(path[0].toUtf8String()),
                                       args[0].getEffectiveTimestamp());
                return result;
            },
            std::make_shared<PathValueInfo>()
    };
}

static RegisterBuiltin registerUnflattenPath(unflatten_path, "unflatten_path");


BoundFunction path_element(const std::vector<BoundSqlExpression> & args)
{
    // Return the given element of a path
    checkArgsSize(args.size(), 2);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 2);
                size_t el = args[1].getAtom().toUInt();
                ExpressionValue result(CellValue(args[0].coerceToPath().at(el)),
                                       calcTs(args[0], args[1]));
                return result;
            },
            std::make_shared<Utf8StringValueInfo>()
    };
}

static RegisterBuiltin registerPathElement(path_element, "path_element");

BoundFunction path_length(const std::vector<BoundSqlExpression> & args)
{
    // Return the length of a path
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);
                ExpressionValue result(CellValue(args[0].coerceToPath().size()),
                                       calcTs(args[0], args[1]));
                return result;
            },
            std::make_shared<IntegerValueInfo>()
    };
}

static RegisterBuiltin registerPathLength(path_length, "path_length");


/*****************************************************************************/
/* DIAGNOSTIC FUNCTIONS                                                      */
/*****************************************************************************/

/* These functions allow for unit testing of MLDB within SQL, and expose some
   of the details of how SQL works.  They are undocumented for the moment and
   all begin with an underscore.
*/

BoundFunction analyze_join(const std::vector<BoundSqlExpression> & args)
{
    // Return the result indexed on a single dimension

    // Arguments are:
    // - A string with the left table expression
    // - A string with the right table expression
    // - A string with the on condition
    // - A string with the external where condition
    checkArgsSize(args.size(), 4, "_analyse_join");

    auto outputInfo
        = std::make_shared<UnknownRowValueInfo>();

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 4);
                std::shared_ptr<TableExpression> left
                    = TableExpression::parse(args[0].getAtom().toUtf8String());
                std::shared_ptr<TableExpression> right
                    = TableExpression::parse(args[1].getAtom().toUtf8String());
                std::shared_ptr<SqlExpression> on
                    = SqlExpression::parse(args[2].getAtom().toUtf8String());
                std::shared_ptr<SqlExpression> where
                    = SqlExpression::parse(args[3].getAtom().toUtf8String());

                AnnotatedJoinCondition cond(left, right, on, where, JOIN_INNER, false/*debug*/);

                Date ts = Date::negativeInfinity();
                return ExpressionValue(jsonEncode(cond), ts);
            },
            outputInfo
        };
}

static RegisterBuiltin registerAnalyzeJoin(analyze_join, "_analyze_join");

BoundFunction remove_table_name(const std::vector<BoundSqlExpression> & args)
{
    // Return the result indexed on a single dimension

    // Arguments are:
    // - An expression to be analyzed (string)
    // - A table name to be removed
    // - A set of aliases
    checkArgsSize(args.size(), 2, "_remove_table_name");

    auto outputInfo
        = std::make_shared<UnknownRowValueInfo>();

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 2);
                std::shared_ptr<SqlExpression> expr
                    = SqlExpression::parse(args[0].getAtom().toUtf8String());
                Utf8String tableName = args[1].getAtom().toUtf8String();

                auto res = removeTableNameFromExpression(*expr, tableName);
                Date ts = Date::negativeInfinity();
                return ExpressionValue(jsonEncode(res), ts);
            },
            outputInfo
        };
}

static RegisterBuiltin registerRemoveTableName(remove_table_name, "_remove_table_name");

BoundFunction sign(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
    auto outputInfo
        = std::make_shared<NumericValueInfo>();
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                const auto val = args[0].getAtom();
                if (val.empty()) {
                    return ExpressionValue();
                }
                if (!val.isNumeric() || val.isNaN()) {
                    return ExpressionValue(CellValue(std::nan("")),
                                           args[0].getEffectiveTimestamp());
                }
                double number = val.toDouble();
                return ExpressionValue(
                    CellValue(number > 0 ? 1 : number < 0 ? -1 : 0),
                    args[0].getEffectiveTimestamp());
            },
            outputInfo
        };
}

static RegisterBuiltin registerSignFunction(sign, "sign");

BoundFunction hash(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);
    auto outputInfo
        = std::make_shared<NumericValueInfo>();
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                if (args[0].empty()) {
                    return ExpressionValue::null(
                        args[0].getEffectiveTimestamp());
                }
                return ExpressionValue(
                    args[0].hash(),
                    args[0].getEffectiveTimestamp());
            },
            outputInfo
        };
}

static RegisterBuiltin registerHashFunction(hash, "hash");

BoundFunction tryFct(const std::vector<BoundSqlExpression> & args)
{
    auto outputInfo
        = std::make_shared<UnknownRowValueInfo>();
    if (args.size() == 2) {
        // In case of error, this handler yields the second arg
        auto bindFunction = [=] (SqlBindingScope & scope,
                                 std::vector<BoundSqlExpression>& boundArgs,
                                 const SqlExpression * expr) ->  BoundSqlExpression
        {
            return {[=] (const SqlRowScope & row,
                         ExpressionValue & storage,
                         const VariableFilter & filter) -> const ExpressionValue &
            {
                ExcAssertEqual(boundArgs.size(), 2);
                try {
                    return storage = boundArgs[0](row, GET_LATEST);
                }
                catch (const std::exception & exc) {
                }
                return storage = boundArgs[1](row, GET_LATEST);
            },
            expr,
            outputInfo};
        };
        return {bindFunction, outputInfo};
    }

    if (args.size() != 1) {
        throw HttpReturnException(400, "requires one or two arguments");
    }

    // In case of error, this handler yields the exception message
    auto bindFunction = [=] (SqlBindingScope & scope,
                             std::vector<BoundSqlExpression>& boundArgs,
                             const SqlExpression * expr) ->  BoundSqlExpression
    {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            ExcAssertEqual(boundArgs.size(), 1);
            try {
                return storage = boundArgs[0](row, GET_LATEST);
            }
            catch (const std::exception & exc) {
                return storage
                = ExpressionValue(getExceptionString(),
                                  Date::negativeInfinity());
            }
        },
        expr,
        outputInfo};
    };
    return {bindFunction, outputInfo};
}

static RegisterBuiltin registerTryFunction(tryFct, "try");

/*****************************************************************************/
/* BUILTIN CONSTANTS                                                         */
/*****************************************************************************/

RegisterBuiltinConstant::
RegisterBuiltinConstant(const Utf8String & name, const CellValue & value)
    : RegisterFunction(name,
                       std::bind(bind,
                                 std::placeholders::_1,
                                 std::placeholders::_2,
                                 std::placeholders::_3,
                                 value))
{
}

BoundFunction
RegisterBuiltinConstant::
bind(const Utf8String &,
     const std::vector<BoundSqlExpression> & args,
     SqlBindingScope & context,
     const CellValue & value)
{
    ExpressionValue resultVal(value, Date::negativeInfinity());

    auto exec = [=] (const std::vector<ExpressionValue> &,
                     const SqlRowScope & context)
        -> ExpressionValue
        {
            return resultVal;
        };

    BoundFunction result(exec,
                         resultVal.getSpecializedValueInfo(true /*isConstant*/));
    return result;
}

/*****************************************************************************/
/* SQL BUILTINS                                                              */
/*****************************************************************************/

SqlBuiltin::
SqlBuiltin(const std::string & name,
           const Utf8String & expr,
           size_t arity)
    : functionName(name), arity(arity)
{
    parsed = SqlExpression::parse(expr);
    this->arity = arity;
    handle = registerFunction(functionName, std::bind(&SqlBuiltin::bind,
                                                      this,
                                                      std::placeholders::_2,
                                                      std::placeholders::_3));
}

BoundFunction
SqlBuiltin::
bind(const std::vector<BoundSqlExpression> & args,
     SqlBindingScope & scope) const
{
    try {
        if (arity != args.size()) {
            throw HttpReturnException
                (400, "Called builtin function '" + functionName
                 + "' with " + std::to_string(args.size())
                 + " parameters instead of " + std::to_string(arity)
                 + " expected parameters");
        }
        bool isConstant = true;
        std::vector<std::shared_ptr<ExpressionValueInfo> > info;
        for (auto & a: args) {
            isConstant = isConstant && a.info->isConst();
            info.emplace_back(a.info);
        }

        SqlExpressionEvalScope evalScope(scope, info);

        BoundSqlExpression bound = parsed->bind(evalScope);

        BoundFunction result;

        result.exec = [=] (const std::vector<ExpressionValue> & args,
                           const SqlRowScope & scope)
            -> ExpressionValue
            {
                auto rowScope = evalScope.getRowScope(scope, args);
    
                try {
                    return bound(rowScope, GET_ALL);
                } MLDB_CATCH_ALL {
                    rethrowHttpException(-1, "Executing builtin function "
                                         + functionName + ": " + getExceptionString(),
                                         "functionName", functionName,
                                         "functionArgs", args);
                }
            };

        result.resultInfo = bound.info->getConst(isConstant && bound.info->isConst());
        
        return result;
    } MLDB_CATCH_ALL {
        rethrowHttpException(-1, "Binding builtin function "
                             + functionName + ": " + getExceptionString(),
                             "functionName", functionName,
                             "functionArgs", args);
    }
}

BoundFunction fetcher(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    vector<KnownColumn> columnsInfo;
    columnsInfo.emplace_back(Path("content"), make_shared<BlobValueInfo>(),
                             ColumnSparsity::COLUMN_IS_DENSE);
    columnsInfo.emplace_back(Path("error"), make_shared<StringValueInfo>(),
                             ColumnSparsity::COLUMN_IS_DENSE);
    auto outputInfo
        = std::make_shared<RowValueInfo>(columnsInfo);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {

                StructValue result;
                auto content = ExpressionValue::null(Date::notADate());
                auto error = ExpressionValue::null(Date::notADate());
                try {

                    filter_istream stream(args[0].toUtf8String().rawString(),
                                          { { "mapped", "true" },
                                            { "httpAbortOnSlowConnection", "true"} });

                    FsObjectInfo info = stream.info();

                    const char * mappedAddr;
                    size_t mappedSize;
                    std::tie(mappedAddr, mappedSize) = stream.mapped();

                    CellValue blob;
                    if (mappedAddr) {
                        blob = CellValue::blob(mappedAddr, mappedSize);
                    }
                    else {
                        std::ostringstream streamo;
                        streamo << stream.rdbuf();
                        blob = CellValue::blob(streamo.str());
                    }
                    content = ExpressionValue(std::move(blob),
                                              info.lastModified);
                }
                MLDB_CATCH_ALL {
                    error = ExpressionValue(getUtf8ExceptionString(),
                                            Date::now());
                }
                result.emplace_back("content", content);
                result.emplace_back("error", error);
                return result;
            },
            outputInfo
        };
}
static RegisterBuiltin registerFetcherFunction(fetcher, "fetcher");

BoundFunction static_is_constant(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    bool isConst = args[0].info->isConst();

    auto outputInfo
        = std::make_shared<BooleanValueInfo>(true);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                return ExpressionValue(isConst, args[0].getEffectiveTimestamp());
            },
            outputInfo
        };
}
static RegisterBuiltin registerIsConstFunction(static_is_constant, "__isconst");

} // namespace Builtins
} // namespace MLDB

