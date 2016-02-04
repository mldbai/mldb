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
#include "mldb/utils/hash.h"
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
                       const SqlBindingScope & context)
            -> BoundFunction
            {
                try {
                    BoundFunction result = std::move(function(args));
                    auto fn = result.exec;
                    result.exec = [=] (const std::vector<ExpressionValue> & args,
                                        const SqlRowScope & context)
                        -> ExpressionValue
                    {
                        try {
                            return fn(args, context);
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

// Calculate the effective timstamps for an expression involving two
// operands.
static Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2)
{
    return std::max(v1.getEffectiveTimestamp(),
                    v2.getEffectiveTimestamp());
}

typedef double (*DoubleFunction2)(double, double);

ExpressionValue twoArgsFunction(const std::vector<ExpressionValue> & args, DoubleFunction2 func)
{
    ExcAssertEqual(args.size(), 2);
    double v1 = args[0].toDouble();
    double v2 = args[1].toDouble();
    return ExpressionValue(func(v1, v2), calcTs(args[0], args[1]));
}

typedef double (*DoubleFunction1)(double);

ExpressionValue oneArgFunction(const std::vector<ExpressionValue> & args, DoubleFunction1 func)
{
    ExcAssertEqual(args.size(), 1);
    double v1 = args[0].toDouble();
    return ExpressionValue(func(v1), calcTs(args[0], args[1]));
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
                const SqlRowScope & context) -> ExpressionValue
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
                const SqlRowScope & context) -> ExpressionValue
            {
                return replaceIf(args, [](double d) { return std::isinf(d); });
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerReplaceInf(replaceIfInf, "replace_inf", "replaceInf");

BoundFunction power(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                return twoArgsFunction(args, std::pow);
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerPow(power, "power", "pow");

BoundFunction abs(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                return oneArgFunction(args, std::abs);
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerAbs(abs, "abs");

BoundFunction sqrt(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                double v = args[0].toDouble();
                //complex numbers not yet supported :-)
                if (v < 0)
                    throw HttpReturnException(400, "sqrt function supports non-negative numbers only");

                return ExpressionValue(std::sqrt(v),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerSqrt(sqrt, "sqrt");

BoundFunction mod(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                ExcAssert(args[0].isInteger());
                ExcAssert(args[1].isInteger());
                int64_t v1 = args[0].toInt();
                int64_t v2 = args[1].toInt();
                return ExpressionValue(v1 % v2, calcTs(args[0], args[1]));
            },
            std::make_shared<Float64ValueInfo>()};
}

    
static RegisterBuiltin registerMod(mod, "mod");

BoundFunction ceil(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                return oneArgFunction(args, std::ceil);
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerCeil(ceil, "ceil", "ceiling");

BoundFunction floor(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                return oneArgFunction(args, std::floor);
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerFloor(floor, "floor");

BoundFunction ln(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                double v = args[0].toDouble();
                if (v <= 0)
                    throw HttpReturnException(400, "ln function supports positive numbers only");

                return ExpressionValue(std::log(v),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerLn(ln, "ln");

BoundFunction exp(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                double v = args[0].toDouble();

                return ExpressionValue(std::exp(v),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerExp(exp, "exp");

BoundFunction quantize(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                double v1 = args[0].toDouble();
                double v2 = args[1].toDouble();
                long long ratio = std::llround(v1 / v2);
                return ExpressionValue(ratio * v2, calcTs(args[0], args[1]));
            },
            std::make_shared<Float64ValueInfo>()};
}

static RegisterBuiltin registerQuantize(quantize, "quantize");

#ifdef THIS_MUST_BE_CLARIFIED_FIRST
BoundFunction cardinality(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    return {[] (const std::vector<ExpressionValue> & args,
                const SqlRowScope & context) -> ExpressionValue
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
                const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                ExcAssert(args[0].isInteger());
                ExcAssert(args[1].isInteger());
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
                const SqlRowScope & context) -> ExpressionValue
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
                const SqlRowScope & context) -> ExpressionValue
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

static RegisterBuiltin registerImplicitCast(implicit_cast, "implicit_cast", "implicitCast");

BoundFunction regex_replace(const std::vector<BoundSqlExpression> & args)
{ 
    // regex_replace(string, regex, replacement)
    checkArgsSize(args.size(), 3);

    Utf8String regexStr = args[1].constantValue().toUtf8String();

    boost::u32regex regex = boost::make_u32regex(regexStr.rawData());

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 3);

                Date ts = args[0].getEffectiveTimestamp();
                ts.setMax(args[1].getEffectiveTimestamp());
                ts.setMax(args[2].getEffectiveTimestamp());

                if (args[0].empty() || args[2].empty())
                    return ExpressionValue::null(ts);

                std::basic_string<char32_t> matchStr = args[0].toWideString();
                std::basic_string<char32_t> replacementStr = args[2].toWideString();

                std::basic_string<int32_t>
                    matchStr2(matchStr.begin(), matchStr.end());
                std::basic_string<int32_t>
                    replacementStr2(replacementStr.begin(), replacementStr.end());

                auto result = boost::u32regex_replace(matchStr2, regex, replacementStr2);
                std::basic_string<char32_t> result2(result.begin(), result.end());

                return ExpressionValue(result2, ts);
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                // TODO: should be able to pass utf-8 string directly in

                ExcAssertEqual(args.size(), 2);

                Date ts = args[0].getEffectiveTimestamp();
                ts.setMax(args[1].getEffectiveTimestamp());

                if (args[0].empty())
                    return ExpressionValue::null(ts);
                    
                std::basic_string<char32_t> matchStr = args[0].toWideString();

                auto result = boost::u32regex_match(matchStr.begin(), matchStr.end(),
                                                    regex);
                return ExpressionValue(result, ts);
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                // TODO: should be able to pass utf-8 string directly in

                ExcAssertEqual(args.size(), 2);
                Date ts = args[0].getEffectiveTimestamp();
                ts.setMax(args[1].getEffectiveTimestamp());

                if (args[0].empty())
                    return ExpressionValue::null(ts);

                std::basic_string<char32_t> searchStr = args[0].toWideString();

                auto result
                    = boost::u32regex_search(searchStr.begin(), searchStr.end(),
                                             regex);
                return ExpressionValue(result, ts);
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                return ExpressionValue(args[0].getEffectiveTimestamp(),
                                       args[0].getEffectiveTimestamp());
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerWhen(when, "when");

BoundFunction min_timestamp(const std::vector<BoundSqlExpression> & args)
{
    // Tell us when an expression happened, ie extract its timestamp and return
    // as its value

    checkArgsSize(args.size(), 1);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                return ExpressionValue(args[0].getMinTimestamp(),
                                       args[0].getEffectiveTimestamp());
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 0);
                ExpressionValue result(Date::now(), Date::negativeInfinity());
                return result;
            },
            std::make_shared<TimestampValueInfo>()};
}

static RegisterBuiltin registerNow(now, "now");

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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
    if (p <= 0.0 || !isfinite(p))
       throw HttpReturnException(500, "Invalid power for normalize() function",
                                  "p", p);

    if (p == 0) {
        val /= (val != 0).total();
    }
    else if (p == 2) {
        val /= val.two_norm();
    }
    else if (p == 1) {
        val /= val.total();
    }
    else if (p == INFINITY) {
        val /= val.max();
    }
    else {
        double total = 0.0;
        for (float f: val) {
            total += powf(f, p);
        }
        total = pow(total, 1.0 / p);

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
            const EmbeddingValueInfo* embeddingInfo = dynamic_cast<EmbeddingValueInfo*>(vectorInfo.get());
            vector<ssize_t> shape = { -1 };
            if (embeddingInfo)
                shape = embeddingInfo->shape;

            return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
            {
                    // Get it as an embedding
                    ML::distribution<float> val = args.at(0).getEmbedding();
                    Date ts = args.at(0).getEffectiveTimestamp();
                    double p = args.at(1).toDouble();

                    normalize(val, p);

                    ExpressionValue result(std::move(val),
                                               ts);

                    return std::move(result);
         
            },
                    std::make_shared<EmbeddingValueInfo>(shape)};
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                // Get it as an embedding
                ML::distribution<float> val = args.at(0).getEmbedding();
                Date ts = args.at(0).getEffectiveTimestamp();
                double p = args.at(1).toDouble();

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
                 const SqlRowScope & context) -> ExpressionValue
            {
                // Get it as an embedding
                ML::distribution<float> val = args.at(0).getEmbedding();
                Date ts = args.at(0).getEffectiveTimestamp();

                double p = args.at(1).toDouble();

                if (p <= 0.0 || !isfinite(p))
                    throw HttpReturnException(500, "Invalid power for norm() function",
                                              "p", p);

                if (p == 2) {
                    return ExpressionValue(val.two_norm(), ts);
                }
                else {
                    double total = 0.0;
                    for (float f: val) {
                        total += powf(f, p);
                    }
                    total = pow(total, 1.0 / p);

                    return ExpressionValue(total, ts);
                }
            },
            std::make_shared<Float64ValueInfo>()};
        
}
static RegisterBuiltin registerNorm(norm, "norm");


BoundFunction parse_sparse_csv(const std::vector<BoundSqlExpression> & args)
{
    // Comma separated list, first is row name, rest are row columns

    if (args.size() == 0)
        throw HttpReturnException(400, "takes at least 1 argument, got " + to_string(args.size()));

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
            {
                std::string str = args.at(0).toString();
                Date ts = args.at(0).getEffectiveTimestamp();

                int skip = 1;
                if (args.size() >= 2)
                    skip = args[1].toInt();
                char separator = ',';
                if (args.size() >= 3) {
                    string s = args[2].toString();
                    if (s.length() != 1)
                        throw HttpReturnException(400,
                                                  "Separator for parse_sparse_csv should be a single character",
                                                  "separator", s,
                                                  "args", args);
                }
                CellValue val(1);
                if (args.size() >= 4)
                    val = args[3].getAtom();

                std::string filename = str;
                if (args.size() >= 5)
                    filename = args[4].toString();
                int line = 1;
                if (args.size() >= 6)
                    line = args[5].toInt();

                int col = 1;
                if (args.size() >= 7)
                    col = args[6].toInt();

                // TODO: support UTF-8
                ML::Parse_Context pcontext(filename, 
                                           str.c_str(), str.length(), line, col);
                    
                vector<string> fields = ML::expect_csv_row(pcontext, -1, separator);
                    
                RowValue row;
                row.reserve(fields.size() - 1);
                for (unsigned i = 1;  i < fields.size();  ++i) {
                    row.emplace_back(ColumnName(fields[i]),
                                     val,
                                     ts);
                                         
                }

                return ExpressionValue(std::move(row));
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerParseSparseCsv(parse_sparse_csv, "parse_sparse_csv");


BoundFunction parse_json(const std::vector<BoundSqlExpression> & args)
{
    if (args.size() != 1)
        throw HttpReturnException(400, "parse_json function takes 1 argument");
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                Utf8String str = args[0].toUtf8String();
                StreamingJsonParsingContext parser(str.rawString(),
                                                   str.rawData(),
                                                   str.rawLength());
                return ExpressionValue::
                    parseJson(parser, args[0].getEffectiveTimestamp(),
                              PARSE_ARRAYS);
            },
            std::make_shared<AnyValueInfo>()
            };
}

static RegisterBuiltin registerJsonDecode(parse_json, "parse_json");


BoundFunction get_bound_unpack_json(const std::vector<BoundSqlExpression> & args)
{
    // Comma separated list, first is row name, rest are row columns
    checkArgsSize(args.size(), 1);

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 1);
                Date ts = args.at(0).getEffectiveTimestamp();

                Utf8String str = args[0].toUtf8String();
                StreamingJsonParsingContext parser(str.rawString(),
                                                   str.rawData(),
                                                   str.rawLength());

                if (!parser.isObject())
                    throw HttpReturnException(400, "JSON passed to unpack_json must be an object",
                                              "json", str);
                
                return ExpressionValue::
                    parseJson(parser, args[0].getEffectiveTimestamp(),
                              ENCODE_ARRAYS);
            },
            std::make_shared<UnknownRowValueInfo>()};
}

static RegisterBuiltin registerUnpackJson(get_bound_unpack_json, "unpack_json");


void
ParseTokenizeArguments(Utf8String& splitchar, Utf8String& quotechar,
                       int& offset, int& limit, int& min_token_length,
                       ML::distribution<float, std::vector<float> > & ngram_range,
                       Utf8String& values, 
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
            values = std::get<1>(arg).toUtf8String();
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                Date ts = args.at(0).getEffectiveTimestamp();

                Utf8String text = args.at(0).toUtf8String();

                Utf8String splitchar = ",";
                Utf8String quotechar = "\"";
                int offset = 0;
                int limit = -1;
                int min_token_length = 1;
                ML::distribution<float, std::vector<float> > ngram_range = {1, 1};
                Utf8String values = "";
                bool check[] = {false, false, false, false, false, false, false};
                
                if (args.size() == 2)
                    ParseTokenizeArguments(splitchar, quotechar, offset, limit,
                            min_token_length, ngram_range, values, check, args.at(1).getRow());

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
                        row.emplace_back(ColumnName(it->first),
                                     values,
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                Date ts = args.at(0).getEffectiveTimestamp();

                Utf8String text = args.at(0).toUtf8String();

                Utf8String splitchar = ",";
                Utf8String quotechar = "\"";
                int offset = 0;
                int limit = 1;
                int min_token_length = 1;
                ML::distribution<float, std::vector<float> > ngram_range;
                Utf8String values = "";
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                     const SqlRowScope & context) -> ExpressionValue
                {
                    //cerr << "val1 = " << jsonEncode(args.at(0)) << endl;
                    //cerr << "val2 = " << jsonEncode(args.at(1)) << endl;

                    // Get it as an embedding
                    ML::distribution<double> val1 = args.at(0).getEmbeddingDouble();
                    ML::distribution<double> val2 = args.at(1).getEmbeddingDouble();
                    Date ts = calcTs(args.at(0), args.at(1));

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
                             args[1](emptyScope).getRow());
    }

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
            {
                ExcAssertEqual(args.size(), 2);
                Utf8String fieldName = args[0].toUtf8String();
                cerr << "extracting " << jsonEncodeStr(args[0])
                     << " from " << jsonEncodeStr(args[1]) << endl;

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
                 const SqlRowScope & context) -> ExpressionValue
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
                 const SqlRowScope & context) -> ExpressionValue
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


} // namespace Builtins
} // namespace MLDB
} // namespace Datacratic

