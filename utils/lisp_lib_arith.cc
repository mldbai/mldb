/* lisp_lib_arith.cc                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib_impl.h"
#include "lisp_predicate.h"

using namespace std;

namespace MLDB {
namespace Lisp {

template<typename Update>
Executor getArithmeticExecutor(Update && update, Value identity, std::vector<CompiledExpression> argExecutors)
{
    auto result
        = [argExecutors = std::move(argExecutors), update = std::move(update), identity = std::move(identity)]
          (ExecutionScope & scope) -> Value
    {
        if (argExecutors.size() == 1)
            return identity;

        auto execN = [&] (size_t i)
        {
            const auto & executor = argExecutors[i];
            return executor(scope);
        };

        Value result = execN(1);

        for (size_t i = 2;  i < argExecutors.size();  ++i) {
            update(result, execN(i));
        }

        return result;
    };

    return std::move(result);
};

template<typename Update>
CompiledExpression compileArithmetic(Update && update, PathElement name, int identity, const CompilationScope & scope, List expr)
{
    Context & context = scope.getContext();
    std::vector<CompiledExpression> args;
    for (auto & item: expr)
        args.emplace_back(scope.compile(item));

    Executor exec = getArithmeticExecutor(std::move(update), Value(context, identity), std::move(args));
    return { name, std::move(exec), nullptr, &context, "ARITH " + name.toUtf8String() };
}

// Implement result += newValue, across all the types that + supports
void updatePlus(Value & result, Value newValue)
{
    Context & context = result.getContext();

    if (result.is<Utf8String>() || newValue.is<Utf8String>()) {
        result = { context, result.asString() + newValue.asString() };
    }
    else if (result.is<double>() || newValue.is<double>()) {
        result = { context, asDouble(result) + asDouble(newValue) };
    }
    else if (result.is<int64_t>() || newValue.is<int64_t>()) {
        result = { context, asInt(result) + asInt(newValue) };
    }
    else if (result.is<uint64_t>() || newValue.is<uint64_t>()) {
        result = { context, asUInt(result) + asUInt(newValue) };
    }
    else {
        MLDB_THROW_RUNTIME_ERROR("incompatible types for addition");
    }
}

DEFINE_LISP_FUNCTION_COMPILER(plus, std, "+")
{
    auto & context = scope.getContext();
    std::vector<Pattern> patterns {
        Pattern::parse(context, "(+ $x:i64) -> $x:i64"),
        Pattern::parse(context, "(+ $x:u64) -> $x:u64"),
        Pattern::parse(context, "(+ $x:i64 $y:i64) -> (`addi64 $x $y):i64"),
        Pattern::parse(context, "(+ $x:i64 $y:i64) -> (`addi64 $x $y):i64"),
        Pattern::parse(context, "(+ $x:i64 $y:i64) -> (`addi64 $x $y):i64"),
        Pattern::parse(context, "(+ $x:i64 $y:u64) -> (`addi64 $x (`tosigned64 $y)):i64"),
        Pattern::parse(context, "(+ $x:u64 $y:i64) -> (`addi64 (`tosigned64 $x) $y):i64"),
        Pattern::parse(context, "(+ $x:u64 $y:u64) -> (`addu64 $x $y):u64"),
        Pattern::parse(context, "(+ $x:str $y:str) -> (`concat $x $y):str"),
        Pattern::parse(context, "(+ $x) -> $x"),
        Pattern::parse(context, "(+ $x $y $z) -> (+ (+ $x $y) $z)"),
        Pattern::parse(context, "(+ $x $y $z $rest...) -> (+ (+ $x $y) (+ $z $rest...))"),
    };

    auto source = Value{ context, expr };
    auto current = recursePatterns(patterns, source);
    cerr << "compiled " << source << " to " << current << endl;

    return compileArithmetic(updatePlus, "plus", 0, scope, std::move(expr));
}

// Implement result += newValue, across all the types that + supports
void updateMinus(Value & result, Value newValue)
{
    Context & context = result.getContext();

    if (result.is<double>() || newValue.is<double>()) {
        result = { context, asDouble(result) - asDouble(newValue) };
    }
    else if (result.is<int64_t>() || newValue.is<int64_t>()) {
        result = { context, asInt(result) - asInt(newValue) };
    }
    else if (result.is<uint64_t>() || newValue.is<uint64_t>()) {
        result = { context, asUInt(result) - asUInt(newValue) };
    }
    else {
        MLDB_THROW_RUNTIME_ERROR("incompatible types for multiplication");
    }
}

DEFINE_LISP_FUNCTION_COMPILER(minus, std, "-")
{
    if (expr.size() == 2) {
        // Special case for unary negation (one argument)
        Context & context = scope.getContext();
        auto [name, argExecutor, createItemScope, itemContext, info] = scope.compile(expr[1]);
        Executor exec = [argExecutor = std::move(argExecutor)] (ExecutionScope & scope) -> Value
        {
            Value result = scope.getContext().i64(0);
            updateMinus(result, argExecutor(scope));
            cerr << "negation returned " << result << endl;
            return result;
        };

        return { name, std::move(exec), nullptr, &context, "UNARY NEGATION" };
    }

    return compileArithmetic(updateMinus, "minus", 0, scope, std::move(expr));
}

// Implement result += newValue, across all the types that + supports
void updateTimes(Value & result, Value newValue)
{
    Context & context = result.getContext();

    if (result.is<Utf8String>()) {
        Utf8String str;
        for (size_t i = 0, n = asUInt(newValue); i < n;  ++i) {
            str += result.asString();
        }
        result = { context, str };
    }
    else if (result.is<double>() || newValue.is<double>()) {
        // TODO: overflow
        result = { context, asDouble(result) * asDouble(newValue) };
    }
    else if (result.is<int64_t>() || newValue.is<int64_t>()) {
        // TODO: overflow
        result = { context, asInt(result) * asInt(newValue) };
    }
    else if (result.is<uint64_t>() || newValue.is<uint64_t>()) {
        // TODO: overflow
        result = { context, asUInt(result) * asUInt(newValue) };
    }
    else {
        MLDB_THROW_RUNTIME_ERROR("incompatible types for multiplication");
    }
}

DEFINE_LISP_FUNCTION_COMPILER(times, std, "*")
{
    return compileArithmetic(updateTimes, "times", 1, scope, std::move(expr));
}

// Implement result += newValue, across all the types that + supports
void updateDivide(Value & result, Value newValue)
{
    //cerr << "calculating " << result.print() << " / " << newValue.print() << endl;

    Context & context = result.getContext();

    if (result.is<double>() || newValue.is<double>()) {
        result = { context, asDouble(result) / asDouble(newValue) };
    }
    else if (result.is<int64_t>() || newValue.is<int64_t>()) {
        auto i = asInt(newValue);
        if (i == 0)
            result = context.f64(std::numeric_limits<double>::quiet_NaN());
        else
            result = { context, asInt(result) / i };
    }
    else if (result.is<uint64_t>() || newValue.is<uint64_t>()) {
        auto i = asUInt(newValue);
        if (i == 0)
            result = context.f64(std::numeric_limits<double>::quiet_NaN());
        else
            result = { context, asUInt(result) / i };
    }
    else {
        MLDB_THROW_RUNTIME_ERROR("incompatible types for multiplication");
    }

    //cerr << "  = " << result.print() << endl;
}

DEFINE_LISP_FUNCTION_COMPILER(divide, std, "/")
{
    return compileArithmetic(updateDivide, "divide", 1, scope, std::move(expr));
}

} // namespace Lisp
} // namespace MLDB
