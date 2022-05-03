/* lisp_lib.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib.h"
#include "lisp_predicate.h"
#include "lisp_visitor.h"
#include "safe_clamp.h"
#include <shared_mutex>
#include <map>

using namespace std;

namespace MLDB {
namespace Lisp {

namespace {

struct FunctionNamespace {
    Path ns;
    mutable std::shared_mutex mutex;
    std::map<PathElement, FunctionCompiler> functionCompilers;

    void addFunctionCompiler(PathElement name, FunctionCompiler compiler)
    {
        std::unique_lock guard { mutex };
        auto [it, inserted] = functionCompilers.emplace(std::move(name), std::move(compiler));
        if (!inserted) {
            throw MLDB::Exception("function compiler " + it->first.toUtf8String().rawString()
                                  + " already registered in namespace " + ns.toUtf8String().rawString());
        }
    }
};

std::shared_mutex lispNamespacesMutex;
std::map<PathElement, FunctionNamespace> lispNamespaces;

} // file scope

void addFunctionCompiler(PathElement ns, PathElement name, FunctionCompiler compiler)
{
    std::unique_lock guard { lispNamespacesMutex };
    auto it = lispNamespaces.find(ns);
    if (it == lispNamespaces.end()) {
        // Insert and set the namespace
        lispNamespaces[ns].ns = ns;
        it = lispNamespaces.find(ns);
    }
    it->second.addFunctionCompiler(std::move(name), std::move(compiler));
}

std::optional<FunctionCompiler>
tryLookupFunction(const PathElement & fn, const std::vector<PathElement> & importedNamespaces)
{
    std::shared_lock guard { lispNamespacesMutex };
    for (auto & n: importedNamespaces) {
        auto it = lispNamespaces.find(n);
        if (it == lispNamespaces.end()) {
            throw MLDB::Exception("Looking up function: unknown namespace " + n.toUtf8String());
        }
        const FunctionNamespace & ns = it->second;
        std::shared_lock guard { ns.mutex };
        auto it2 = ns.functionCompilers.find(fn);
        if (it2 != ns.functionCompilers.end()) {
            return it2->second;
        }
    }
    return std::nullopt;
}

FunctionCompiler
lookupFunction(const PathElement & fn,
               const std::vector<PathElement> & importedNamespaces)
{
    auto tried = tryLookupFunction(fn, importedNamespaces);
    if (!tried)
        throw MLDB::Exception("Couldn't find " + fn.toUtf8String() + " in any namespace");
    return *tried;
}

#if 0
const FunctionCompiler & getFunctionCompiler(const PathElement & name)
{
    std::shared_lock guard { FunctionCompilersMutex };
    auto it = FunctionCompilers.find(name);
    if (it == FunctionCompilers.end()) {
        throw MLDB::Exception("function compiler " + name.toUtf8String().rawString() + " not registered");
    }
    return it->second;
}
#endif

Value
recursePatterns(const std::vector<Pattern> & patterns,
                const Value & input)
{
    auto applyPatterns = [&] (const Value & input) -> Value
    {
        Value current = input;

        for (bool matched = true; matched; matched = false) {
            for (auto & p: patterns) {
                auto res = p.apply(current);
                if (res) {
                    //cerr << "matched: " << current << " : " << p.toLisp() << " -> " << *res << endl;
                    current = *res;
                    matched = true;
                    break;
                }
            }
        }
 
        return current;
    };

    RecursiveLambdaVisitor visitor { applyPatterns };

    return recurse(visitor, applyPatterns(input));
}

double asDouble(const Value & v)
{
    LambdaVisitor visitor {
        ExceptionOnUnknownReturning<double>("asDouble not defined for this value"),
        [] (int64_t i)            { return i; },
        [] (uint64_t i)           { return i; },
        [] (double d)             { return d; },
    };
    return visit(visitor, v);
}

uint64_t asUInt(const Value & v)
{
    LambdaVisitor visitor {
        ExceptionOnUnknownReturning<uint64_t>("asUInt not defined for this value"),
        [] (int64_t i)            { if (i < 0) MLDB_THROW_LOGIC_ERROR();  return i; },
        [] (uint64_t i)           { return i; },
        [] (double d)             { return safe_clamp<uint64_t>(d); },
    };
    return visit(visitor, v);
}

int64_t asInt(const Value & v)
{
    LambdaVisitor visitor {
        ExceptionOnUnknownReturning<uint64_t>("asInt not defined for this value"),
        [] (int64_t i)            { return i; },
        [] (uint64_t i)           { if (i > numeric_limits<int64_t>::max()) MLDB_THROW_LOGIC_ERROR(); return i; },
        [] (double d)             { return safe_clamp<int64_t>(d); },
    };
    return visit(visitor, v);
}

template<typename Update>
Executor getArithmeticExecutor(Update && update, Value identity, std::vector<Executor> argExecutors)
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
    std::vector<CreateExecutionScope> scopeCreators;
    std::vector<Executor> argExecutors;

    for (auto & item: expr) {
        auto [name, itemExecutor, createItemScope, itemContext, info] = scope.compile(item);
        scopeCreators.emplace_back(std::move(createItemScope));
        argExecutors.emplace_back(std::move(itemExecutor));
    }

    // TODO: scopeCreators... should not be ignored

    Executor exec = getArithmeticExecutor(std::move(update), Value(context, identity), std::move(argExecutors));
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

Value returnNil(ExecutionScope & scope)
{
    return scope.getContext().list();
}

template<typename Compare>
CompiledExpression compileComparison(Compare && compare, PathElement name, const CompilationScope & scope, List expr)
{
    if (expr.size() != 3) {
        scope.exception("Comparison expression requires two arguments");
    }

    Context & context = scope.getContext();
    std::array<CreateExecutionScope, 2> scopeCreators;
    std::array<Executor, 2> argExecutors;

    for (size_t i = 0;  i < 2;  ++i) {
        auto & item = expr[i + 1];
        auto [name, itemExecutor, createItemScope, itemContext, info] = scope.compile(item);
        scopeCreators[i] = std::move(createItemScope);
        argExecutors[i] = std::move(itemExecutor);
    }

    Executor exec = [argExecutors = std::move(argExecutors), compare = std::move(compare)]
          (ExecutionScope & scope) -> Value
    {
        return scope.getContext().boolean(compare(argExecutors[0](scope), argExecutors[1](scope)));
    };

    return { name, std::move(exec), nullptr, &context, "COMPARISON " + name.toUtf8String() };
}

// Implement binary comparison with the underlying comparison operator, specialized to
// the common type
template<template<typename> typename Cmp>
bool compareImpl(const Value & v1, const Value & v2)
{
    //cerr << "calculating " << result.print() << " / " << v2.print() << endl;

    if (v1.is<double>() || v2.is<double>()) {
        // TODO: inexactness
        return Cmp<double>()(asDouble(v1),asDouble(v2));
    }
    else if (v1.is<int64_t>() || v2.is<int64_t>()) {
        return Cmp<uint64_t>()(asInt(v1),asInt(v2));
    }
    else if (v1.is<uint64_t>() || v2.is<uint64_t>()) {
        return Cmp<int64_t>()(asUInt(v1),asUInt(v2));
    }
    else {
        MLDB_THROW_RUNTIME_ERROR("incompatible types for multiplication");
    }
}

DEFINE_LISP_FUNCTION_COMPILER(less, std, "<")
{
    return compileComparison(compareImpl<std::less>, "less", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(greater, std, ">")
{
    return compileComparison(compareImpl<std::greater>, "greater", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(equal, std, "=")
{
    return compileComparison(compareImpl<std::equal_to>, "equal", scope, std::move(expr));
}

// (let (binding...) form...)
DEFINE_LISP_FUNCTION_COMPILER(let, std, "let")
{
    auto & context = scope.getContext();

    std::vector<CreateExecutionScope> scopeCreators;
    std::vector<Executor> argExecutors;

    // Empty let does nothing and returns nil
    if (expr.empty()) {
        return { "let", returnNil, nullptr, &context, "LET ()" };
    }

    std::vector<std::pair<PathElement, CompiledExpression>> locals;

    auto visitBind = [&] (const Value & val)
    {
        cerr << "doing binding " << val << endl;
        PathElement name;
        CompiledExpression value;

        LambdaVisitor visitor {
            ExceptionOnUnknownReturning<bool>("Cannot bind this value in let"),
            [&] (const List & l)       
            {
                switch (l.size()) {
                case 0:                 return false;
                case 2:                 value = scope.compile(l[1]);  // fall through
                case 1:                 name = l[0].as<Symbol>().sym;  return true;
                default:                scope.exception("a binding; list should have 0-2 elements");
                }
            },
            [&] (const Symbol & s)     { name = s.sym; return true; },
        };
        if (!visit(visitor, val))
            return;

        locals.emplace_back(std::move(name), std::move(value));
    };

    for (auto & b: expr[1].expect<List>("Expected list of bindings for second argument to let"))
        visitBind(b);

    cerr << "locals.size() = " << locals.size() << endl;

    // The rest are the forms in which we evaluate it
    for (size_t i = 2;  i < expr.size();  ++i) {
        const Value & item = expr[i];
        auto [name, itemExecutor, createItemScope, itemContext, info] = scope.compile(item);
        scopeCreators.emplace_back(std::move(createItemScope));
        argExecutors.emplace_back(std::move(itemExecutor));
    }

    auto [innerScope, innerScopeCreator] = scope.enterScopeWithLocals(std::move(locals));

    CreateExecutionScope createScope
        = [innerScopeCreator=innerScopeCreator, scopeCreators]
          (std::shared_ptr<ExecutionScope> outerScope, List args) -> std::shared_ptr<ExecutionScope>
    {
        // TODO: we need to call the scope creators...
        return innerScopeCreator(outerScope, std::move(args));
    };

    Executor exec = [argExecutors, scopeCreators] (ExecutionScope & scope) -> Value
    {
        Value result = scope.getContext().list();
        
        for (size_t i = 0;  i < argExecutors.size();  ++i) {
            result = argExecutors[i](scope);
        }

        return result;
    };

    return { "let", std::move(exec), std::move(createScope), &context, "LET (...)" };
}

// (setq var1 val1 var2 val2 ...)
DEFINE_LISP_FUNCTION_COMPILER(setq, std, "setq")
{
    auto & context = scope.getContext();

    std::vector<std::tuple<VariableWriter, CompiledExpression>> todos;

    for (size_t i = 1;  i < expr.size();  i += 2) {
        LambdaVisitor nameVisitor {
            ExceptionOnUnknownReturning<PathElement>("Not implemented: evaluate symbol name"),
            [&] (const Symbol & sym) -> PathElement { return sym.sym; },
            // TODO: calculted symbol names?
        };

        PathElement varName = visit(nameVisitor, expr[i]);
        VariableWriter setter = scope.getVariableWriter(varName);
        CompiledExpression varVal = scope.compile(expr.at(i + 1));

        todos.emplace_back(std::move(setter), std::move(varVal));
    }

    CreateExecutionScope createScope = nullptr;
    
    Executor exec = [todos] (ExecutionScope & scope) -> Value
    {
        Value result = scope.getContext().list();
        for (auto & [writer, calcValue]: todos) {
            writer(scope, result = calcValue(scope));            
        }

        return result;
    };

    return { "setq", std::move(exec), std::move(createScope), &context, "SETQ (...)" };
}

// (list ...)
DEFINE_LISP_FUNCTION_COMPILER(list, std, "list")
{
    auto & context = scope.getContext();

    // Evaluate all arguments
    std::vector<CompiledExpression> todos;
    for (size_t i = 1;  i < expr.size();  ++i) {
        todos.emplace_back(scope.compile(expr.at(i)));
    }

    CreateExecutionScope createScope = nullptr;
    
    Executor exec = [todos] (ExecutionScope & scope) -> Value
    {
        List result;
        result.reserve(todos.size());
        for (auto & t: todos) {
            result.emplace_back(t(scope));
        }
        return { scope.getContext(), result };
    };

    return { "list", std::move(exec), std::move(createScope), &context, "LIST (...)" };
}

// (quote val)
DEFINE_LISP_FUNCTION_COMPILER(quote, std, "quote")
{
    if (expr.size() != 2) {
        scope.exception("quote function takes one argument");
    }

    auto & context = scope.getContext();

    CreateExecutionScope createScope = nullptr;

    Value val = std::move(expr[1]);
    val.setQuotes(1);
    
    Executor exec = [val = expr[1]] (ExecutionScope & scope) -> Value
    {
        // TODO: switch context to execution cont4ext
        return val;
    };

    return { "quote", std::move(exec), std::move(createScope), &context, "QUOTE (...)" };
}

// (length list)
DEFINE_LISP_FUNCTION_COMPILER(length, std, "length")
{
    auto & context = scope.getContext();

    if (expr.size() != 2) {
        scope.exception("length function takes one argument");
    }

    // Evaluate all arguments?  Currently we do for side effects; this could be simplified
    CompiledExpression cmp = scope.compile(expr[1]);

    CreateExecutionScope createScope = nullptr;
    
    Executor exec = [cmp] (ExecutionScope & scope) -> Value
    {
        Value v = cmp(scope);
        if (!v.is<List>()) {
            scope.exception("length function applied to non-list " + v.print());
        }
        return scope.getContext().u64(v.as<List>().size());
    };

    return { "length", std::move(exec), std::move(createScope), &context, "LENGTH (...)" };
}

// (nth int list)
DEFINE_LISP_FUNCTION_COMPILER(nth, std, "nth")
{
    auto & context = scope.getContext();

    if (expr.size() != 3) {
        scope.exception("nth function takes two arguments");
    }

    // Evaluate all arguments?  Currently we do for side effects; this could be simplified
    CompiledExpression nexp = scope.compile(expr[1]);
    CompiledExpression lexp = scope.compile(expr[2]);

    CreateExecutionScope createScope = nullptr;
    
    Executor exec = [nexp, lexp] (ExecutionScope & scope) -> Value
    {
        Value n = nexp(scope);
        Value l = lexp(scope);

        if (!l.is<List>()) {
            scope.exception("length function applied to non-list " + l.print());
        }

        return l.as<List>()[asUInt(n)];
    };

    return { "nth", std::move(exec), std::move(createScope), &context, "NTH (...)" };
}

// (eval expr)
DEFINE_LISP_FUNCTION_COMPILER(eval, std, "eval")
{
    if (expr.size() != 2) {
        scope.exception("eval function takes one argument");
    }

    auto & context = scope.getContext();
    CompiledExpression toEval = scope.compile(std::move(expr[1]));

    CreateExecutionScope createScope = nullptr;
    
    Executor exec = [toEval] (ExecutionScope & scope) -> Value
    {
        Value program = toEval(scope);
        CompilationScope cscope(scope.getContext());
        Value result = cscope.eval(std::move(program));

        return result;
    };

    return { "eval", std::move(exec), std::move(createScope), &context, "EVAL (...)" };
}

// (null expr)
DEFINE_LISP_FUNCTION_COMPILER(null, std, "null")
{
    if (expr.size() != 2) {
        scope.exception("null function takes one argument");
    }

    auto & context = scope.getContext();
    CompiledExpression toEval = scope.compile(std::move(expr[1]));

    CreateExecutionScope createScope = nullptr;
    
    Executor exec = [toEval] (ExecutionScope & scope) -> Value
    {
        Value val = toEval(scope);

        return scope.getContext().boolean(val.is<Null>() || (val.is<List>() && val.as<List>().empty()));
    };

    return { "null", std::move(exec), std::move(createScope), &context, "NULL (...)" };
}

} // namespace Lisp
} // namespace MLDB
