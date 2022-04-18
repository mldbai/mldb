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

    std::vector<CreateExecutionScope> scopeCreators;
    std::vector<Executor> argExecutors;

    for (auto & item: expr) {
        auto [name, itemExecutor, createItemScope, itemContext] = scope.compile(item);
        scopeCreators.emplace_back(std::move(createItemScope));
        argExecutors.emplace_back(std::move(itemExecutor));
    }

#if 0
    CreateExecutionScope createScope = [scopeCreators] (const ExecutionScope & scope) -> std::shared_ptr<ExecutionScope>
    {
        return nullptr;
    };
#endif

    Executor exec = [argExecutors] (ExecutionScope & scope) -> Value
    {
        if (argExecutors.size() == 1)
            return scope.getContext().null();

        auto execN = [&] (size_t i)
        {
            const auto & executor = argExecutors[i];
            return executor(scope);
        };

        Value result = execN(1);

        auto update = [&] (const Value & newValue)
        {
            if (result.is<Utf8String>() || newValue.is<Utf8String>()) {
                result = { scope.getContext(), result.asString() + newValue.asString() };
            }
            else if (result.is<double>() || newValue.is<double>()) {
                result = { scope.getContext(), asDouble(result) + asDouble(newValue) };
            }
            else if (result.is<int64_t>() || newValue.is<int64_t>()) {
                result = { scope.getContext(), asInt(result) + asInt(newValue) };
            }
            else if (result.is<uint64_t>() || newValue.is<uint64_t>()) {
                result = { scope.getContext(), asUInt(result) + asUInt(newValue) };
            }
            else {
                MLDB_THROW_RUNTIME_ERROR("incompatible types for addition");
            }
        };

        for (size_t i = 2;  i < argExecutors.size();  ++i) {
            update(execN(i));
        }

        return result;
    };

    return { "plus", std::move(exec), nullptr /* std::move(createScope) */, &context };
}

Value returnNil(ExecutionScope & scope)
{
    return scope.getContext().list();
}

// (let (binding...) form...)
DEFINE_LISP_FUNCTION_COMPILER(let, std, "let")
{
    auto & context = scope.getContext();
    auto source = Value{ context, expr };

    std::vector<CreateExecutionScope> scopeCreators;
    std::vector<Executor> argExecutors;

    // Empty let does nothing and returns nil
    if (expr.empty()) {
        return { "let", returnNil, nullptr, &context };
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

    cerr << "binding let expression" << source << endl;
    for (auto & b: expr[1].expect<List>("Expected list of bindings for second argument to let"))
        visitBind(b);

    cerr << "locals.size() = " << locals.size() << endl;

    // The rest are the forms in which we evaluate it
    for (size_t i = 2;  i < expr.size();  ++i) {
        const Value & item = expr[i];
        auto [name, itemExecutor, createItemScope, itemContext] = scope.compile(item);
        scopeCreators.emplace_back(std::move(createItemScope));
        argExecutors.emplace_back(std::move(itemExecutor));
    }

    auto [innerScope, innerScopeCreator] = scope.enterScopeWithLocals(std::move(locals));

    CreateExecutionScope createScope
        = [innerScopeCreator=innerScopeCreator, scopeCreators]
          (ExecutionScope & outerScope, List args) -> std::shared_ptr<ExecutionScope>
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

    return { "let", std::move(exec), std::move(createScope), &context };
}

// (setq var1 val1 var2 val2 ...)
DEFINE_LISP_FUNCTION_COMPILER(setq, std, "setq")
{
    auto & context = scope.getContext();
    auto source = Value{ context, expr };

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

    return { "setq", std::move(exec), std::move(createScope), &context };
}

} // namespace Lisp
} // namespace MLDB
