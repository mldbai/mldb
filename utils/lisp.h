/* lisp.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_fwd.h"
#include "lisp_value.h"
#include <optional>
#include <vector>
#include <memory>
#include <functional>
#include <map>

namespace MLDB {
namespace Lisp {

/*******************************************************************************/
/* LISP EXECUTION SCOPE                                                        */
/*******************************************************************************/

struct ExecutionScope {
    ExecutionScope(Context & context);
    ExecutionScope(ExecutionScope & parent);
    ExecutionScope(ExecutionScope & parent, std::map<PathElement, Value> locals);

    Context & getContext() const { return *context_; }

    Value getVariableValue(const PathElement & sym) const;
    void setVariableValue(const PathElement & sym, Value newValue);

    void exception(const Utf8String & reason) const MLDB_NORETURN;

private:
    ExecutionScope * parent_ = nullptr;
    Context * context_ = nullptr;
    std::map<PathElement, Value> locals_;
};


/*******************************************************************************/
/* LISP COMPILED EXPRESSION                                                    */
/*******************************************************************************/


struct CompiledExpression {

    // Execute the function directly
    Value operator () (ExecutionScope & outer) const
    {
        if (!execute_)
            MLDB_THROW_LOGIC_ERROR();
        using namespace std;
        cerr << "executor call" << endl;
        return execute_(outer);
    }

    PathElement name_;
    Executor execute_;
    CreateExecutionScope createScope_;
    Context * context_ = nullptr;
    Utf8String info_;  // for debug
    //LispValue source_;
    //LispValue type_;

    Value toValue() const;
};


/*******************************************************************************/
/* LISP COMPILATION SCOPE                                                      */
/*******************************************************************************/


struct CompilationScope {
    // Create a new scope in which to compile a program
    CompilationScope(Context & lcontext);
    CompilationScope(CompilationScope &&) = default;

    void exception(const Utf8String & reason) const MLDB_NORETURN;
    //std::function<void (Value val)> getVariableSetter(PathElement name);
    //std::function<Value ()> getVariableReference(PathElement name) const;

    // Consult the value, and mutate this compilation scope with the side effects
    // of anything (defun, set, etc) that was consulted.
    Value consult(const Value & program);

    // Evaluate the given expression in this context, inside a new execution
    // scope.  Side effects will cause an exception as this method does not
    // mutate this object.
    Value eval(const Value & program) const;

    // Compile the given expression in the context of this scope, returning a
    // callable version that evalutes to its output.
    CompiledExpression compile(const Value & expr) const;

    // Compile a call to the the given callable with the given parameters
    //CompiledExpression call(const Value & callable, const Value & parameters) const;

    // Bind the given symbol as a variable with the given value
    Value bindVariable(const Value & name, const Value & value);

    // Bind the given symbol as a function with the given compiler for when it's called
    Value bindFunction(const Value & name, FunctionCompiler call);

    FunctionCompiler getFunctionCompiler(const Path & fn) const;

    VariableReader getVariableReader(const PathElement & sym) const;
    VariableWriter getVariableWriter(const PathElement & sym) const;

    Context & getContext() const
    {
        ExcAssert(context_);
        return *context_;
    }

    // Return a scope creator which will enter a new scope with the given variables bound.
    // Each one has its name and the compiled expression used to construct its initial value.
    std::tuple<CompilationScope, CreateExecutionScope>
    enterScopeWithLocals(const std::vector<std::pair<PathElement, CompiledExpression>> & locals) const;

private:
    CompilationScope();
    CompilationScope(const CompilationScope & parent);
    std::vector<PathElement> importedNamespaces = { "std" };
    Context * context_ = nullptr;
    mutable uint64_t uniqueNumber_ = 0;

    // List of variables defined in this scope, along with the expression
    // that constructs their initial value.
    std::vector<std::pair<PathElement, CompiledExpression>> variables_;

    // List of functions defined in this scope.  Note that functions and variables
    // live in distinct namespaces, so it's not possible to confuse them.
    std::vector<std::pair<PathElement, FunctionCompiler>> functions_;

    /// Return a unique name to identify a symbol or function that's a temporary
    PathElement getUniqueName() const;
};

std::tuple<CompilationScope, Executor>
compileLispExpression(const Value & program, const CompilationScope & scope);


/*******************************************************************************/
/* LISP CONTEXT                                                                */
/*******************************************************************************/

void pushContext(Context & context);
void popContext(Context & context);
Context & getCurrentContext();

struct EnterContext {
    EnterContext(Context & context)
        : context(context)
    {
        pushContext(context);
    }

    ~EnterContext()
    {
        popContext(context);
    }

private:
    Context & context;
};

// TODO: this will expand...
// Designed to efficiently keep track of allocations of lisp objects, do GC if needed, etc
// Also to allow a single contiguous buffer so we can evaluate things / interoperate with
// GPUs
struct Context {
    virtual ~Context() = default;
    Value null() { return Value(*this, Null{}); }
    Value call(PathElement head);
    Value call(PathElement head, std::vector<Value> args);
    Value path(PathElement path);
    Value str(Utf8String str);
    Value u64(uint64_t i);
    Value i64(int64_t i);
    Value f64(double d);
    Value boolean(bool b);

    template<typename T>
    void addArg(List & l, T&&arg)
    {
        l.emplace_back(*this, std::forward<T>(arg));
    }

    template<typename T>
    void addArg(List & l, std::vector<T> arg)
    {
        l.insert(l.end(), std::make_move_iterator(arg.begin()), std::make_move_iterator(arg.end()));
    }

    void addArg(List & l, Value val)
    {
        val.verifyContext(this);
        l.emplace_back(std::move(val));
    }

    template<typename First, typename... Rest>
    void addArgs(List & l, First&&first, Rest&&... rest)
    {
        addArg(l, std::forward<First>(first));
        addArgs(l, std::forward<Rest>(rest)...);
    }

    void addArgs(List & l) {}

    template<typename... Args>
    Value list(Args&&... args)
    {
        List l;
        l.reserve(sizeof...(Args));
        addArgs(l, std::forward<Args>(args)...);
        return Value{*this, std::move(l)};
    }
};

} // namespace Lisp
} // namespace MLDB
