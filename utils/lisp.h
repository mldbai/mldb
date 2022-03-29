/* lisp.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_value.h"
#include <optional>
#include <vector>
#include <memory>
#include <functional>
#include <map>

namespace MLDB {
namespace Lisp {

struct CompilationScope;
struct ExecutionScope;
struct Value;
struct CompilationState;


/*******************************************************************************/
/* LISP EXECUTION SCOPE                                                        */
/*******************************************************************************/

struct ExecutionScope {
    ExecutionScope(Context & context);

    Context & getContext() const { return *context_; }
private:
    Context * context_;
};


/*******************************************************************************/
/* LISP COMPILED FUNCTION                                                      */
/*******************************************************************************/


using Executor = std::function<Value (ExecutionScope & scope)>;
using CreateExecutionScope = std::function<std::shared_ptr<ExecutionScope>(const ExecutionScope &)>;

struct CompiledExpression {

    // Execute the function directly
    Value operator () (const ExecutionScope & outer) const;

    Executor execute_;
    CreateExecutionScope createScope_;
    //LispValue source_;
    //LispValue type_;
};

/*******************************************************************************/
/* LISP COMPILATION SCOPE                                                      */
/*******************************************************************************/

using FunctionCompiler
    = std::function<CompiledExpression
                    (const List & expr, const CompilationScope & scope)>;

struct CompilationScope {
    // Create a new scope in which to compile a program
    CompilationScope(Context & lcontext);

    void exception(const Utf8String & reason) const MLDB_NORETURN;
    std::function<void (Value val)> getVariableSetter(PathElement name);
    std::function<Value ()> getVariableReference(PathElement name) const;

    CompiledExpression compile(const Value & program) const;
    FunctionCompiler getFunctionCompiler(const Path & fn) const;

private:
    CompilationScope();
    CompilationScope(CompilationScope & parent);
    std::shared_ptr<CompilationState> state;
    std::vector<PathElement> importedNamespaces = { "std" };
    Context * context_ = nullptr;
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
    Value list(PathElement head);
    Value list(PathElement head, std::vector<Value> vals);
    Value path(PathElement path);
};

} // namespace Lisp
} // namespace MLDB
