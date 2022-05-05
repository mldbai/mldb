/* lisp_lib_basics.cc                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib_impl.h"

using namespace std;

namespace MLDB {
namespace Lisp {

// (let (binding...) form...)
DEFINE_LISP_FUNCTION_COMPILER(let, std, "let")
{
    auto & context = scope.getContext();

    // Empty let does nothing and returns nil
    if (expr.empty()) {
        return { "let", returnNil, nullptr, &context, "LET ()" };
    }

    std::vector<CompiledExpression> args;
    std::vector<std::pair<PathElement, CompiledExpression>> locals;

    auto visitBind = [&] (const Value & val)
    {
        //cerr << "doing binding " << val << endl;
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

    //cerr << "locals.size() = " << locals.size() << endl;

    // The rest are the forms in which we evaluate it
    for (size_t i = 2;  i < expr.size();  ++i) {
        const Value & item = expr[i];
        args.emplace_back(scope.compile(item));
    }

    auto [innerScope, innerScopeCreator] = scope.enterScopeWithLocals(std::move(locals));

    CreateExecutionScope createScope
        = [innerScopeCreator=innerScopeCreator]
          (std::shared_ptr<ExecutionScope> outerScope, List args) -> std::shared_ptr<ExecutionScope>
    {
        return innerScopeCreator(outerScope, std::move(args));
    };

    Executor exec = [args] (ExecutionScope & scope) -> Value
    {
        Value result = scope.getContext().list();
        for (auto & a: args)
            result = a(scope);
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

    Executor exec = [todos] (ExecutionScope & scope) -> Value
    {
        Value result = scope.getContext().list();
        for (auto & [writer, calcValue]: todos) {
            writer(scope, result = calcValue(scope));            
        }

        return result;
    };

    return { "setq", std::move(exec), nullptr /* createScope */, &context, "SETQ (...)" };
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

    Executor exec = [todos] (ExecutionScope & scope) -> Value
    {
        List result;
        result.reserve(todos.size());
        for (auto & t: todos) {
            result.emplace_back(t(scope));
        }
        return { scope.getContext(), result };
    };

    return { "list", std::move(exec), nullptr /* createScope */, &context, "LIST (...)" };
}

// (quote val)
DEFINE_LISP_FUNCTION_COMPILER(quote, std, "quote")
{
    if (expr.size() != 2) {
        scope.exception("quote function takes one argument");
    }

    auto & context = scope.getContext();

    Value val = std::move(expr[1]);
    val.setQuotes(1);
    
    Executor exec = [val = expr[1]] (ExecutionScope & scope) -> Value
    {
        // TODO: switch context to execution cont4ext
        return val;
    };

    return { "quote", std::move(exec), nullptr /* createScope */, &context, "QUOTE (...)" };
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

    Executor exec = [cmp] (ExecutionScope & scope) -> Value
    {
        Value v = cmp(scope);
        if (!v.is<List>()) {
            scope.exception("length function applied to non-list " + v.print());
        }
        return scope.getContext().u64(v.as<List>().size());
    };

    return { "length", std::move(exec), nullptr /* createScope */, &context, "LENGTH (...)" };
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

    Executor exec = [nexp, lexp] (ExecutionScope & scope) -> Value
    {
        Value n = nexp(scope);
        Value l = lexp(scope);

        if (!l.is<List>()) {
            scope.exception("length function applied to non-list " + l.print());
        }

        return l.as<List>()[asUInt(n)];
    };

    return { "nth", std::move(exec), nullptr /* createScope */, &context, "NTH (...)" };
}

// (eval expr)
DEFINE_LISP_FUNCTION_COMPILER(eval, std, "eval")
{
    if (expr.size() != 2) {
        scope.exception("eval function takes one argument");
    }

    auto & context = scope.getContext();
    CompiledExpression toEval = scope.compile(std::move(expr[1]));

    Executor exec = [toEval] (ExecutionScope & scope) -> Value
    {
        Value program = toEval(scope);
        CompilationScope cscope(scope.getContext());
        Value result = cscope.eval(std::move(program));

        return result;
    };

    return { "eval", std::move(exec), nullptr /* createScope */, &context, "EVAL (...)" };
}

// (null expr)
DEFINE_LISP_FUNCTION_COMPILER(null, std, "null")
{
    if (expr.size() != 2) {
        scope.exception("null function takes one argument");
    }

    auto & context = scope.getContext();
    CompiledExpression toEval = scope.compile(std::move(expr[1]));

    Executor exec = [toEval] (ExecutionScope & scope) -> Value
    {
        Value val = toEval(scope);

        return scope.getContext().boolean(val.is<Null>() || (val.is<List>() && val.as<List>().empty()));
    };

    return { "null", std::move(exec), nullptr /* createScope */, &context, "NULL (...)" };
}

// (cond (pred1 val1) (pred2 val2)... )
DEFINE_LISP_FUNCTION_COMPILER(cond, std, "cond")
{
    // Decompose into the list of predicates and the resulting values
    std::vector<std::tuple<CompiledExpression, CompiledExpression>> clauses;
    for (size_t i = 1;  i < expr.size();  ++i) {
        const Value & clause = expr[i];
        if (!clause.is<List>()) {
            scope.exception("cond argument " + std::to_string(i - 1) + " should be list of (pred result) but is not a list: " + clause.print());
        }
        const List & l = clause.as<List>();
        if (l.size() != 2) {
            scope.exception("cond argument " + std::to_string(i - 1) + " should be two element list of (pred result) but doesn't have two elements: " + clause.print());
        }

        clauses.emplace_back(scope.compile(l[0]), scope.compile(l[1]));
    }

    auto & context = scope.getContext();

    Executor exec = [clauses = std::move(clauses)] (ExecutionScope & scope) -> Value
    {
        for (auto & [c, v]: clauses) {
            if (c(scope).truth()) {
                Value result = v(scope);
                cerr << "cond: returning " << result << endl;
                return result;
            }
        }

        return scope.getContext().null();
    };

    return { "cond", std::move(exec), nullptr /* createScope */, &context, "COND (...)" };
}

} // namespace Lisp
} // namespace MLDB
