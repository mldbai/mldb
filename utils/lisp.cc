/* lisp.cc                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp.h"
#include "lisp_lib.h"
#include "lisp_visitor.h"
#include "mldb/types/json_parsing.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/scope.h"
#include "mldb/types/any_impl.h"
#include "mldb/base/parse_context.h"

using namespace std;

namespace MLDB {
namespace Lisp {

namespace {

CompiledExpression constantGenerator(Value constant)
{
    CompiledExpression result;
    result.context_ = &constant.getContext();
    result.name_ = "__constant_" + constant.print();
    result.createScope_ = nullptr;
    result.execute_ = [constant=std::move(constant)] (ExecutionScope & scope)
    {
        return constant.toContext(scope.getContext());
    };
    result.info_ = "CONSTANT " + constant.print();
    return result;
}

FunctionCompiler compilerReturningExpression(CompiledExpression compiled)
{
    auto compiler = [compiled=std::move(compiled)] (const List & expr, const CompilationScope & scope) -> CompiledExpression
    {
        return compiled;
    };

    return std::move(compiler);
}

FunctionCompiler compilerReturningConstant(Value val)
{
    return compilerReturningExpression(constantGenerator(std::move(val)));
}

} // file scope


/*******************************************************************************/
/* LISP EXECUTION SCOPE                                                        */
/*******************************************************************************/

ExecutionScope::
ExecutionScope(Context & context)
    : context_(&context)
{
    ExcAssert(context_);
}

ExecutionScope::
ExecutionScope(ExecutionScope & parent)
    : parent_(&parent), context_(parent.context_)
{
}

ExecutionScope::
ExecutionScope(ExecutionScope & parent, std::map<PathElement, Value> locals)
    : parent_(&parent), context_(parent.context_), locals_(std::move(locals))
{
    //cerr << "Creating scope with locals" << endl;
    //for (auto & [n,v]: locals_) {
    //    cerr << n << " = " << v << endl;
    //}
}

Value
ExecutionScope::
getVariableValue(const PathElement & sym) const
{
    auto it = locals_.find(sym);
    if (it != locals_.end())
        return it->second;
    else {
        if (!parent_)
            return Value();
        else return parent_->getVariableValue(sym);
    }
}

void
ExecutionScope::
setVariableValue(const PathElement & sym, Value newValue)
{
    //cerr << "setting " << sym << " to " << newValue << endl;
    auto it = locals_.find(sym);
    if (it != locals_.end())
        it->second = std::move(newValue);
    else {
        if (!parent_)
            exception("couldn't setq of undefined global " + sym.toUtf8String());
        else parent_->setVariableValue(sym, std::move(newValue));
    }
}

void
ExecutionScope::
exception(const Utf8String & reason) const
{
    throw MLDB::Exception(reason);  // TODO: add context, etc
}

/*******************************************************************************/
/* LISP COMPILED EXPRESSION                                                    */
/*******************************************************************************/

Value
CompiledExpression::
toValue() const
{
    ExcAssert(this->context_);
    Function fn(name_, *this);
    return Value(*this->context_, std::move(fn));
}


/*******************************************************************************/
/* LISP COMPILATION SCOPE                                                      */
/*******************************************************************************/

CompilationScope::
CompilationScope(Context & lcontext)
    : context_(&lcontext)
{
}

CompilationScope::
CompilationScope()
{
}

CompilationScope::
CompilationScope(const CompilationScope & parent)
    : context_(parent.context_), parent_(&parent)
{
}

void
CompilationScope::
exception(const Utf8String & reason) const
{
    throw MLDB::Exception(reason);  // TODO: add context, etc
}

CompiledExpression
CompilationScope::
compile(const Value & program) const
{
    ExcAssert(context_);
    program.verifyContext(context_);

    // Compiling a quoted value just returns the value with one less level of quoting
    if (program.isQuoted())
        return constantGenerator(program.unquoted());

    CreateExecutionScope createExecutionScope;

    // If we have any variable with an initial value, we initialize our scope with it
    for (auto & v: variables_) {
        if (v.second.isUninitialized())
            continue;
        
        createExecutionScope = [variables = variables_] (std::shared_ptr<ExecutionScope> scope, const List & args) -> std::shared_ptr<ExecutionScope>
            {
                std::map<PathElement, Value> locals;

                for (auto & [name, value]: variables) {
                    if (value.isUninitialized())
                        continue;
                    locals[name] = value;
                }

                return locals.empty() ? scope : std::make_shared<ExecutionScope>(*scope, std::move(locals));
            };
        break;
    }

    LambdaVisitor visitor {
        [&] (Value val) -> CompiledExpression // this is for unmatched values
        {
            Executor result = [val = std::move(val)] (ExecutionScope & scope) -> Value
            {
                return val;
            };

            return { getUniqueName(), std::move(result), createExecutionScope, context_, "LITERAL" };
        },
        [&] (const Symbol & sym) -> CompiledExpression
        {
            PathElement name = sym.sym;
            VariableReader reader = this->getVariableReader(sym.sym);

            Executor result = [reader, name] (ExecutionScope & scope) -> Value
            {
                Value result = reader(scope);
                if (result.isUninitialized())
                    scope.exception("Couldn't find value of symbol " + name.toUtf8String());
                return result;
            };

            return { sym.sym, std::move(result), createExecutionScope, context_, "VARIABLE" };
        },
        [&] (const Value & val, const List & list) -> CompiledExpression
        {
            if (!list.empty() && list.front().is<Symbol>()) {
                const Symbol & sym = list.front().as<Symbol>();
                auto compiler = this->getFunctionCompiler(sym.sym);
                //cerr << "Getting function compiler for " << sym.sym << endl;
                return compiler(list, *this);
            }
            else {
                // Just a list, execute each element of it and return the result
                // as a list
                std::vector<CompiledExpression> exprs;
                for (const auto & expr: list) {
                    exprs.emplace_back(compile(expr));
                }

                Executor exec = [exprs] (ExecutionScope & scope) -> Value
                {
                    ListBuilder result;
                    for (auto & e: exprs) {
                        result.emplace_back(e(scope));
                    }
                    return { scope.getContext(), std::move(result) };
                };

                return { getUniqueName(), std::move(exec), createExecutionScope, context_, "LITERAL LIST" };
            }
        }
    };

    return visit(visitor, program);
}

std::optional<PathElement> evaluateAsSymbol(const Value & val)
{
    if (!val.is<Symbol>()) {
        return nullopt;
    }
    // TODO: evaluate an expression if it's constant

    const auto & sym = val.as<Symbol>();
    return sym.sym;
}

Value
CompilationScope::
consult(const Value & program)
{
    LambdaVisitor visitor {
        [&] (Value val) -> Value // first is for unmatched values
        {
            return val;
        },
        [&] (const Value & val, const List & l) -> Value
        {
            //cerr << "consult: got " << val << endl;
            if (l.empty())
                return val;
            auto & head = l.front();
            if (auto sym = evaluateAsSymbol(head)) {
                //cerr << "got sym " << *sym << endl;
                // It's a fixed symbol... we can implement it directly
                if (sym->stringEqual("set")) {
                    //cerr << "set" << endl;
                    for (size_t i = 1;  i < l.size();  i += 2) {
                        //cerr << i << " of " << l.size() << endl;
                        bindVariable(eval(l[i]), l.at(i + 1));
                    }
                    return l.back();
                }
                else if (sym->stringEqual("setq")) {
                    for (size_t i = 1;  i < l.size();  i += 2) {
                        bindVariable(l[i], l.at(i + 1));
                    }
                    return l.back();
                }
                else if (sym->stringEqual("defun")) {
                    // (defun fnname (args) form...)
                    if (l.size() < 4) {
                        exception("defun takes 3 arguments (defun <name> (args...) form): got "
                                  + std::to_string(l.size() - 1) + ": " + val.print());
                    }

                    //cerr << "doing defun " << l[1].print() << " " << l[2].print() << " " << l[3].print() << endl;

                    auto functionName = l[1].getSymbolName();

                    std::set<PathElement> parameterSet;
                    std::vector<PathElement> parameterNames;
                    for (const auto & param: l[2].as<List>()) {
                        if (!parameterSet.insert(param.getSymbolName()).second)
                            exception("compiling " + functionName.toUtf8String() + " : duplicate parameter name " + param.getSymbolName().toUtf8String());
                        parameterNames.emplace_back(param.getSymbolName());
                    }

                    // There are two ways to proceed from here:
                    // 1.  Compile right away a generic version of the function call, which
                    //     will work with any kind of arguments.
                    // 2.  Keep note of the function definition, and compile it with concrete
                    //     arguments when it's called.
                    //
                    // We currently are going to do 2, but some kind of hybrid may make sense,
                    // especially if a function is frequently called and thus needs to be compiled
                    // over and over or it's a recursive function call.
                    //
                    // Note also that only what is visible at the point of function definition
                    // should be available, so we definitely will need to do some kind of analysis
                    // at the point of definition.

                    // We know the parameter names
                    // When we compile a call to this, we need to invoke a compiler which can
                    // bind them to their values in the current scope.  This is a bit like inlining,
                    // and probably needs to be rethought for recursive function calls.

                    std::vector<Value> forms(l.begin() + 3, l.end());

                    FunctionCompiler compileCall
                        = [functionName, parameterNames, forms = std::move(forms)]
                            (const List & args, const CompilationScope & outerScope) -> CompiledExpression
                    {
                        //cerr << "compiling call to function " << functionName << " with " << args.size() << " args" << endl;
                        // This is called when we compile a call to the function.
                        //
                        // It's passed the full list that invokes the function, ie the name of the
                        // function in the first position and the arguments in subsequent positions.
                        //
                        // We need to:
                        //
                        // 1.  Compile each of the parameter expressions so that we can
                        //     evaluate the parameters.
                        // 2.  Bind each of the parameter expressions to the variable name that
                        //     it represents in the function definition.
                        // 3.  Compile the body of the function in scope of its arguments so
                        //     that it can evaluate.

                        // Can't have a function call without at least the name of the function
                        ExcAssert(!args.empty());

                        if (args.size() != parameterNames.size() + 1) {
                            outerScope.exception("function " + functionName.toUtf8String()
                                                 + " needs " + to_string(parameterNames.size()) + " arguments but "
                                                 + to_string(args.size() - 1) + " were passed");
                        }

                        std::vector<std::pair<PathElement, CompiledExpression>> compiledArgs(args.size() - 1);

                        for (size_t i = 1;  i < args.size();  ++i) {
                            compiledArgs[i-1] = { parameterNames[i-1], outerScope.compile(args[i]) };
                        }

                        auto [innerScope, createInnerScope] = outerScope.enterScopeWithLocals(std::move(compiledArgs));

                        std::vector<CompiledExpression> execBodies;
                        for (auto & f: forms) {
                            execBodies.emplace_back(innerScope.compile(f));
                        }

                        CompiledExpression result;
                        result.context_ = &outerScope.getContext();
                        result.name_ = functionName;
                        result.createScope_ = [compiledArgs, functionName] (std::shared_ptr<ExecutionScope> scope, List args) -> std::shared_ptr<ExecutionScope>
                        {
                            //cerr << "creating scope for call to " << functionName << endl;
                            std::map<PathElement, Value> locals;

                            // Execute each argument, and inject them as locals
                            for (size_t i = 0;  i < compiledArgs.size();  ++i) {
                                auto & [name, expr] = compiledArgs[i];
                                locals.emplace(name, expr(*scope));
                            }

                            return std::make_shared<ExecutionScope>(*scope, std::move(locals));
                        };

                        if (forms.size() == 1) {
                            // We just pass through the execution
                            result.execute_ = std::move(execBodies[0]);
                        }
                        else {
                            // Execute all the forms, return the last as our result
                            result.execute_ = [execBodies] (ExecutionScope & scope) -> Value
                            {
                                Value result = scope.getContext().list();
                                for (auto & b: execBodies) {
                                    result = b(scope);
                                }
                                return result;
                            };
                        }

                        result.info_ = "DEFUN " + functionName.toUtf8String();

                        return result;
                    };

                    return bindFunction(l[1], std::move(compileCall));
                }
                else {
                    cerr << "warning: consulting function " << *sym << " has no effect" << endl;
                    MLDB_THROW_UNIMPLEMENTED();
                }
            }
            else {
                // We have to evaluate it when we create the execution scope as it's
                // not a fixed symbol
                MLDB_THROW_UNIMPLEMENTED();
            }
        }
    };

    return visit(visitor, program);
}

Value
CompilationScope::
eval(const Value & program) const
{
    //cerr << "eval of " << program << endl;
    auto [name, executor, createXScope, context, info] = compile(program);
    auto outer = std::make_shared<ExecutionScope>(getContext());
    List args;  // no arguments
    std::shared_ptr<ExecutionScope> xscope = createXScope ? createXScope(outer, args) : outer;
    auto result = executor(*xscope);
    //cerr << "  eval of " << program << " returned " << result << endl;
    return result;
}

Value
CompilationScope::
bindVariable(const Value & name, const Value & value)
{
    //cerr << "binding symbol named " << name << " to value " << value << endl;

    LambdaVisitor nameVisitor {
        ExceptionOnUnknownReturning<PathElement>("Not implemented: evaluate symbol name"),
        [&] (const Symbol & sym) -> PathElement { return sym.sym; },
        // TODO: calculted symbol names?
    };

    PathElement nameVal = visit(nameVisitor, name);

    variables_.emplace_back(std::move(nameVal), value);

    return value;
}

Value
CompilationScope::
bindFunction(const Value & name, FunctionCompiler call)
{
    LambdaVisitor nameVisitor {
        ExceptionOnUnknownReturning<PathElement>("Not implemented: evaluate symbol name"),
        [&] (const Symbol & sym) -> PathElement { return sym.sym; },
        // TODO: calculted symbol names?
    };

    PathElement nameVal = visit(nameVisitor, name);

    functions_.emplace_back(std::move(nameVal), std::move(call));

    // TODO: return the real thing
    return context_->list();
}

FunctionCompiler
CompilationScope::
getFunctionCompiler(const Path & fn) const
{
    if (fn.size() != 1)
        MLDB_THROW_UNIMPLEMENTED("paths with size() != 1");

    // First look at what's defined locally
    for (auto & [name, compiler]: functions_) {
        if (name == fn.front()) {
            return compiler;
        }
    }

    // If not found, try the parent, or if not, try our imported namespaces
    if (parent_)
        return parent_->getFunctionCompiler(fn);
    else
        return lookupFunction(fn.front(), importedNamespaces);
}

VariableReader
CompilationScope::
getVariableReader(const PathElement & sym) const
{
    auto getVariable = [sym] (ExecutionScope & scope)
    {
        return scope.getVariableValue(sym);
    };

    return getVariable;
}

VariableWriter
CompilationScope::
getVariableWriter(const PathElement & sym) const
{
    auto setVariable = [sym] (ExecutionScope & scope, Value value)
    {
        scope.setVariableValue(sym, value);
    };

    return setVariable;
}

PathElement
CompilationScope::
getUniqueName() const
{
    return "__tmp_" + std::to_string(++uniqueNumber_);
}

std::tuple<CompilationScope, CreateExecutionScope>
CompilationScope::
enterScopeWithLocals(const std::vector<std::pair<PathElement, CompiledExpression>> & locals) const
{
    //cerr << "enterScopeWithLocals: " << locals.size() << " locals:";
    //for (auto & [name, val]: locals)
    //    cerr << " " << name;
    //cerr << endl;
    // Record its name but not its value... it gets initialized in the scope constructor below
    CompilationScope result(*this);
    for (auto & [name, expr]: locals) {
        result.variables_.emplace_back(name, Value());
    }
    CreateExecutionScope scopeConstructor
        = [symbols=locals] (std::shared_ptr<ExecutionScope> scope, List args) -> std::shared_ptr<ExecutionScope>
    {
        if (!args.empty()) {
            scope->exception("Locals scope doesn't take arguments");
        }
        ExcAssert(args.empty());
        std::map<PathElement, Value> newLocals;
        // Do it...
        for (auto & [name, constructor]: symbols) {
            auto val = constructor(*scope);
            //cerr << "constructing " << name << " = " << val << endl;
            newLocals[name] = val;
        }

        return std::make_shared<ExecutionScope>(*scope, std::move(newLocals));
    };

    return { std::move(result), std::move(scopeConstructor) };
}

std::tuple<CompilationScope, CreateExecutionScope>
CompilationScope::
enterScopeWithArgs(const std::vector<PathElement> & argNames) const
{
    CompilationScope result(*this);
    for (auto & name: argNames) {
        // Record its name, but don't initialize the value
        result.variables_.emplace_back(name, Value());
    }

    CreateExecutionScope scopeConstructor
        = [argNames=argNames] (std::shared_ptr<ExecutionScope> scope, List args) -> std::shared_ptr<ExecutionScope>
    {
        if (args.size() != argNames.size()) {
            scope->exception("Wrong number of arguments in scope creation");
        }

        std::map<PathElement, Value> newLocals;
        for (size_t i = 0;  i < argNames.size();  ++i) {
            newLocals[argNames[i]] = std::move(args[i]);
        }

        return std::make_shared<ExecutionScope>(*scope, std::move(newLocals));
    };

    return { std::move(result), std::move(scopeConstructor) };
}


/*******************************************************************************/
/* LISP CONTEXT                                                                */
/*******************************************************************************/

Value Context::call(PathElement head)
{
    ListBuilder l;
    l.emplace_back(*this, Symbol{std::move(head)});
    return { *this, std::move(l) };
}

Value Context::call(PathElement head, std::vector<Value> vals)
{
    ListBuilder l;
    l.emplace_back(*this, Symbol{std::move(head)});
    l.insert(l.end(), std::make_move_iterator(vals.begin()), std::make_move_iterator(vals.end()));
    return { *this, std::move(l) };
}

Value Context::sym(PathElement sym)
{
    return Value(*this, Symbol{std::move(sym)});
}

Value Context::str(Utf8String str)
{
    return Value{*this, std::move(str)};
}

Value Context::u64(uint64_t i)
{
    return Value{*this, i};
}

Value Context::i64(int64_t i)
{
    return Value{*this, i};
}

Value Context::f64(double d)
{
    return Value{*this, d};
}

Value Context::boolean(bool b)
{
    return Value(*this, b);
}

} // namespace Lisp
} // namespace MLDB
