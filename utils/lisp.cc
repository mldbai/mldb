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
    cerr << "setting " << sym << " to " << newValue << endl;
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
CompilationScope(Context & lcontext, SourceLocation loc, PathElement functionName)
    : context_(&lcontext), loc_(std::move(loc)), fn_(std::move(functionName))
{
}

CompilationScope::
CompilationScope()
{
}

CompilationScope::
CompilationScope(const CompilationScope & parent, SourceLocation loc, PathElement functionName)
    : context_(parent.context_), parent_(&parent), loc_(std::move(loc)), fn_(std::move(functionName))
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
            //cerr << "compiling list " << val << endl;
            if (!list.empty() && list.front().is<Symbol>()) {
                const Symbol & sym = list.front().as<Symbol>();

                //cerr << "call stack: " << endl;
                //for (const CompilationScope * cscope = this;  cscope;  cscope = cscope->parentScope()) {
                //    cerr << "  " << cscope->functionName() << endl;
                //}

                auto compiler = this->getFunctionCompiler(sym.sym);

                // If we're doing a recursive call of this function, we can't do an inline call
                bool inlineCall = true;//!inScopeOfFunction(sym.sym);

                if (inlineCall) {
                    return compiler(list, *this);
                }
                else {
                    // Get a reference to a compiled (generic) version of the function
                    List args = list.tail(1);
                    size_t nArgs = args.size();

                    std::vector<PathElement> argNames;
                    ListBuilder passedArgs;
                    std::vector<CompiledExpression> argExpressions;
                    passedArgs.emplace_back(this->getContext().sym(sym.sym));
                    for (size_t i = 0; i < nArgs;  ++i) {
                        const Value & arg = args[i];
                        PathElement argName = sym.sym.toUtf8String() + "_arg_" + std::to_string(i);
                        argNames.push_back(argName);
                        passedArgs.emplace_back(this->getContext().sym(argName));
                        argExpressions.emplace_back(this->compile(arg));
                    }

                    // TODO: function should be compiled with the scope as it was a the point of definition
                    // Currently we let it reference things at the call scope

                    SourceLocation loc = LISP_CREATE_SOURCE_LOCATION(val);
                    auto [callScope, createCallScope] = this->enterScopeWithArgs(argNames, loc, sym.sym);
                    CompiledExpression compiled = compiler(passedArgs, callScope);

                    Executor exec = [createCallScope = createCallScope, argExpressions, compiled] (ExecutionScope & scope) -> Value
                    {
                        // Evaluate the arguments
                        ListBuilder args;
                        for (auto & expr: argExpressions)
                            args.emplace_back(expr(scope));

                        std::shared_ptr<ExecutionScope> scopePtr(&scope, [] (...) {});

                        // Create a scope with the evaluated arguments
                        auto executionScope = createCallScope(scopePtr, args);

                        // Call the generic function
                        return compiled(*executionScope);
                    };

                    return { sym.sym, std::move(exec), createExecutionScope, context_, "CALL" };
                }
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

                    cerr << "doing defun " << l[1].print() << " " << l[2].print() << " " << l[3].print() << endl;

                    auto functionName = l[1].getSymbolName();
                    List forms = l.tail(3);

                    std::set<PathElement> parameterSet;
                    std::vector<PathElement> parameterNames;
                    const List & paramList = l[2].as<List>();
                    for (size_t i = 0;  i < paramList.size();  ++i) {
                        const auto & param = paramList[i];
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

                    auto sourceLocation = LISP_CREATE_SOURCE_LOCATION(val);

                    //cerr << "sourceLocation is " << jsonEncodeStr(sourceLocation) << endl;
                    //cerr << "value source location is " << jsonEncodeStr(getSourceLocation(val)) << endl;

                    SourceLocationEntry locEntry;
                    locEntry.file = sourceLocation.file();
                    locEntry.line = sourceLocation.line();
                    locEntry.column = sourceLocation.column();
                    locEntry.type = SourceLocationType::FUNCTION;
                    locEntry.name = functionName.toUtf8String();
                    sourceLocation.locations.emplace_back(std::move(locEntry));

                    auto [callScope, createCallScope] = this->enterScopeWithArgs(parameterNames, sourceLocation, functionName);

                    bool hasRecursiveCall = false;

                    auto compiledForms = std::make_shared<std::vector<CompiledExpression>>();

                    // Set up to intercept recursive calls of this function so we can handle
                    // them separately (and without recursing in the compilation)
                    FunctionCompiler compileRecursiveCall = [&, callScopePtr=&callScope] (const List & args, const CompilationScope & scope) -> CompiledExpression
                    {
                        cerr << "compiling recursive call to " << functionName << endl;
                        cerr << "args are " << Value(getContext(), args) << endl;
                        hasRecursiveCall = true;
                        CompiledExpression result;
                        result.context_ = &getContext();

                        std::vector<std::pair<PathElement, CompiledExpression>> compiledArgs(args.size() - 1);

                        for (size_t i = 1;  i < args.size();  ++i) {
                            compiledArgs[i-1] = { parameterNames[i-1], callScopePtr->compile(args[i]) };
                        }

                        result.createScope_ = [compiledArgs, functionName] (std::shared_ptr<ExecutionScope> scope, List args) -> std::shared_ptr<ExecutionScope>
                        {
                            std::map<PathElement, Value> locals;

                            // Execute each argument, and inject them as locals
                            for (size_t i = 0;  i < compiledArgs.size();  ++i) {
                                auto & [name, expr] = compiledArgs[i];
                                locals.emplace(name, expr(*scope));
                            }

                            return std::make_shared<ExecutionScope>(*scope, std::move(locals));
                        };

                        result.execute_ = [compiledForms] (ExecutionScope & scope) -> Value
                        {
                            Value result = scope.getContext().list();
                            for (auto & f: *compiledForms) {
                                result = f(scope);
                            }
                            return result;
                        };

                        result.name_ = functionName;
                        result.info_ = "RECURSIVE CALL TO " + functionName.toUtf8String();

                        return result;
                    };

                    callScope.bindFunction(getContext().sym(functionName), compileRecursiveCall);

                    //cerr << "Compiling forms" << endl;
                    for (const auto & f: forms) {
                        compiledForms->emplace_back(callScope.compile(f));
                    }
                    //cerr << "Done compiling forms" << endl;

#if 0
                    Executor exec = [createCallScope = createCallScope, compiledForms] (ExecutionScope & scope) -> Value
                    {
                        Value result = scope.getContext().null();
                        for (auto & form: compiledForms) {
                            result = form(scope);
                        }
                        return result;
                    };
#endif

                    FunctionCompiler compileCall
                        = [functionName, parameterNames, forms = std::move(forms), compiledForms, sourceLocation]
                            (const List & args, const CompilationScope & outerScope) -> CompiledExpression
                    {
                        cerr << "compiling call to function " << functionName << " with " << args.size() - 1 << " args:";
                        for (size_t i = 1;  i < args.size();  ++i) cerr << " " << args[i];
                        cerr << endl;

                        //cerr << "call stack: " << endl;
                        //for (const CompilationScope * cscope = &outerScope;  cscope;  cscope = cscope->parentScope()) {
                        //    cerr << jsonEncodeStr(cscope->sourceLocation()) << endl;
                        //}

                        bool isRecursive = outerScope.inScopeOfFunction(functionName);
                        if (isRecursive) {
                            cerr << "  RECURSIVE CALL" << endl;
                        }

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
                        // 3.  Either
                        // 3.a   Compile the body of the function in scope of its arguments so
                        //       that it can evaluate (inline), or
                        // 3.b   Call the generically compiled function body with the arguments in the frame

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

                        bool inlineCall = !isRecursive;

                        if (inlineCall) {

                            auto [innerScope, createInnerScope] = outerScope.enterScopeWithLocals(std::move(compiledArgs), sourceLocation, functionName);

                            std::vector<CompiledExpression> execBodies;
                            for (const auto & f: forms) {
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

                            result.info_ = "DEFUN INLINE " + functionName.toUtf8String();
                            return result;
                        }
                        else {
                            cerr << "compiling non-inline call" << endl;
                            auto [innerScope, createInnerScope] = outerScope.enterScopeWithArgs(parameterNames, sourceLocation, functionName);

                            CompiledExpression result;
                            result.context_ = &outerScope.getContext();
                            result.name_ = functionName;
                            result.createScope_ = [compiledArgs, functionName, createInnerScope=createInnerScope] (std::shared_ptr<ExecutionScope> scope, List unusedArgs) -> std::shared_ptr<ExecutionScope>
                            {
                                cerr << "creating scope for deferred call to " << functionName << endl;
                                ExcAssert(unusedArgs.empty());
                                ListBuilder args;

                                // Execute each argument, and inject them as locals
                                for (size_t i = 0;  i < compiledArgs.size();  ++i) {
                                    auto & [name, expr] = compiledArgs[i];
                                    args.emplace_back(expr(*scope));
                                    cerr << "  arg " << i << " " << name << " " << expr.info_ << " = " << args.back() << endl;
                                }

                                return createInnerScope(scope, std::move(args));
                            };

                            // Execute all the forms, return the last as our result
                            result.execute_ = [functionName, compiledForms] (ExecutionScope & scope) -> Value
                            {
                                cerr << "executing deferred call to " << functionName << endl;

                                Value result = scope.getContext().list();
                                for (auto & f: *compiledForms) {
                                    result = f(scope);
                                }

                                cerr << "  done executing deferred call; result = " << result << endl;
                                return result;
                            };

                            result.info_ = "FIRST RECURSIVE CALL " + functionName.toUtf8String();

                            return result;
                        }
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
        cerr << "setting variable " << sym << " to value " << value << endl;
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
enterScopeWithLocals(const std::vector<std::pair<PathElement, CompiledExpression>> & locals,
                     SourceLocation loc, PathElement functionName) const
{
    //cerr << "enterScopeWithLocals: " << locals.size() << " locals:";
    //for (auto & [name, val]: locals)
    //    cerr << " " << name;
    //cerr << endl;
    // Record its name but not its value... it gets initialized in the scope constructor below
    CompilationScope result(*this, std::move(loc), std::move(functionName));
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
enterScopeWithArgs(const std::vector<PathElement> & argNames, SourceLocation loc, PathElement functionName) const
{
    CompilationScope result(*this, std::move(loc), std::move(functionName));
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

bool
CompilationScope::
inScopeOfFunction(PathElement fn) const
{
    const CompilationScope * scope = this;

    while (scope) {
        if (scope->fn_ == fn)
            return true;
        scope = scope->parent_;
    }

    return false;
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

Value Context::loc(SourceLocation loc)
{
    return Value(*this, std::move(loc));
}
} // namespace Lisp
} // namespace MLDB
