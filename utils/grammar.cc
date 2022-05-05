/* grammar.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "grammar.h"
#include "types/structure_description.h"
#include "types/vector_description.h"
#include "mldb/base/scope.h"
#include "mldb/types/any_impl.h"
#include "lisp_visitor.h"
#include <shared_mutex>

using namespace std;

namespace MLDB {

// in json_parsing.cc
Utf8String expectJsonStringUtf8(ParseContext & context);
int getEscapedJsonCharacterPointUtf8(ParseContext & context);


namespace Grammar {

bool isUpperUtf8(int c)
{
    static const std::locale loc("en_US.UTF-8");
    return std::isupper((wchar_t)c, loc);
}

std::optional<PathElement>
match_rule_name(ParseContext & context)
{
    std::string segment;
    segment.reserve(18);
    skipLispWhitespace(context);
    if (!context) {
        return nullopt;
    }
    else {
        char c = *context;
        // Rules start with a capital letter or an @
        if (!isupper(c) && c != '@')
            return nullopt;
        segment += c;  c = *(++context);
        while (isalpha(c) || c == '_' || (!segment.empty() && isnumber(c))) {
            segment += c;
            ++context;
            if (context.eof())
                break;
            c = *context;
        }
        if (segment.empty())
            return nullopt;
        else return PathElement(segment);
    }
}

std::optional<PathElement>
match_variable_name(ParseContext & context)
{
    std::string segment;
    segment.reserve(18);
    skipLispWhitespace(context);
    if (!context) {
        return nullopt;
    }
    else {
        char c = *context;
        // Variables start with a lowercase letter or an $
        if (!islower(c) && c != '$')
            return nullopt;
        segment += c;  c = *(++context);
        while (isalpha(c) || c == '_' || (!segment.empty() && isnumber(c))) {
            segment += c;
            ++context;
            if (context.eof())
                break;
            c = *context;
        }
        if (segment.empty())
            return nullopt;
        else return PathElement(segment);
    }
}

optional<Utf8String> match_delimited_string(ParseContext & context, char delim)
{
    ParseContext::Revert_Token token(context);

    skipLispWhitespace(context);

    if (!context.match_literal(delim))
        return nullopt;

    try {
        char internalBuffer[4096];

        char * buffer = internalBuffer;
        size_t bufferSize = 4096;
        size_t pos = 0;
        Scope_Exit(if (buffer != internalBuffer) delete[] buffer);

        // Keep expanding until it fits
        while (!context.match_literal(delim)) {
            if (context.eof())
                return nullopt;

            // We need up to 4 characters to add a new UTF-8 code point
            if (pos >= bufferSize - 4) {
                size_t newBufferSize = bufferSize * 8;
                char * newBuffer = new char[newBufferSize];
                std::copy(buffer, buffer + bufferSize, newBuffer);
                if (buffer != internalBuffer)
                    delete[] buffer;
                buffer = newBuffer;
                bufferSize = newBufferSize;
            }

            int c = *context;
            
            //cerr << "c = " << c << " " << (char)c << endl;

            if (c < 0 || c > 127) {
                // Unicode
                c = context.expect_utf8_code_point();

                // 3.  Write the decoded character to the buffer
                char * p1 = buffer + pos;
                char * p2 = p1;
                pos += utf8::append(c, p2) - p1;

                continue;
            }
            ++context;

            if (c == '\\') {
                c = getEscapedJsonCharacterPointUtf8(context);
            }

            if (c < ' ' || c >= 127) {
                char * p1 = buffer + pos;
                char * p2 = p1;
                pos += utf8::append(c, p2) - p1;
            }
            else buffer[pos++] = c;
        }

        Utf8String result(string(buffer, buffer + pos));
        
        token.ignore();
        return result;
    } catch (const MLDB::Exception & exc) {
        return nullopt;
    }
}

struct IndentedParseContext: public ParseContext {
    using ParseContext::ParseContext;

    int indent = 0;

    bool next(int minIndent)
    {
        while (*this) {
            Rewind_Token token(*this);
            auto before = get_col();
            skipLispWhitespace(*this);
            if (match_eol()) {
                // blank line
                continue;
            }
            else if (match_literal('#')) {
                // comment
                skip_line();
                continue;
            }
            indent = get_col() - before;
            //cerr << "at " << where() << " indent = " << indent << endl;

            if (indent < minIndent) {
                token.apply();
                return false;
                break;
            }

            return true;
        }
        return false;
    }
};

optional<std::string> parseLispQualifiers(IndentedParseContext & context)
{
    std::string result;
    while (context) {
        switch (*context) {
        case '*': // fallthrough
        case '?': // fallthrough
        case '+': result += "+";  ++context;  break;
        default:
            return result;
        }
    }
    return result;
}

optional<std::vector<Value>> matchLispSequence(Lisp::Context & lcontext, IndentedParseContext & context)
{
    std::vector<Value> result;

    while (auto val = Value::match(lcontext, context)) {
        result.emplace_back(std::move(*val));
    }

    if (result.empty())
        return nullopt;

    return result;
}

optional<Value>
matchLispExpression(Lisp::Context & lcontext, IndentedParseContext & context)
{
    ParseContext::Revert_Token token(context);

    Value result;

    skipLispWhitespace(context);

    if (context.match_literal('*')) {
        auto next = matchLispExpression(lcontext, context);
        if (!next)
            return nullopt;
        result = lcontext.call("@deref", { std::move(*next) });
    }
    else if (auto val = Value::match(lcontext, context)) {
        result = std::move(*val);
    }
    else {
        return nullopt;
    }

    skipLispWhitespace(context);

    while (context) {
        //cerr << "got " << result.print() << " *context = " << *context << endl;
        if (context.match_literal('*')) {
            result = lcontext.call("@rep", { lcontext.u64(0), lcontext.null(), std::move(result) });
        }
        else if (context.match_literal('+')) {
            result = lcontext.call("@rep", { lcontext.u64(1), lcontext.null(), std::move(result) });
        }
        else if (context.match_literal('?')) {
            result = lcontext.call("@rep", { lcontext.u64(0), lcontext.u64(1), std::move(result) });
        }
        else break;
    }
    
    token.ignore();
    return std::move(result);
}

optional<std::vector<Value>>
matchLispExpressionSequence(Lisp::Context & lcontext, IndentedParseContext & context)
{
    std::vector<Value> result;

    while (auto val = matchLispExpression(lcontext, context)) {
        result.emplace_back(std::move(*val));
    }

    if (result.empty())
        return nullopt;

    return result;
}

std::optional<GrammarRule>
matchGrammarRule(Lisp::Context & lcontext, IndentedParseContext & context)
{
    GrammarRule result;
    auto minIndent = context.indent;

    ParseContext::Revert_Token token(context);

    auto name = match_rule_name(context);
    if (!name)
        return nullopt;
    result.name = *name;
    skipLispWhitespace(context);
    if (!context.match_literal(':'))
        return nullopt;
    skipLispWhitespace(context);
    context.expect_eol();
    int blockIndent = -1;

    while (context.next(minIndent + 1)) {
        if (blockIndent == -1) {
            blockIndent = context.indent;
        }
        else if (context.indent != blockIndent) {
            context.exception("inconsistent indentation");
        }

        auto rule = matchGrammarRule(lcontext, context);
        if (rule) {
            if (!result.rules.emplace(rule->name, std::move(*rule)).second) {
                context.exception("duplicate rule name");
            }
            continue;
        }

        GrammarProduction production;
        auto match = matchLispExpressionSequence(lcontext, context);
        if (!match)
            context.exception("expected lisp expression for match clause");
        if (match->size() == 1) {
            production.match = std::move(match->at(0));
        }
        else {
            production.match = lcontext.call("@seq", std::move(*match));
        }
        skipLispWhitespace(context);
        context.expect_literal("->");
        auto produce = matchLispExpressionSequence(lcontext, context);
        if (!produce)
            context.exception("expected lisp expression for match production");
        production.produce = lcontext.call(result.name, std::move(*produce));

        cerr << "got " << production.match.print() << " -> " << production.produce.print() << endl;
        skipLispWhitespace(context);
        context.expect_eol();
        result.productions.emplace_back(std::move(production));
    }

    token.ignore();
    return std::move(result);
}

GrammarRule parseGrammar(Lisp::Context & lcontext, ParseContext & contextIn)
{
    IndentedParseContext context;
    ParseContext & pcontext = context;
    pcontext = std::move(contextIn);

    auto result = matchGrammarRule(lcontext, context);
    if (!result)
        context.exception("expected main rule");
    while (context && (skipLispWhitespace(context) || context.match_eol())) ;
    context.expect_eof();

    return std::move(*result);
}

ParsingContext::
ParsingContext(Lisp::Context & lcontext, ParseContext & context)
    : Lisp::ExecutionScope(lcontext), context(context)
{
}

ParsingContext::
ParsingContext(const ParsingContext & parent)
    : Lisp::ExecutionScope(parent.getContext()), context(parent.context), parent(&parent)
{
}

ParsingContext
ParsingContext::
enter(PathElement scope) const
{
    return ParsingContext(*this);
}

const Value &
ParsingContext::
getVariable(PathElement name) const
{
    auto it = vars.find(name);
    if (it != vars.end()) {
        return it->second;
    }
    else {
        if (parent)
            return parent->getVariable(name);
        exception("attempt to read variable " + name.toUtf8String() + " that hasn't been set yet");
    }
}

void
ParsingContext::
setVariable(PathElement name, Value value)
{
    auto it = vars.find(name);
    if (it == vars.end()) {
        vars.emplace(std::move(name), std::move(value));
    }
    else {
        // TODO: unify
        exception("attempt to set variable " + name.toUtf8String() + " for a second time");
    }
}

void ParsingContext::exception(const Utf8String & reason) const
{
    context.exception(reason.rawString());
}

struct CompilationState {
    CompilationState() = default;
    CompilationState(const std::shared_ptr<CompilationState> & parent)
        : parent(parent)
    {
    }

    Parser getRule(const PathElement & ruleName) const
    {
        auto it = rules.find(ruleName);
        if (it == rules.end()) {
            auto sharedParent = parent.lock();
            if (sharedParent)
                return sharedParent->getRule(ruleName);
            throw MLDB::Exception(("rule " + ruleName.toUtf8String() + " not found").rawString());
        }
        return it->second;        
    };

    std::weak_ptr<CompilationState> parent;
    std::map<PathElement, Parser> rules;    
};

CompilationContext::
CompilationContext(Lisp::Context & lcontext)
    : Lisp::CompilationScope(lcontext), state(new CompilationState())
{
}

CompilationContext::
CompilationContext(CompilationContext & parent)
    : Lisp::CompilationScope(parent.getContext()), state(new CompilationState(parent.state))
{
}

std::shared_ptr<CompilationContext>
CompilationContext::
enter(PathElement where)
{
    return std::make_shared<CompilationContext>(*this);
}

void CompilationContext::exception(const Utf8String & reason) const
{
    throw MLDB::Exception(("compiling parser: " + reason).rawString());
}

void
CompilationContext::
setCompiledRule(PathElement ruleName, Parser parser)
{
    auto [it, success] = state->rules.emplace(std::move(ruleName), std::move(parser));
    if (!success)
        exception("rule " + it->first.toUtf8String() + " already defined");
}

std::function<Parser ()>
CompilationContext::
getCompiledRule(PathElement ruleName) const
{
    auto result = [state=this->state, ruleName=std::move(ruleName)] ()
    {
        return state->getRule(ruleName);
    };
    return result;
}

std::function<void (ParsingContext & context, Value val)>
CompilationContext::
getVariableSetter(PathElement name)
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::function<Value (const ParsingContext & context)>
CompilationContext::
getVariableReference(PathElement name) const
{
    // For now... later we'll find a more elegant way than reaching through the entire scope
    auto result = [name] (const ParsingContext & context) -> Value
    {
        return context.getVariable(name);
    };

    return result;
}

namespace {

using MatchFunctionCompiler = std::function<std::tuple<Parser, std::shared_ptr<CompilationContext>> (const Value & expr, std::shared_ptr<CompilationContext> context)>;

std::shared_mutex matchFunctionCompilersMutex;
std::map<PathElement, MatchFunctionCompiler> matchFunctionCompilers;

void addMatchFunctionCompiler(PathElement name, MatchFunctionCompiler compiler)
{
    std::unique_lock guard { matchFunctionCompilersMutex };
    auto [it, inserted] = matchFunctionCompilers.emplace(std::move(name), std::move(compiler));
    if (!inserted) {
        throw MLDB::Exception("function compiler " + it->first.toUtf8String().rawString() + " already registered");
    }
}

const MatchFunctionCompiler & getMatchFunctionCompiler(const PathElement & name)
{
    std::shared_lock guard { matchFunctionCompilersMutex };
    auto it = matchFunctionCompilers.find(name);
    if (it == matchFunctionCompilers.end()) {
        throw MLDB::Exception("function compiler " + name.toUtf8String().rawString() + " not registered");
    }
    return it->second;
}

struct RegisterMatchFunctionCompiler {
    RegisterMatchFunctionCompiler(const char * name, MatchFunctionCompiler fn)
    {
        addMatchFunctionCompiler(name, std::move(fn));
    };
};

#define DEFINE_MATCH_FUNCTION_COMPILER(identifier, name) \
static inline std::tuple<Parser, std::shared_ptr<CompilationContext>> compile_ ## identifier(const Value & expr, std::shared_ptr<CompilationContext> context); \
static const RegisterMatchFunctionCompiler register ## identifier(name, &compile_ ## identifier); \
std::tuple<Parser, std::shared_ptr<CompilationContext>> compile_ ## identifier(const Value & expr, std::shared_ptr<CompilationContext> context)

#define DEFINE_MATCH_FUNCTION(identifier, name) \
ParserOutput parse_ ## identifier(ParsingContext & context); \
DEFINE_MATCH_FUNCTION_COMPILER(identifier, name) { return { Parser{parse_ ## identifier}, nullptr }; } \
ParserOutput parse_ ## identifier(ParsingContext & context)

} // file scope

std::tuple<Parser, std::shared_ptr<CompilationContext>>
compileMatcher(const Value & expr, std::shared_ptr<CompilationContext> context);

DEFINE_MATCH_FUNCTION(jsonnstring, "@jsonstring")
{
    ParseContext::Revert_Token token(context.context);
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        auto res = expectJsonStringUtf8(context.context);
        token.ignore();
        return context.getContext().str(std::move(res));
    } MLDB_CATCH_ALL {
        return nullopt;
    }
}

DEFINE_MATCH_FUNCTION(i64, "@i64")
{
    int64_t val;
    if (!context.context.match_numeric(val))
        return nullopt;
    return context.getContext().i64(val);
}

DEFINE_MATCH_FUNCTION(u64, "@u64")
{
    uint64_t val;
    if (!context.context.match_numeric(val))
        return nullopt;
    return context.getContext().u64(val);
}

DEFINE_MATCH_FUNCTION(f64, "@f64")
{
    double val;
    if (!context.context.match_numeric(val))
        return nullopt;
    return context.getContext().f64(val);
}

DEFINE_MATCH_FUNCTION_COMPILER(seq, "@seq")
{
    // Compile each thing in the sequence
    std::vector<Parser> parsers;
    std::shared_ptr<CompilationContext> currentContext(context);

    for (auto & s: expr.as<Lisp::List>()) {
        auto [parser, nextContext] = compileMatcher(s, currentContext);
        parsers.emplace_back(std::move(parser));
        if (nextContext) currentContext = std::move(nextContext);
    }

    // Apply the sequence
    auto result = [parsers] (ParsingContext & context) -> ParserOutput
    {
        ParseContext::Revert_Token token(context.context);
        std::vector<Value> output;

        for (auto & parser: parsers) {
            auto thisOutput = parser.parse(context);
            if (!thisOutput)
                return nullopt;
            output.emplace_back(*thisOutput);
        }

        token.ignore();

        return context.getContext().call("@seq", std::move(output));
    };

    return { Parser{result}, currentContext == context ? nullptr : std::move(currentContext) };
}

DEFINE_MATCH_FUNCTION_COMPILER(rep, "@rep")
{
    uint64_t minRep = 0;
    uint64_t maxRep = -1;

    ExcAssertEqual(expr.as<Lisp::List>().size(), 3);
    minRep = expr.as<Lisp::List>()[0].as<uint64_t>();
    if (!expr.as<Lisp::List>()[1].is<Lisp::Null>()) {
        maxRep = expr.as<Lisp::List>()[1].as<uint64_t>();
    }
    ExcAssertLess(minRep, maxRep);

    auto [parser, subContext] = compileMatcher(expr.as<Lisp::List>()[2], context);

    // Apply the rule repeatedly until max count is reached or we didn't match
    auto result = [parser=parser, minRep, maxRep] (ParsingContext & pcontext) -> ParserOutput
    {
        // TODO: not if minRep is 0
        ParseContext::Revert_Token token(pcontext.context);

        std::vector<Value> output;

        for (size_t i = 0;  i < maxRep;  ++i) {
            auto thisOutput = parser.parse(pcontext);
            if (!thisOutput)
                break;
            output.emplace_back(std::move(*thisOutput));
        }

        if (output.size() < minRep) {
            return nullopt;
        }

        token.ignore();

        return pcontext.getContext().call("@rep", std::move(output));
    };

    // If there could be zero repetitions, we return the input context, otherwise the subContext
    return { Parser{result}, minRep == 0 ? nullptr : std::move(subContext) };
}

std::tuple<Parser, std::shared_ptr<CompilationContext>>
compileMatchString(const Utf8String & expr, std::shared_ptr<CompilationContext> context)
{
    auto parse = [=] (ParsingContext & pcontext) -> std::optional<Value>
    {
        if (!pcontext.context.match_literal_str(expr.rawData(), expr.rawLength()))
            return nullopt;

        return Value();
    };

    return { Parser{std::move(parse)}, std::move(context)};
}

std::tuple<Parser, std::shared_ptr<CompilationContext>>
compileMatchVariable(const PathElement & varName, std::shared_ptr<CompilationContext> context)
{
    MLDB_THROW_UNIMPLEMENTED();
}

static bool isRule(const Lisp::List & list)
{
    if (list.empty())
        return false;
    // Things that start with an uppercase character are rules
    auto nm = list.functionName();
    ExcAssertEqual(nm.size(), 1);
    int firstLetter = *nm[0].toUtf8String().begin();
    return isUpperUtf8(firstLetter);
}

std::tuple<Parser, std::shared_ptr<CompilationContext>>
compileMatcher(const Value & expr, std::shared_ptr<CompilationContext> contextIn)
{
    using namespace Lisp;
    LambdaVisitor visitor {
        [&] (const Value & val) -> std::tuple<Parser, std::shared_ptr<CompilationContext>>  // default case
        { 
            MLDB_THROW_UNIMPLEMENTED(("don't know how to compile matcher " + val.print()).c_str());
        },
        [&] (const Symbol & sym) -> std::tuple<Parser, std::shared_ptr<CompilationContext>>
        {
            return compileMatchVariable(sym.sym, contextIn);
        },
        [&] (const Utf8String & str) -> std::tuple<Parser, std::shared_ptr<CompilationContext>>
        {
            return compileMatchString(str, contextIn);
        },
        [&] (const List & list) -> std::tuple<Parser, std::shared_ptr<CompilationContext>>
        {

            Parser parser;
            std::shared_ptr<CompilationContext> currentContext = contextIn;

            // Things that start with an uppercase character are rules
            auto nm = list.functionName();
            if (isRule(list)) {
                auto rule = currentContext->getCompiledRule(nm[0]);
                auto result = [=] (ParsingContext & context) -> ParserOutput
                {
                    // We only instantiate the rule once called, as rules can be recursive
                    // and so aren't necesssarily available at call time.
                    static const Parser ruleParser = rule();
                    return ruleParser.parse(context);
                };

                parser = Parser{std::move(result)};
            }
            else {
                // It's a list... compile the matcher
                auto & compiler = getMatchFunctionCompiler(nm[0]);
                std::shared_ptr<CompilationContext> nextContext;
                std::tie(parser, nextContext) = compiler(expr, currentContext);
                if (nextContext)
                    currentContext = nextContext;
            }

            // We unify the arguments
            std::vector<Parser> argParsers;
            for (auto & arg: expr.as<Lisp::List>()) {
                auto [argParser, nextContext] = compileMatcher(arg, currentContext);
                if (nextContext)
                    currentContext = nextContext;
                argParsers.emplace_back(argParser);
            }

            auto result = [=] (ParsingContext & context) -> ParserOutput
            {
                auto output = parser.parse(context);
                if (!output)
                    return nullopt;  // no match
                auto & list = output->as<Lisp::List>();
#if 0
                if (output->type != expr.type)
                    return nullopt;  // didn't match the right type
#endif
                if (list.size() != argParsers.size())
                    return nullopt;  // wrong length
                ParsingContext argContext = context.enter("arg");
                for (size_t i = 0;  i < argParsers.size();  ++i) {
                    auto argOutput = argParsers[i].parse(argContext);
                }

                MLDB_THROW_UNIMPLEMENTED();
            };

            MLDB_THROW_UNIMPLEMENTED();
        }
    };
    
    return visit(visitor, expr);
}

namespace {

using ProduceFunction = std::function<Value (const ParsingContext & context)>;
using ProduceFunctionCompiler = std::function<ProduceFunction (const Value & expr, std::shared_ptr<CompilationContext> context)>;

std::shared_mutex produceFunctionCompilersMutex;
std::map<PathElement, ProduceFunctionCompiler> produceFunctionCompilers;

void addProduceFunctionCompiler(PathElement name, ProduceFunctionCompiler compiler)
{
    std::unique_lock guard { produceFunctionCompilersMutex };
    auto [it, inserted] = produceFunctionCompilers.emplace(std::move(name), std::move(compiler));
    if (!inserted) {
        throw MLDB::Exception("function compiler " + it->first.toUtf8String().rawString() + " already registered");
    }
}

const ProduceFunctionCompiler & getProduceFunctionCompiler(const PathElement & name)
{
    std::shared_lock guard { produceFunctionCompilersMutex };
    auto it = produceFunctionCompilers.find(name);
    if (it == produceFunctionCompilers.end()) {
        throw MLDB::Exception("function compiler " + name.toUtf8String().rawString() + " not registered");
    }
    return it->second;
}

struct RegisterProduceFunctionCompiler {
    RegisterProduceFunctionCompiler(const char * name, ProduceFunctionCompiler fn)
    {
        addProduceFunctionCompiler(name, std::move(fn));
    };
};

#define DEFINE_PRODUCE_FUNCTION_COMPILER(identifier, name) \
static inline ProduceFunction compile_ ## identifier(const Value & expr, std::shared_ptr<CompilationContext> context); \
static const RegisterProduceFunctionCompiler register ## identifier(name, &compile_ ## identifier); \
ProduceFunction compile_ ## identifier(const Value & expr, std::shared_ptr<CompilationContext> context)

#define DEFINE_PRODUCE_FUNCTION(identifier, name) \
Value produce_ ## identifier(const ParsingContext & context); \
DEFINE_PRODUCE_FUNCTION_COMPILER(identifier, name) { return produce_ ## identifier; } \
Value produce_ ## identifier(const ParsingContext & context)

} // file scope

DEFINE_PRODUCE_FUNCTION(deref, "@deref")
{
    MLDB_THROW_UNIMPLEMENTED();
}

ProduceFunction
compileProduceVariable(const PathElement & name, const std::shared_ptr<CompilationContext> ccontext)
{
    auto ref = ccontext->getVariableReference(name);

    auto result = [ref] (const ParsingContext & context) -> Value
    {
        return ref(context); 
    };
    return result;
}

ProduceFunction
compileProduceLiteral(const Value & val, std::shared_ptr<CompilationContext> ccontext)
{
    auto result = [val] (const auto & context) -> Value
    {
        return val;
    };
    return result;
}

ProduceFunction
compileProducer(const Value & expr, std::shared_ptr<CompilationContext> ccontext)
{
    using namespace Lisp;
    LambdaVisitor visitor {
        [&] (const Value & val) -> ProduceFunction  // default case
        { 
            return compileProduceLiteral(val, ccontext);  // return atom as-is
        },
        [&] (const Symbol & sym) -> ProduceFunction
        {
            return compileProduceVariable(sym.sym, ccontext);
        },
        [&] (const List & list) -> ProduceFunction
        {
            // Things that start with an uppercase character are rules
            // We can't produce them
            if (!isRule(list)) {
                // This is a list literal
                std::vector<ProduceFunction> argProducers;
                for (auto & arg: list) {
                    argProducers.emplace_back(compileProducer(arg, ccontext));
                }
                auto result = [expr, argProducers] (const ParsingContext & pcontext) -> Value
                {
                    std::vector<Value> args;
                    for (auto & producer: argProducers) {
                        args.emplace_back(producer(pcontext));
                    }
                    return pcontext.getContext().list(std::move(args));
                };
                return std::move(result);
            }

            // It's a list... what we do depends on the type
            auto & compiler = getProduceFunctionCompiler(list.simpleFunctionName());
            return compiler(expr, ccontext);
        },
    };

    return visit(visitor, expr);
}

Parser
GrammarProduction::
compile(std::shared_ptr<CompilationContext> ccontext) const
{
    cerr << "compiling " << match.print() << " --> " << produce.print() << endl;
    auto [matcher, mcontext] = compileMatcher(match, ccontext);
    auto producer = compileProducer(produce, mcontext);

    auto parse = [matcher=matcher, producer, matchin=this->match] (ParsingContext & pcontext) -> ParserOutput
    {
        cerr << "running matcher for " << matchin.print() << endl;
        auto match = matcher.parse(pcontext);
        cerr << "done running matcher for " << matchin.print() << endl;
        if (!match)
            return nullopt;
        cerr << "match produced " << match->print() << endl;
        auto result = producer(pcontext);
        cerr << "production returned " << result.print() << endl;
        return std::move(result);
    };

    return { std::move(parse) };
}

Parser
GrammarRule::
compile(std::shared_ptr<CompilationContext> context) const
{
    auto subContext = context->enter("GrammarRule::compile");
    for (auto & [name, rule]: rules) {
        subContext->setCompiledRule(name, rule.compile(subContext));
    }

    std::vector<Parser> productionParsers;
    for (auto & p: productions) {
        productionParsers.emplace_back(p.compile(subContext));
    }

    // To parse a grammar rule, we attempt the productions one by one
    // until we find one that works
    auto parse = [=] (ParsingContext & context) -> ParserOutput
    {
        cerr << "running parser for rule " << name << endl;
        for (auto & p: productionParsers) {
            if (auto output = p.parse(context)) {
                return output;
            }
        }
        return nullopt;
    };

    return { std::move(parse) };
}

ParserOutput
Parser::
parse(Lisp::Context & lcontext, ParseContext & gcontext) const
{
    ParsingContext pcontext(lcontext, gcontext);
    return parse(pcontext);
}

ParserOutput
Parser::
parse(Lisp::Context & lcontext, const std::string & toParse) const
{
    ParseContext context(toParse, toParse.data(), toParse.length());
    return parse(lcontext, context);
}

} // namespace Grammar
} // namespace MLDB
