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
#include <shared_mutex>

using namespace std;

namespace MLDB {

bool isUpperUtf8(int c)
{
    static const std::locale loc("en_US.UTF-8");
    return std::isupper((wchar_t)c, loc);
}

Value Value::list(PathElement type, std::vector<Value> args)
{
    Value result;
    result.type = std::move(type);
    result.args = std::move(args);
    return result;
}

Value Value::path(PathElement path)
{
    Value result;
    result.atom = std::move(path);
    return result;
}

Value Value::var(PathElement name)
{
    Value result;
    result.atom = Variable{std::move(name)};
    return result;
}

Value Value::str(Utf8String str)
{
    Value result;
    result.atom = std::move(str);
    return result;
}

Value Value::boolean(bool b)
{
    Value result;
    result.atom = b;
    return result;
}

Value Value::i64(int64_t i)
{
    Value result;
    result.atom = i;
    return result;
}

Value Value::u64(uint64_t i)
{
    Value result;
    result.atom = i;
    return result;
}

Value Value::f64(double d)
{
    Value result;
    result.atom = d;
    return result;
}

Value Value::null()
{
    Value result;
    result.atom = Null{};
    return result;
}

Utf8String Value::print() const
{
    Utf8String result;
    if (type.null()) {
        result = jsonEncodeStr(this->atom);
    }
    else {
        result = "(" + type.toUtf8String();
        for (auto & a: args)
            result += " " + a.print();
        result += ")";
    }
    return result;
}

bool
Value::
operator == (const Value & other) const
{
    return type == other.type && args == other.args && atom == other.atom;
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Variable)
{
    addField("name", &Variable::name, "Name of variable");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Null)
{
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Value)
{
    addAuto("type", &Value::type, "Type of list");
    addAuto("args", &Value::args, "Arguments of list");
    addAuto("atom", &Value::atom, "Atom value");
}

std::optional<PathElement>
match_rule_name(ParseContext & context)
{
    std::string segment;
    segment.reserve(18);
    context.skip_whitespace();
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
    context.skip_whitespace();
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

int getEscapedJsonCharacterPointUtf8(ParseContext & context);

optional<Utf8String> match_delimited_string(ParseContext & context, char delim)
{
    ParseContext::Revert_Token token(context);

    context.skip_whitespace();

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
            skip_whitespace();
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

optional<Value> parseLisp(IndentedParseContext & context)
{
    Value result;
    context.skip_whitespace();
    if (context.match_literal('(')) {
        auto type = match_rule_name(context);
        if (!type)
            context.exception("expected list type");
        result.type = std::move(*type);
        std::optional<Value> arg;
        while (context.match_whitespace() && (arg = parseLisp(context))) {
            result.args.emplace_back(*arg);
        }
        context.skip_whitespace();
        context.expect_literal(')', "expected closing lisp expression");
    }
    else if (auto str = match_delimited_string(context, '\'')) {
        result = Value::str(std::move(*str));
    }
    else if (context.match_literal("true")) {
        result = Value::boolean(true);
    }
    else if (context.match_literal("false")) {
        result = Value::boolean(false);
    }
    else if (context.match_literal("null")) {
        result = Value::null();
    }
    else if (auto varName = match_variable_name(context)) {
        result = Value::var(std::move(*varName));
    }
    else return nullopt;

    return std::move(result);
}

optional<std::vector<Value>> parseLispSequence(IndentedParseContext & context)
{
    std::vector<Value> result;

    while (auto val = parseLisp(context)) {
        result.emplace_back(std::move(*val));
    }

    if (result.empty())
        return nullopt;

    return result;
}

optional<Value>
parseLispExpression(IndentedParseContext & context)
{
    ParseContext::Revert_Token token(context);

    Value result;

    context.skip_whitespace();

    if (context.match_literal('*')) {
        auto next = parseLispExpression(context);
        if (!next)
            return nullopt;
        result = Value::list("@deref", { std::move(*next) });
    }
    else if (auto val = parseLisp(context)) {
        result = std::move(*val);
    }
    else {
        return nullopt;
    }

    context.skip_whitespace();

    while (context) {
        //cerr << "got " << result.print() << " *context = " << *context << endl;
        if (context.match_literal('*')) {
            result = Value::list("@rep", { Value::u64(0), Value::null(), std::move(result) });
        }
        else if (context.match_literal('+')) {
            result = Value::list("@rep", { Value::u64(1), Value::null(), std::move(result) });
        }
        else if (context.match_literal('?')) {
            result = Value::list("@rep", { Value::u64(0), Value::u64(1), std::move(result) });
        }
        else break;
    }
    
    token.ignore();
    return std::move(result);
}

optional<std::vector<Value>> parseLispExpressionSequence(IndentedParseContext & context)
{
    std::vector<Value> result;

    while (auto val = parseLispExpression(context)) {
        result.emplace_back(std::move(*val));
    }

    if (result.empty())
        return nullopt;

    return result;
}

std::optional<GrammarRule> parseGrammarRule(IndentedParseContext & context)
{
    GrammarRule result;
    auto minIndent = context.indent;

    ParseContext::Revert_Token token(context);

    auto name = match_rule_name(context);
    if (!name)
        return nullopt;
    result.name = *name;
    context.skip_whitespace();
    if (!context.match_literal(':'))
        return nullopt;
    context.skip_whitespace();
    context.expect_eol();
    int blockIndent = -1;

    while (context.next(minIndent + 1)) {
        if (blockIndent == -1) {
            blockIndent = context.indent;
        }
        else if (context.indent != blockIndent) {
            context.exception("inconsistent indentation");
        }

        auto rule = parseGrammarRule(context);
        if (rule) {
            if (!result.rules.emplace(rule->name, std::move(*rule)).second) {
                context.exception("duplicate rule name");
            }
            continue;
        }

        GrammarProduction production;
        auto match = parseLispExpressionSequence(context);
        if (!match)
            context.exception("expected lisp expression for match clause");
        if (match->size() == 1) {
            production.match = std::move(match->at(0));
        }
        else {
            production.match = Value::list("@seq", std::move(*match));
        }
        context.skip_whitespace();
        context.expect_literal("->");
        auto produce = parseLispExpressionSequence(context);
        if (!produce)
            context.exception("expected lisp expression for match production");
        production.produce = Value::list(result.name, std::move(*produce));

        cerr << "got " << production.match.print() << " -> " << production.produce.print() << endl;
        context.skip_whitespace();
        context.expect_eol();
        result.productions.emplace_back(std::move(production));
    }

    token.ignore();
    return std::move(result);
}

GrammarRule parseGrammar(ParseContext & contextIn)
{
    IndentedParseContext context;
    ParseContext & pcontext = context;
    pcontext = std::move(contextIn);

    auto result = parseGrammarRule(context);
    if (!result)
        context.exception("expected main rule");
    while (context && (context.match_whitespace() || context.match_eol())) ;
    context.expect_eof();

    return std::move(*result);
}

ParsingContext::
ParsingContext(ParseContext & context, const ParsingContext * parent)
    : context(context), parent(parent)
{
}

ParsingContext
ParsingContext::
enter(PathElement scope) const
{
    return ParsingContext(this->context, this);
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
CompilationContext()
    : state(new CompilationState())
{
}

CompilationContext::
CompilationContext(CompilationContext & parent)
    : state(new CompilationState(parent.state))
{
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

using MatchFunctionCompiler = std::function<std::tuple<Parser, CompilationContext> (const Value & expr, CompilationContext & context)>;

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
static inline std::tuple<Parser, CompilationContext> compile_ ## identifier(const Value & expr, CompilationContext & context); \
static const RegisterMatchFunctionCompiler register ## identifier(name, &compile_ ## identifier); \
std::tuple<Parser, CompilationContext> compile_ ## identifier(const Value & expr, CompilationContext & context)

#define DEFINE_MATCH_FUNCTION(identifier, name) \
ParserOutput parse_ ## identifier(ParsingContext & context); \
DEFINE_MATCH_FUNCTION_COMPILER(identifier, name) { return { Parser{parse_ ## identifier}, context }; } \
ParserOutput parse_ ## identifier(ParsingContext & context)

} // file scope

std::tuple<Parser, CompilationContext>
compileMatcher(const Value & expr, CompilationContext & context);

// in json_parsing.cc
Utf8String expectJsonStringUtf8(ParseContext & context);
DEFINE_MATCH_FUNCTION(jsonnstring, "@jsonstring")
{
    ParseContext::Revert_Token token(context.context);
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        auto res = expectJsonStringUtf8(context.context);
        token.ignore();
        return Value::str(std::move(res));
    } MLDB_CATCH_ALL {
        return nullopt;
    }
}

DEFINE_MATCH_FUNCTION(i64, "@i64")
{
    int64_t val;
    if (!context.context.match_numeric(val))
        return nullopt;
    return Value::i64(val);
}

DEFINE_MATCH_FUNCTION(u64, "@u64")
{
    uint64_t val;
    if (!context.context.match_numeric(val))
        return nullopt;
    return Value::u64(val);
}

DEFINE_MATCH_FUNCTION(f64, "@f64")
{
    double val;
    if (!context.context.match_numeric(val))
        return nullopt;
    return Value::f64(val);
}

DEFINE_MATCH_FUNCTION_COMPILER(seq, "@seq")
{
    // Compile each thing in the sequence
    std::vector<Parser> parsers;
    CompilationContext lastContext = context;
    for (auto & s: expr.args) {
        auto [parser, nextContext] = compileMatcher(s, lastContext);
        parsers.emplace_back(std::move(parser));
        lastContext = std::move(nextContext);
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

        return Value::list("@seq", std::move(output));
    };

    return { Parser{result}, context };
}

DEFINE_MATCH_FUNCTION_COMPILER(rep, "@rep")
{
    uint64_t minRep = 0;
    uint64_t maxRep = -1;

    ExcAssertEqual(expr.args.size(), 3);
    minRep = expr.args[0].atom.as<uint64_t>();
    if (!expr.args[1].atom.is<Null>()) {
        maxRep = expr.args[1].atom.as<uint64_t>();
    }
    ExcAssertLess(minRep, maxRep);

    auto [parser, subContext] = compileMatcher(expr.args[2], context);

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

        return Value::list("@rep", std::move(output));
    };

    // If there could be zero repetitions, we return the input context, otherwise the subContext
    return { Parser{result}, minRep == 0 ? context : subContext };
}

std::tuple<Parser, CompilationContext>
compileMatchString(const Utf8String & expr, CompilationContext & context)
{
    auto parse = [=] (ParsingContext & pcontext) -> std::optional<Value>
    {
        if (!pcontext.context.match_literal_str(expr.rawData(), expr.rawLength()))
            return nullopt;

        return Value();
    };

    return { Parser{std::move(parse)}, context };
}

std::tuple<Parser, CompilationContext>
compileMatchVariable(const PathElement & varName, CompilationContext & context)
{
    MLDB_THROW_UNIMPLEMENTED();
}

std::tuple<Parser, CompilationContext>
compileMatcher(const Value & expr, CompilationContext & contextIn)
{
    if (!expr.type.null()) {
        // Things that start with an uppercase character are rules
        ExcAssert(!expr.type.empty());

        Parser parser;
        CompilationContext context = contextIn;

        int firstLetter = *expr.type.toUtf8String().begin();
        if (isUpperUtf8(firstLetter)) {
            auto rule = context.getCompiledRule(expr.type);
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
            auto & compiler = getMatchFunctionCompiler(expr.type);
            std::tie(parser, context) = compiler(expr, context);
        }

#if 0
        using Unifier = std::function<bool (Value val, ParsingContext & context)>;

        auto compileUnifier = [] (const Value & expr, CompilationContext & context) -> Unifier
        {
            if (!expr.type.null()) {
                MLDB_THROW_UNIMPLEMENTED("unification of lists");
            }
            else if (expr.atom.is<Variable>()) {
                return compileMatchVariable(expr.atom.as<Variable>().name, context);
            }
        };
#endif

        // We unify the arguments
        std::vector<Parser> argParsers;
        for (auto & arg: expr.args) {
            CompilationContext argContext(context);
            auto [argParser, nextContext] = compileMatcher(arg, argContext);
            argParsers.emplace_back(argParser);
        }

        auto result = [=] (ParsingContext & context) -> ParserOutput
        {
            auto output = parser.parse(context);
            if (!output)
                return nullopt;  // no match
            if (output->type != expr.type)
                return nullopt;  // didn't match the right type
            if (output->args.size() != argParsers.size())
                return nullopt;  // wrong length
            ParsingContext argContext = context.enter("arg");
            for (size_t i = 0;  i < argParsers.size();  ++i) {
                auto argOutput = argParsers[i].parse(argContext);
            }
        };
    }
    else {
        if (expr.atom.is<Utf8String>()) {
            return compileMatchString(expr.atom.as<Utf8String>(), context);
        }
        else if (expr.atom.is<Variable>()) {
            return compileMatchVariable(expr.atom.as<Variable>().name, context);
        }
        else {
            context.exception("don't know how to match literal " + expr.atom.asJsonStr() + " of type " + demangle(expr.atom.type()));
        }
    }
}

namespace {

using ProduceFunction = std::function<Value (const ParsingContext & context)>;
using ProduceFunctionCompiler = std::function<ProduceFunction (const Value & expr, CompilationContext & context)>;

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
static inline ProduceFunction compile_ ## identifier(const Value & expr, CompilationContext & context); \
static const RegisterProduceFunctionCompiler register ## identifier(name, &compile_ ## identifier); \
ProduceFunction compile_ ## identifier(const Value & expr, CompilationContext & context)

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
compileProduceVariable(const PathElement & name, const CompilationContext & ccontext)
{
    auto ref = ccontext.getVariableReference(name);

    auto result = [ref] (const ParsingContext & context) -> Value
    {
        return ref(context); 
    };
    return result;
}

ProduceFunction
compileProduceLiteral(const Value & val, const CompilationContext & ccontext)
{
    auto result = [val] (const auto & context) -> Value
    {
        return val;
    };
    return result;
}

ProduceFunction
compileProducer(const Value & expr, CompilationContext & ccontext)
{
    if (!expr.type.null()) {
        // Things that start with an uppercase character are rules
        // We can't produce them
        ExcAssert(!expr.type.empty());
        int firstLetter = *expr.type.toUtf8String().begin();
        if (isUpperUtf8(firstLetter)) {
            // This is a literal
            std::vector<ProduceFunction> argProducers;
            for (auto & arg: expr.args) {
                argProducers.emplace_back(compileProducer(arg, ccontext));
            }
            auto result = [expr, argProducers] (const ParsingContext & pcontext) -> Value
            {
                std::vector<Value> args;
                for (auto & producer: argProducers) {
                    args.emplace_back(producer(pcontext));
                }
                return Value::list(expr.type, std::move(args));
            };
            return std::move(result);
        }

        // It's a list... what we do depends on the type
        auto & compiler = getProduceFunctionCompiler(expr.type);
        return compiler(expr, ccontext);
    }
    else {
        if (expr.atom.is<Variable>()) {
            return compileProduceVariable(expr.atom.as<Variable>().name, ccontext);
        }
        else {
            return compileProduceLiteral(expr, ccontext);  // return atom as-is
        }
    }
}

Parser
GrammarProduction::
compile(CompilationContext & ccontext) const
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
compile(CompilationContext & context) const
{
    CompilationContext subContext(context);
    for (auto & [name, rule]: rules) {
        subContext.setCompiledRule(name, rule.compile(subContext));
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
parse(ParseContext & gcontext) const
{
    ParsingContext pcontext(gcontext);
    return parse(pcontext);
}

ParserOutput
Parser::
parse(const std::string & toParse) const
{
    ParseContext context(toParse, toParse.data(), toParse.length());
    return parse(context);
}


} // namespace MLDB
