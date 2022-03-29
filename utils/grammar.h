/* grammar.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <optional>
#include "mldb/base/parse_context.h"
#include <vector>
#include <memory>
#include <functional>
#include <map>
#include "mldb/types/string.h"
#include "mldb/types/path.h"
#include "mldb/types/any.h"


namespace MLDB {

struct Parser;
struct ParsingContext;
struct CompilationContext;
struct ExecutionContext;
struct Value;

using ParserOutput = std::optional<Value>;

struct Variable {
    PathElement name;
};

struct Null {
};

DECLARE_STRUCTURE_DESCRIPTION(Variable);
DECLARE_STRUCTURE_DESCRIPTION(Null);

struct Value {
    PathElement type;
    std::vector<Value> args;
    Any atom;

    static Value list(PathElement type, std::vector<Value> args);
    static Value var(PathElement name);
    static Value path(PathElement path);
    static Value str(Utf8String str);
    static Value boolean(bool val);
    static Value i64(int64_t val);
    static Value u64(uint64_t val);
    static Value f64(double val);
    static Value null();

    Utf8String print() const;

    bool operator == (const Value & other) const;
    bool operator != (const Value & other) const = default;
};

DECLARE_STRUCTURE_DESCRIPTION(Value);

struct CompilationState;
struct ParsingContext;

struct CompilationContext {
    CompilationContext();
    CompilationContext(CompilationContext & parent);

    void exception(const Utf8String & reason) const MLDB_NORETURN;
    void setCompiledRule(PathElement ruleName, Parser parser);
    std::function<Parser ()> getCompiledRule(PathElement ruleName) const;
    std::function<void (ParsingContext & context, Value val)> getVariableSetter(PathElement name);
    std::function<Value (const ParsingContext & context)> getVariableReference(PathElement name) const;

private:
    std::shared_ptr<CompilationState> state;
};

struct ExecutionContext {

};


struct GrammarProduction {
    Value match;
    Value produce;
    Parser compile(CompilationContext & context) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GrammarProduction);

struct Parser;

struct GrammarRule {
    PathElement name;
    std::map<PathElement, GrammarRule> rules;
    std::vector<GrammarProduction> productions;
    Parser compile(CompilationContext & context) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GrammarRule);

GrammarRule parseGrammar(ParseContext & context);

struct ParsingContext {
    ParsingContext(ParseContext & context, const ParsingContext * parent = nullptr);

    ParsingContext enter(PathElement scope) const;
    const Value & getVariable(PathElement name) const;
    void setVariable(PathElement name, Value value);
    void exception(const Utf8String & reason) const MLDB_NORETURN;

    ParseContext & context;
    Value input;

private:
    const ParsingContext * parent = nullptr;
    std::map<PathElement, Value> vars;
};

struct Parser {
    ParserOutput parse(ParseContext & pcontext) const;
    ParserOutput parse(const std::string & toParse) const;

    inline ParserOutput parse(ParsingContext & context) const
    {
        return parse_(context);
    }
    std::function<ParserOutput (ParsingContext & context)> parse_;
};

} // namespace MLDB
