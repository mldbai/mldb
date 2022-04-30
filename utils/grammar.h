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
#include "lisp.h"
#include "lisp_predicate.h"


namespace MLDB {
namespace Grammar {

struct Parser;
struct CompilationState;
struct ParsingContext;
struct CompilationContext;
struct ExecutionContext;
using Lisp::Value;

using ParserOutput = std::optional<Value>;



struct CompilationContext: public Lisp::CompilationScope {
    CompilationContext(Lisp::Context & lcontext);
    CompilationContext(CompilationContext & parent);

    void exception(const Utf8String & reason) const MLDB_NORETURN;
    void setCompiledRule(PathElement ruleName, Parser parser);
    std::function<Parser ()> getCompiledRule(PathElement ruleName) const;
    std::function<void (ParsingContext & context, Value val)> getVariableSetter(PathElement name);
    std::function<Value (const ParsingContext & context)> getVariableReference(PathElement name) const;

    std::shared_ptr<CompilationContext> enter(PathElement where);

private:
    std::shared_ptr<CompilationState> state;
};

struct ExecutionContext {

};


struct GrammarProduction {
    Value match;
    Value produce;
    Parser compile(std::shared_ptr<CompilationContext> context) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GrammarProduction);

struct Parser;

struct GrammarRule {
    PathElement name;
    std::map<PathElement, GrammarRule> rules;
    std::vector<GrammarProduction> productions;
    Parser compile(std::shared_ptr<CompilationContext> context) const;
};

DECLARE_STRUCTURE_DESCRIPTION(GrammarRule);

GrammarRule parseGrammar(Lisp::Context & lcontext, ParseContext & context);

struct ParsingContext: Lisp::ExecutionScope {
    ParsingContext(Lisp::Context & lcontext, ParseContext & context);
    ParsingContext(const ParsingContext & parent);

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
    ParserOutput parse(Lisp::Context & lcontext, ParseContext & pcontext) const;
    ParserOutput parse(Lisp::Context & lcontext, const std::string & toParse) const;

    inline ParserOutput parse(ParsingContext & context) const
    {
        return parse_(context);
    }
    std::function<ParserOutput (ParsingContext & context)> parse_;
};

} // namespace Grammar
} // namespace MLDB
