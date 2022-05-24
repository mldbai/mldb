/* lisp_parsing.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_value.h"
#include <optional>
#include "mldb/base/parse_context.h"


#pragma once

namespace MLDB {
namespace Lisp {

std::optional<PathElement> match_rule_name(ParseContext & context);

// Current character is included if include is true
std::optional<PathElement> match_rest_of_name(ParseContext & context, bool includeFirst, ParseContext::Revert_Token * token = nullptr);
std::optional<PathElement> match_variable_name(ParseContext & context);
std::optional<PathElement> match_symbol_name(ParseContext & context);
std::optional<PathElement> match_operator_name(ParseContext & context);
std::optional<Utf8String> match_delimited_string(ParseContext & context, char delim);

// Skip over comments, whitespace, eof... returns true if something was matched
bool skipLispWhitespace(ParseContext & context);

// Expect some kind of whitespace, throws if not found
void expectLispWhitespace(ParseContext & context);

template<typename Matcher, typename Error, typename... Args>
auto parse_from_matcher(Matcher && matcher,
                        Error && error,
                        Args&&... args)
{
    auto matchRes = matcher(std::forward<Args>(args)...);
    if (!matchRes) {
        error();
        // Error should throw, so we won't reach here if matchRes is null
    }
    return std::move(*matchRes);
}

template<typename MatchAtomFn, typename MatchMetadataFn, typename RecurseFn>
std::optional<Value>
match_recursive(Context & lcontext, ParseContext & pcontext,
                MatchAtomFn&& matchAtom, MatchMetadataFn&& matchMetadata, RecurseFn&& recurse)
{
    ParseContext::Revert_Token token(pcontext);
    Value result;
    
    skipLispWhitespace(pcontext);

    auto loc = getSourceLocation(pcontext);

    int quotes = 0;
    while (pcontext.match_literal('\''))
        ++quotes;
    
    if (pcontext.match_literal('(')) {
        skipLispWhitespace(pcontext);
        std::optional<Value> arg;
        ListBuilder list;
        while ((arg = recurse(lcontext, pcontext))) {
            list.emplace_back(std::move(*arg));
            if (!skipLispWhitespace(pcontext))
                break;
        }
        result = Value(lcontext, std::move(list));
        skipLispWhitespace(pcontext);
        pcontext.expect_literal(')', "expected ')' to close list");
    }
    else if (auto atom = matchAtom(lcontext, pcontext)) {
        result = std::move(*atom);
    }
    else {
        return std::nullopt;
    }

    result.setQuotes(quotes);

    if (matchMetadata && pcontext.match_literal(':')) {
        // metadata
        std::optional<Value> mdo = matchMetadata(lcontext, pcontext);
        if (!mdo || !mdo->is<List>())
            pcontext.exception("expected list for metadata after a ':'");
        //cerr << "parsing metadata " << *mdo << endl;
        const List & mdl = mdo->as<List>();
        if (mdl.size() % 2 != 0) {
            pcontext.exception("metadata list must have an even number of elements (type then value)");
        }
        for (size_t i = 0;  i < mdl.size();  i += 2) {
            const Value & key = mdl[i + 0];
            const Value & val = mdl[i + 1];
            MetadataType tp = valueToMetadataType(key);
            result.addMetadata(tp, val);
        }
    }

    if (!result.hasMetadata(MetadataType::SOURCE_LOCATION))
        addSourceLocation(result, loc);

    token.ignore();
    return std::move(result);
}

template<typename ParseAtomFn, typename ParseMetadataFn, typename RecurseFn>
Value parse_recursive(Context & lcontext, ParseContext & pcontext,
                      ParseAtomFn&&parseAtom, ParseMetadataFn&& parseMetadata, RecurseFn&&recurse)
{
    Value result;
    skipLispWhitespace(pcontext);

    auto loc = getSourceLocation(pcontext);

    int quotes = 0;
    while (pcontext.match_literal('\''))
        ++quotes;

    if (pcontext.match_literal('(')) {
        skipLispWhitespace(pcontext);
        ListBuilder list;
        bool first = true;
        while (!pcontext.match_literal(')')) {
            if (first) skipLispWhitespace(pcontext);
            else       expectLispWhitespace(pcontext);

            if (pcontext.match_literal(')'))
                break;

            list.emplace_back(recurse(lcontext, pcontext));
            first = false;
        }
        result = Value(lcontext, std::move(list));
    }
    else {
        result = parseAtom(lcontext, pcontext);
    }

    result.setQuotes(quotes);

    if (parseMetadata && pcontext.match_literal(':')) {
        // metadata
        Value md = parseMetadata(lcontext, pcontext);
        if (!md.is<List>())
            pcontext.exception("expected list for metadata after a ':'");
        const List & mdl = md.as<List>();
        if (mdl.size() % 2 != 0) {
            pcontext.exception("metadata list must have an even number of elements (type then value)");
        }
        for (size_t i = 0;  i < mdl.size();  i += 2) {
            const Value & key = mdl[i + 0];
            const Value & val = mdl[1 + 1];
            MetadataType tp = valueToMetadataType(key);
            result.addMetadata(tp, val);
        }
    }

    if (!result.hasMetadata(MetadataType::SOURCE_LOCATION))
        addSourceLocation(result, loc);

    return result;
}

} // namespace Lisp
} // namespace MLDB
