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

template<typename MatchAtomFn, typename RecurseFn>
std::optional<Value>
match_recursive(Context & lcontext, ParseContext & pcontext, MatchAtomFn&& matchAtom, RecurseFn&&recurse)
{
    ParseContext::Revert_Token token(pcontext);
    Value result;
    
    pcontext.skip_whitespace();
    if (pcontext.match_literal('(')) {
        pcontext.match_whitespace();
        std::optional<Value> arg;
        List list;
        while ((arg = recurse(lcontext, pcontext))) {
            list.emplace_back(std::move(*arg));
            if (list.size() == 1 && list.back().is<Variable>()) {
                list.back() = { lcontext, Function{list.back().as<Variable>().var, nullptr} };
            }
            if (!pcontext.match_whitespace())
                break;
        }
        result = Value(lcontext, std::move(list));
        pcontext.skip_whitespace();
        pcontext.expect_literal(')', "expected ')' to close list");
    }
    else if (auto atom = matchAtom(lcontext, pcontext)) {
        result = std::move(*atom);
    }
    else {
        return std::nullopt;
    }

    token.ignore();
    return std::move(result);
}

template<typename ParseAtomFn, typename RecurseFn>
Value parse_recursive(Context & lcontext, ParseContext & pcontext, ParseAtomFn&&parseAtom, RecurseFn&&recurse)
{
    pcontext.skip_whitespace();
    if (pcontext.match_literal('(')) {
        pcontext.match_whitespace();
        List list;
        bool first = true;
        while (!pcontext.match_literal(')')) {
            if (first) pcontext.match_whitespace();
            else       pcontext.expect_whitespace();

            if (pcontext.match_literal(')'))
                break;

            list.emplace_back(recurse(lcontext, pcontext));
            if (first && list[0].is<Variable>()) {
                list.back() = { lcontext, Function{list[0].as<Variable>().var, nullptr} };
            }
            first = false;
        }
        return Value(lcontext, std::move(list));
    }
    else {
        return parseAtom(lcontext, pcontext);
    }
}

} // namespace Lisp
} // namespace MLDB
