/* lisp_predicate.h                                               -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_value.h"
#include <map>

namespace MLDB {
namespace Lisp {

typedef std::map<PathElement, Value> UnifiedValues;
typedef std::vector<std::tuple<UnifiedValues::iterator, std::optional<Value>>> UndoList;


/*******************************************************************************/
/* LISP PREDICATE                                                              */
/*******************************************************************************/

/** This is a predicate that attempts to match an input expression, and captures
    information from that input expression to be later used in a substitution.
*/

struct Predicate {
    static Predicate parse(Context & lcontext, ParseContext & context);
    static Predicate parse(Context & lcontext, const Utf8String & pred);
    Value match(const Value & input) const;

    Value toLisp() const;

private:
    Value source;
    static std::optional<UndoList>
    matchImpl(UnifiedValues & vars, const Value & input, const Value & source);
};


/*******************************************************************************/
/* LISP SUBSTITUTION                                                           */
/*******************************************************************************/

/** This is a lisp expression that synthesizes an output from information
    captured by a matched predicate.
*/

struct Substitution {
    static Substitution parse(Context & lcontext, ParseContext & context);
    static Substitution parse(Context & lcontext, const Utf8String & subst);
    Value subst(Value matched) const;
    Value toLisp() const;

private:
    Value source;
    static Value substImpl(Context & context, const Value & source, const UnifiedValues & vals);
};

} // namespace Lisp
} // namespace MLDB
