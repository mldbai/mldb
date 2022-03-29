/* lisp_predicate.cc                                              -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_predicate.h"
#include "lisp.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/any_impl.h"
#include "lisp_visitor.h"

using namespace std;

namespace MLDB {
namespace Lisp {


/*******************************************************************************/
/* LISP PREDICATE                                                              */
/*******************************************************************************/

Predicate
Predicate::
parse(Context & lcontext, ParseContext & pcontext)
{
    Predicate result;
    result.source = Value::parse(lcontext, pcontext);
    return result;
}

Predicate
Predicate::
parse(Context & lcontext, const Utf8String & pred)
{
    ParseContext pcontext("<<<internal string>>>", pred.rawData(), pred.rawLength());
    return parse(lcontext, pcontext);
}

Value
Predicate::
toLisp() const
{
    return source;
}

Value
Predicate::
match(const Value & input) const
{
    Context & context = input.getContext();

    std::map<PathElement, Value> vars;

    if (!matchImpl(vars, input, source)) {
        return context.null();
    }

    List list;

    list.reserve(2 * vars.size());
    for (auto & [k,v]: vars) {
        list.emplace_back(Value{context, Variable{k}});
        list.emplace_back(std::move(v));
    }
    return context.list("matched", list);
}

void applyUndoList(UnifiedValues & vars, UndoList & undos)
{
    for (auto & [it,v]: undos) {
        if (v) {
            it->second = std::move(*v);
        }
        else {
            vars.erase(it);
        }
    }
}

std::optional<UndoList>
Predicate::
matchImpl(UnifiedValues & vars,
          const Value & input, const Value & source)
{
    //Context & context = input.getContext();

    UndoList allUndos;
    auto match = [&] () -> std::optional<UndoList>
    {
        return std::move(allUndos);
    };

    auto nomatch = [&] () -> std::optional<UndoList>
    {
        applyUndoList(vars, allUndos);
        return nullopt;
    };

    if (source.is<List>()) {
        if (!input.is<List>()) {
            return nomatch();            
        }
        const List & sourceList = source.as<List>();
        const List & valueList = input.as<List>();

        if (sourceList.size() != valueList.size()) {
            // TODO: we could have multiple matches...
            return nomatch();
        }

        std::function<bool (size_t)> unify = [&] (size_t n) -> bool
        {
            if (n == sourceList.size())
                return true;

            //cerr << "n = " << n << ": unifying " << sourceList[n] << " with " << valueList[n] << endl;

            auto res = matchImpl(vars, valueList[n], sourceList[n]);

            //cerr << " res = " << (bool)res << endl;

            if (!res)
                return false;
            if (!unify(n + 1)) {
                applyUndoList(vars, *res);
                return false;
            }
            else {
                allUndos.insert(allUndos.end(),
                                make_move_iterator(res->begin()), make_move_iterator(res->end()));
                return true;
            }
        };

        if (unify(0)) {
            return std::move(allUndos);
        }
        else {
            return nullopt;
        }
    }
    else if (source.is<Wildcard>()) {
        return match();
    }
    else if (source.is<Variable>()) {
        auto & var = source.as<Variable>();

        //cerr << "unify variable: var = " << var.var << " input = " << input << endl;
        auto it = vars.find(var.var);
        if (it == vars.end()) {
            auto it = vars.emplace(var.var, input).first;
            allUndos.emplace_back(std::move(it), std::nullopt);
            return match();
        }
        else {
            // Unify variable
            if (input == it->second) {
                return match();
            }
            else {
                return nomatch();
            }
        }
    }
    else {
        //cerr << "fallthrough; type = " << jsonEncodeStr(source) << endl;
        if (input == source) {
            return match();
        }
        else {
            return nomatch();
        }
    }
}


/*******************************************************************************/
/* LISP SUBSTITUTION                                                           */
/*******************************************************************************/

Substitution
Substitution::
parse(Context & lcontext, ParseContext & pcontext)
{
    Substitution result;
    result.source = Value::parse(lcontext, pcontext);
    return result;
}

Substitution
Substitution::
parse(Context & lcontext, const Utf8String & pred)
{
    ParseContext pcontext("<<<internal string>>>", pred.rawData(), pred.rawLength());
    return parse(lcontext, pcontext);
}

Value
Substitution::
toLisp() const
{
    return source;
}

Value
Substitution::
subst(Value matched) const
{
    Context & context = matched.getContext();

    const List & list = matched.as<List>();
    if (list.functionName() != PathElement{"matched"}) {
        throw MLDB::Exception("Error applying substitution: not a match: " + matched.print());
    }

    UnifiedValues vals;
    for (size_t i = 1;  i < list.size();  i += 2) {
        auto var = list.at(i).getVariableName();
        auto & val = list.at(i + 1);
        vals.emplace(std::move(var), std::move(val));
    }

    return substImpl(context, source, vals);
}

Value
Substitution::
substImpl(Context & context, const Value & source, const UnifiedValues & vals)
{
    RecursiveLambdaVisitor visitor {
        [&] (const Variable & var)
        {
            auto it = vals.find(var.var);
            if (it == vals.end()) {
                throw MLDB::Exception("Substituting unset variable " + var.var.toUtf8String());
            }
            return it->second;
        }
    };

    return recurse(visitor, source);
}


} // namespace Lisp
} // namespace MLDB
