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
#include "lisp_parsing.h"

using namespace std;

namespace MLDB {
namespace Lisp {


/*******************************************************************************/
/* LISP PREDICATE                                                              */
/*******************************************************************************/

optional<Value>
matchPredicateAtom(Context & lcontext, ParseContext & pcontext)
{
    Value result;
    auto valAtom = Value::matchAtom(lcontext, pcontext);
    if (valAtom)
        result = std::move(*valAtom);
    else return nullopt;
    return std::move(result);
}

optional<Value>
matchPredicateRecurse(Context & lcontext, ParseContext & pcontext)
{
    auto m = Lisp::match_recursive(lcontext, pcontext, matchPredicateAtom, matchPredicateRecurse);
    if (!m)
        return m;
    Value result = std::move(*m);

    if (pcontext.match_literal(':')) {
        auto md = Value::parse(lcontext, pcontext);
        if (md.hasMetadata())
            pcontext.exception("Metadata can't have metadata");
        result.addMetadata(std::move(md));
    }

    if (pcontext.match_literal("...")) {
        // ellipsis as a suffix means repeated
        result = lcontext.make_list(Ellipsis{}, std::move(result));
    }

    return result;
}

optional<Predicate>
Predicate::
match(Context & lcontext, ParseContext & pcontext)
{
    Predicate result;
    if (auto val = matchPredicateRecurse(lcontext, pcontext)) {
        result.source = *val;
        return result;
    }
    return nullopt;
}

Predicate
Predicate::
parse(Context & lcontext, ParseContext & pcontext)
{
    return parse_from_matcher([] (Context & lc, ParseContext & pc) { return match(lc, pc); },
                              [&] () { pcontext.exception("expected predicate"); },
                              lcontext, pcontext);
}

Predicate
Predicate::
parse(Context & lcontext, const Utf8String & pred)
{
    ParseContext pcontext("<<<internal string>>>", pred.rawData(), pred.rawLength());
    auto res = parse(lcontext, pcontext);
    pcontext.skip_whitespace();
    pcontext.expect_eof();
    return res;
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

    if (!matchImpl(vars, input, source, false /* append */)) {
        return context.null();
    }

    List list;

    list.reserve(2 * vars.size());
    for (auto & [k,v]: vars) {
        list.emplace_back(Value{context, Variable{k}});
        list.emplace_back(std::move(v));
    }
    return context.call("matched", list);
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

static inline bool isEllipsis(const Value & v)
{
    if (v.is<Ellipsis>()) return true;
    if (!v.is<List>()) return false;
    auto & l = v.as<List>();
    return !l.empty() && l[0].is<Ellipsis>();
}

bool matchesMd(Value input, Value source)
{
    if (!source.hasMetadata())
        return true;

    const Value & md = source.getMetadata();

    cerr << "testing if " << input << " matches md " << jsonEncodeStr(md) << endl;

    return true;
}

std::optional<UndoList>
Predicate::
matchImpl(UnifiedValues & vars,
          const Value & input, const Value & source,
          bool appendVars)
{
    Context & context = input.getContext();

    if (!matchesMd(input, source))
        return nullopt;

    UndoList allUndos;

    auto addUndos = [&] (UndoList & undos)
    {
        allUndos.insert(allUndos.end(),
                        make_move_iterator(undos.begin()), make_move_iterator(undos.end()));
    };

    auto match = [&] () -> std::optional<UndoList>
    {
        return std::move(allUndos);
    };

    auto nomatch = [&] () -> std::optional<UndoList>
    {
        applyUndoList(vars, allUndos);
        return nullopt;
    };

    if (isEllipsis(source)) {
        // Acts as a wildcard, but adds captures to a list
        const List & sourceList = source.as<List>();
        for (size_t i = 1;  i < sourceList.size();  ++i) {
            auto undos = matchImpl(vars, input, sourceList[i], true /* appendVars */);
            if (!undos)
                return nomatch();
            addUndos(*undos);
        }
        return match();
    }
    else if (source.is<List>()) {
        if (!input.is<List>()) {
            return nomatch();            
        }
        const List & sourceList = source.as<List>();
        const List & valueList = input.as<List>();

        bool hasEllipsis = false;
        if (!sourceList.empty()) {
            hasEllipsis = isEllipsis(sourceList.back());
        }

        //cerr << "hasEllipsis = " << hasEllipsis << endl;

        if (hasEllipsis) {
            if (sourceList.size() - 1 > valueList.size()) {
                // Not enough even if ellipsis matches nothing
                return nomatch();
            }
        }
        else {
            if (sourceList.size() != valueList.size()) {
                return nomatch();
            }
        }

        bool doneEllipsis = false;

        std::function<bool (size_t, size_t)> unify = [&] (size_t nSrc, size_t nInput) -> bool
        {
            if (nInput == valueList.size())
                return true;

            //cerr << "nSrc = " << nSrc << " nInput = " << nInput << ": unifying " << sourceList[nSrc] << " with " << valueList[nInput] << endl;

            bool inEllipsis = nSrc == sourceList.size() - 1 && hasEllipsis;

            auto res = matchImpl(vars, valueList[nInput], sourceList[nSrc], inEllipsis);

            //cerr << " res = " << (bool)res << endl;

            if (!res)
                return false;

            bool success = false;
            if (inEllipsis) {
                success = unify(nSrc, nInput + 1);
                doneEllipsis = true;
            }
            else {
                success = unify(nSrc + 1, nInput + 1);
            }
            if (!success) {
                applyUndoList(vars, *res);
                return false;
            }
            else {
                addUndos(*res);
                return true;
            }
        };

        if (unify(0, 0)) {
            if (hasEllipsis && !doneEllipsis && sourceList.back().is<List>()) {
                auto & ell = sourceList.back().as<List>();
                for (size_t i = 1;  i < ell.size();  ++i) {
                    auto undos = matchImpl(vars, context.make_list(), ell[i], false /* append */);
                    if (!undos)
                        MLDB_THROW_LOGIC_ERROR("unify with empty list shouldn't fail (I think)...");
                    addUndos(*undos);
                }
            }
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
            UnifiedValues::iterator it;
            if (appendVars) {
                it = vars.emplace(var.var, context.make_list(input)).first;
            }
            else {
                it = vars.emplace(var.var, input).first;
            }
            allUndos.emplace_back(std::move(it), std::nullopt);
            return match();
        }
        else {
            // Unify variable
            if (appendVars) {
                if (!it->second.is<List>())
                    return nomatch();
                else {
                    // TODO mutate in place
                    List l = it->second.as<List>();
                    l.emplace_back(input);
                    it->second = { context, std::move(l) };
                    return match();
                }
            }
            else {
                if (input == it->second) {
                    return match();
                }
                else {
                    return nomatch();
                }
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

optional<Value>
matchSubstitutionAtom(Context & lcontext, ParseContext & pcontext)
{
    Value result;
    auto valAtom = Value::matchAtom(lcontext, pcontext);
    if (valAtom)
        result = std::move(*valAtom);
    else return nullopt;

    if (pcontext.match_literal(':')) {
        // metadata
        auto mdo = Value::match(lcontext, pcontext);
        if (!mdo)
            pcontext.exception("expected metadata after a ':'");
        else if (mdo->hasMetadata())
            pcontext.exception("metadata can't have metadata");
        else {
            result.addMetadata(std::move(*mdo));
        }
    }
    if (pcontext.match_literal("...")) {
        // ellipsis as a suffix means repeated
        result = lcontext.make_list(Ellipsis{}, std::move(result));
    }

    return std::move(result);
}

optional<Value>
matchSubstitutionRecurse(Context & lcontext, ParseContext & pcontext)
{
    return Lisp::match_recursive(lcontext, pcontext, matchSubstitutionAtom, matchSubstitutionRecurse);
}

optional<Substitution>
Substitution::
match(Context & lcontext, ParseContext & pcontext)
{
    Substitution result;
    if (auto val = matchSubstitutionRecurse(lcontext, pcontext)) {
        result.source = *val;
        return result;
    }
    return nullopt;
}

Substitution
Substitution::
parse(Context & lcontext, ParseContext & pcontext)
{
    return parse_from_matcher([] (Context & lc, ParseContext & pc) { return match(lc, pc); },
                              [&] () { pcontext.exception("expected substitution"); },
                              lcontext, pcontext);
}

Substitution
Substitution::
parse(Context & lcontext, const Utf8String & pred)
{
    ParseContext pcontext("<<<internal string>>>", pred.rawData(), pred.rawLength());
    auto res = parse(lcontext, pcontext);
    pcontext.skip_whitespace();
    pcontext.expect_eof();
    return res;
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
        },
        [&] (const List & list) -> Value  // expand ellipsis
        {
            //cerr << "handling " << Value{context, list}.print() << endl;
            List result;
            for (auto & val: list) {
                if (val.is<List>() && isEllipsis(val)) {
                    auto & l = val.as<List>();
                    for (size_t i = 1;  i < l.size();  ++i) {
                        auto & li = l[i].as<List>();
                        //cerr << "i = " << i << " li = " << l[i] << endl;
                        result.insert(result.end(),
                                      make_move_iterator(li.begin()),
                                      make_move_iterator(li.end()));
                    }
                }
                else {
                    result.emplace_back(val);
                }
            }
            
            //cerr << "  returning " << Value{context, result}.print() << endl;

            return { context, std::move(result) };
        }
    };

    return recurse(visitor, source);
}


/*******************************************************************************/
/* LISP PATTERN                                                                */
/*******************************************************************************/

optional<Pattern>
Pattern::
match(Context & lcontext, ParseContext & pcontext)
{
    Pattern result;
    ParseContext::Revert_Token token(pcontext);

    if (auto val = Predicate::match(lcontext, pcontext))
        result.pred = std::move(*val);
    else return nullopt;
    pcontext.skip_whitespace();
    pcontext.match_literal("->");

    if (auto val = Substitution::match(lcontext, pcontext))
        result.subst = std::move(*val);
    else return nullopt;

    token.ignore();
    return result;
}

Pattern
Pattern::
parse(Context & lcontext, ParseContext & pcontext)
{
    Pattern result;
    result.pred = Predicate::parse(lcontext, pcontext);
    pcontext.skip_whitespace();
    pcontext.expect_literal("->");
    result.subst = Substitution::parse(lcontext, pcontext);
    return result;
}

Pattern
Pattern::
parse(Context & lcontext, const Utf8String & pred)
{
    ParseContext pcontext("<<<internal string>>>", pred.rawData(), pred.rawLength());
    auto res = parse(lcontext, pcontext);
    pcontext.skip_whitespace();
    pcontext.expect_eof();
    return res;
}

Value
Pattern::
toLisp() const
{
    return getContext().call("pattern", { pred.toLisp(), subst.toLisp() });
}

static bool didMatch(const Value & val)
{
    return val.is<List>() && !val.as<List>().empty();
}

std::optional<Value>
Pattern::
apply(Value input) const
{
    auto matches = pred.match(input);
    if (!didMatch(matches))
        return nullopt;
    auto output  = subst.subst(matches);
    return std::move(output);
}

} // namespace Lisp
} // namespace MLDB
