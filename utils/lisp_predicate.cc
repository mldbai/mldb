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

namespace {

static inline bool isEllipsis(const Value & v)
{
    if (v.is<Ellipsis>()) return true;
    if (!v.is<List>()) return false;
    auto & l = v.as<List>();
    return !l.empty() && l[0].is<Ellipsis>();
}

static bool isVariable(const PathElement & el)
{
    return el.startsWith("$");
}

static bool isVariable(const Symbol & sym)
{
    return isVariable(sym.sym);
}

static bool isVariable(const Value & v)
{
    if (!v.is<Symbol>())
        return false;
    if (v.isQuoted())
        return false;
    return isVariable(v.as<Symbol>());
}

} // file scope

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
    auto m = Lisp::match_recursive(lcontext, pcontext, matchPredicateAtom, Value::match, matchPredicateRecurse);
    if (!m)
        return m;
    Value result = std::move(*m);

    if (pcontext.match_literal("...")) {
        // ellipsis as a suffix means repeated
        result = lcontext.list(Ellipsis{}, std::move(result));
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
parse(Context & lcontext, const Utf8String & pred, const SourceLocation & loc)
{
    ParseContext pcontext(loc.file().c_str(), pred.rawData(), pred.rawLength(), loc.line(), loc.column());
    auto res = parse(lcontext, pcontext);
    skipLispWhitespace(pcontext);
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

    ListBuilder list;

    list.reserve(2 * vars.size());
    for (auto & [k,v]: vars) {
        list.emplace_back(Value{context, Symbol{k}});
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

void mergeUndos(UndoList & allUndos, UndoList & undos)
{
    allUndos.insert(allUndos.end(),
                    make_move_iterator(undos.begin()), make_move_iterator(undos.end()));
}

Value inferType(const Value & input)
{
    if (input.hasMetadata(MetadataType::TYPE_INFO)) {
        return input.getMetadata(MetadataType::TYPE_INFO);
    }

    auto make = [&] (const char * tp) -> Value
    {
        return { input.getContext(), Symbol { tp } };
    };

    LambdaVisitor visitor {
        [&] (const Value & val) -> Value  // default case
        { 
            MLDB_THROW_UNIMPLEMENTED(("don't know how to test metadata " + val.print()).c_str());
        },
        [&] (const Symbol & sym) { return make("any"); },
        [&] (int64_t i)  { return make("i64"); },
        [&] (uint64_t i) { return make("u64"); },
        [&] (double d)   { return make("f64"); },
        [&] (bool b)     { return make("bool"); },
        [&] (Null)       { return make("nil"); },
        [&] (Utf8String) { return make("str"); },
        [&] (const List & list) { return make("list"); },
    };
    
    return visit(visitor, input);
}

bool matchesMd(const Value & input, const Value & source)
{
    if (!source.hasMetadata(MetadataType::TYPE_INFO))
        return true;

    //const Value & md = source.getMetadata();
    
    auto tp = inferType(input);

    //cerr << "testing if inferred " << tp << " matches md " << md << endl;

    LambdaVisitor visitor {
        [&] (const Value & val) -> bool  // default case
        { 
            MLDB_THROW_UNIMPLEMENTED(("don't know how to test metadata " + val.print()).c_str());
        },
        [&] (const Symbol & sym) -> bool
        {
            return tp.as<Symbol>().sym == sym.sym;
        },
    };

    return visit(visitor, source.getMetadata(MetadataType::TYPE_INFO));
}

// Called when we have an ellipsis which matches zero values.  We need to put empty
// matches into the ununified variables.
bool unifyEllipsisNoMatches(Context & lcontext,
                            UnifiedValues & vars, UndoList & undos, const Value & source,
                            int ellipsisLevel)
{
    //cerr << "unifyEllipsisNoMatches " << source << " level " << ellipsisLevel << endl;

    if (source.isQuoted())
        return true;

    LambdaVisitor visitor {
        [&] (const Value & val) -> bool
        { 
            //cerr << "ellipsis unify: default case " << val << endl;
            return true;
        },  // Default: return true
        [&] (const Value & val, const List & list) -> bool
        {
            //cerr << "ellipsis unify: list " << val << endl;
            // List: unify elements
            bool ell = isEllipsis(val);
            UndoList myUndos;
            for (const auto & child: list) {
                if (!unifyEllipsisNoMatches(lcontext, vars, myUndos, child, ellipsisLevel + ell)) {
                    applyUndoList(vars, myUndos);
                    return false;
                }
            }
            mergeUndos(undos, myUndos);
            return true;
        },
        [&] (const Value & val, const Symbol & sym) -> bool
        {
            //cerr << "ellipsis unify: sym " << val << endl;
            if (!isVariable(source))
                return true;
            auto var = sym.sym;
            auto it = vars.find(var);
            if (it != vars.end()) {
                cerr << "var value is " << it->second << endl;
                MLDB_THROW_UNIMPLEMENTED("found var");
            }
            else {
                vars[var] = lcontext.list();
            }
            return true;
        }
    };            

    return visit(visitor, source);
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
        mergeUndos(allUndos, undos);
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

            auto res = matchImpl(vars, valueList[nInput], sourceList[nSrc], appendVars || inEllipsis);

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
                UndoList undos;
                if (unifyEllipsisNoMatches(input.getContext(), vars, undos, sourceList.back(), 0 /* ellipsis count */))
                    addUndos(undos);
#if 0
                auto & ell = sourceList.back().as<List>();
                for (size_t i = 1;  i < ell.size();  ++i) {
                    auto undos = matchImpl(vars, context.list(), ell[i], false /* append */);
                    if (!undos)
                    addUndos(*undos);
                }
#endif
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
    else if (isVariable(source)) {
        auto var = source.as<Symbol>().sym;

        //cerr << "unify variable: var = " << var << " input = " << input << endl;
        auto it = vars.find(var);
        if (it == vars.end()) {
            UnifiedValues::iterator it;
            if (appendVars) {
                it = vars.emplace(var, context.list(input)).first;
            }
            else {
                it = vars.emplace(var, input).first;
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
                    ListBuilder l;
                    l.insert(l.end(), it->second.as<List>().begin(), it->second.as<List>().end());
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

    if (pcontext.match_literal("...")) {
        // ellipsis as a suffix means repeated
        result = lcontext.list(Ellipsis{}, std::move(result));
    }

    return std::move(result);
}

optional<Value>
matchSubstitutionRecurse(Context & lcontext, ParseContext & pcontext)
{
    return Lisp::match_recursive(lcontext, pcontext,
                                 matchSubstitutionAtom, Value::match, matchSubstitutionRecurse);
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
parse(Context & lcontext, const Utf8String & pred, const SourceLocation & loc)
{
    ParseContext pcontext(loc.file().c_str(), pred.rawData(), pred.rawLength(), loc.line(), loc.column());
    auto res = parse(lcontext, pcontext);
    skipLispWhitespace(pcontext);
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

    //cerr << "substituting matched values " << matched << endl;

    UnifiedValues vals;
    for (size_t i = 1;  i < list.size();  i += 2) {
        auto var = list.at(i).getSymbolName();
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
        [&] (const Value & val, const Symbol & sym) -> Value
        {
            //cerr << "substituting symbol " << val << endl;
            if (isVariable(val)) {
                auto it = vals.find(sym.sym);
                if (it == vals.end()) {
                    throw MLDB::Exception("Substituting unset variable " + sym.sym.toUtf8String());
                }
                return it->second;
            }
            else return val.unquoted();
        },
        [&] (const Value & val, const List & list) -> Value  // expand ellipsis
        {
            //cerr << "substituting list" << endl;
            if (val.isQuoted())
                return val.unquoted();
            
            //cerr << "handling " << Value{context, list}.print() << endl;
            ListBuilder listOut;
            for (const auto & val: list) {
                if (val.is<List>() && isEllipsis(val)) {
                    auto & l = val.as<List>();
                    for (size_t i = 1;  i < l.size();  ++i) {
                        auto li = l[i].as<List>().steal();
                        //cerr << "i = " << i << " li = " << l[i] << endl;
                        listOut.insert(listOut.end(),
                                       make_move_iterator(li.begin()),
                                       make_move_iterator(li.end()));
                    }
                }
                else {
                    listOut.emplace_back(val);
                }
            }
            
            //cerr << "  returning " << Value{context, result}.print() << endl;

            Value result { context, std::move(listOut) };
            if (source.hasMetadata(MetadataType::TYPE_INFO))
                result.addMetadata(MetadataType::TYPE_INFO, source.getMetadata(MetadataType::TYPE_INFO));
            return result;
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
    skipLispWhitespace(pcontext);
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
    skipLispWhitespace(pcontext);
    pcontext.expect_literal("->");
    result.subst = Substitution::parse(lcontext, pcontext);
    return result;
}

Pattern
Pattern::
parse(Context & lcontext, const Utf8String & pred, const SourceLocation & loc)
{
    ParseContext pcontext(loc.file().c_str(), pred.rawData(), pred.rawLength(), loc.line(), loc.column());
    auto res = parse(lcontext, pcontext);
    skipLispWhitespace(pcontext);
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

Value
recursePatterns(const std::vector<Pattern> & patterns,
                const Value & input)
{
    auto applyPatterns = [&] (const Value & input) -> Value
    {
        Value current = input;

        for (bool matched = true; matched; matched = false) {
            for (auto & p: patterns) {
                auto res = p.apply(current);
                if (res) {
                    //cerr << "matched: " << current << " : " << p.toLisp() << " -> " << *res << endl;
                    current = *res;
                    matched = true;
                    break;
                }
            }
        }
 
        return current;
    };

    RecursiveLambdaVisitor visitor { applyPatterns };

    return recurse(visitor, applyPatterns(input));
}

} // namespace Lisp
} // namespace MLDB
