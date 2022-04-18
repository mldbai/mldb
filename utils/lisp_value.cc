/* lisp.cc                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp.h"
#include "lisp_lib.h"
#include "lisp_visitor.h"
#include "lisp_parsing.h"
#include "mldb/types/json_parsing.h"
#include "mldb/types/json_visitor.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"

using namespace std;

namespace MLDB {
namespace Lisp {

/*******************************************************************************/
/* LISP VALUE                                                                  */
/*******************************************************************************/

Path
List::
functionName() const
{
    if (empty())
        throw MLDB::Exception("nil value is not a function");
    if (!front().is<Symbol>())
        throw MLDB::Exception("head of list is not a symbol: " + front().print());
    return front().as<Symbol>().sym;
}

PathElement
List::
simpleFunctionName() const
{
    return functionName().toSimpleName();
}

Function::
Function(PathElement name, CompiledExpression expr)
    : Function(std::move(name), std::make_shared<CompiledExpression>(std::move(expr)))
{
}

Function::
Function(PathElement name, std::shared_ptr<const CompiledExpression> compiled)
    : Symbol({name}), compiled(std::move(compiled))
{
}

Value::
Value()
{
}

Value::
Value(const Value & other)
    : context_(other.context_), quotes_(other.quotes_), value_(other.value_), md_(other.md_)
{
}

Value::
Value(Value && other)
    : context_(other.context_), quotes_(other.quotes_), value_(std::move(other.value_)), md_(std::move(other.md_))
{
    other.context_ = nullptr;
}

Value &
Value::
operator = (const Value & other)
{
    Value oldVal = std::move(*this);
    context_ = other.context_;
    value_ = other.value_;
    md_ = other.md_;
    quotes_ = other.quotes_;
    return *this;
}

Value &
Value::
operator = (Value && other)
{
    Value oldVal = std::move(*this);
    context_ = other.context_;
    value_ = std::move(other.value_);
    md_ = std::move(other.md_);
    quotes_ = other.quotes_;
    other.context_ = nullptr;
    return *this;
}

Value::
~Value()
{
    // Here is where we tell the context it's garbage
}

Value::
Value(Context & context, const Utf8String & str)
    : context_(&context), value_(str)
{
}

Value::
Value(Context & context, Utf8String&& str)
    : context_(&context), value_(std::move(str))
{
}

Value::
Value(Context & context, std::string&& str)
    : context_(&context), value_(Utf8String(std::move(str)))
{
}

Value::
Value(Context & context, int64_t i)
    : context_(&context), value_(i)
{
}

Value::
Value(Context & context, uint64_t i)
    : context_(&context), value_(i)
{
}

Value::
Value(Context & context, double d)
    : context_(&context), value_(d)
{
}

Value::
Value(Context & context, bool b)
    : context_(&context), value_(b)
{
}

Value::
Value(Context & context, Symbol sym)
    : context_(&context), value_(std::move(sym))
{
}

Value::
Value(Context & context, Wildcard)
    : context_(&context), value_(Wildcard{})
{
}

Value::
Value(Context & context, Ellipsis)
    : context_(&context), value_(Ellipsis{})
{
}

Value::
Value(Context & context, List list)
    : context_(&context), value_(std::move(list))
{
}

Value::
Value(Context & context, Null)
    : context_(&context), value_(Null{})
{
}

Value::
Value(Context & context, Function fn)
    : context_(&context), value_(std::move(fn))
{
}

#if 0
Value::
Value(Context & context, Type t)
    : context_(&context), value_(std::move(t))
{
}
#endif

bool
Value::
operator == (const Value & other) const
{
    if (is<uint64_t>()) {
        uint64_t i1 = as<uint64_t>();
        if (other.is<int64_t>()) {
            int64_t i2 = other.as<int64_t>();
            return i2 >= 0 && i1 == i2;
        }
        else if (other.is<double>()) {
            double d2 = other.as<double>();
            return (double)i1 == d2 && (uint64_t)d2 == i1;
        }
    }
    else if (is<int64_t>()) {
        int64_t i1 = as<int64_t>();
        if (other.is<uint64_t>()) {
            uint64_t i2 = other.as<uint64_t>();
            return i1 >= 0 && i1 == i2;
        }
        else if (other.is<double>()) {
            double d2 = other.as<double>();
            return (double)i1 == d2 && (int64_t)d2 == i1;
        }
    }
    else if (is<double>()) {
        int64_t d1 = as<double>();
        if (other.is<uint64_t>()) {
            uint64_t i2 = other.as<uint64_t>();
            return (double)i2 == d1 && (int64_t)d1 == i2;
        }
        else if (other.is<int64_t>()) {
            int64_t i2 = other.as<int64_t>();
            return (double)i2 == d1 && (int64_t)d1 == i2;
        }
    }

    return value_ == other.value_;
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Symbol)
{
    addField("sym", &Symbol::sym, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Null)
{
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Function)
{
    addParent<Symbol>();
}

namespace {
thread_local std::vector<Context *> contexts;
} // file scope

void pushContext(Context & context)
{
    contexts.push_back(&context);
}

void popContext(Context & context)
{
    ExcAssert(!contexts.empty());
    ExcAssertEqual(contexts.back(), &context);
    contexts.pop_back();
}

Context & getCurrentContext()
{
    ExcAssert(!contexts.empty());
    return *contexts.back();
}

struct LispWildcardDescription: public ValueDescriptionT<Wildcard> {
    virtual void parseJsonTyped(Wildcard * val,
                                JsonParsingContext & context) const
    {
        auto j = context.expectJson();
        if (j.toStringUtf8() != "{\"wc\":\"_\"}") {
            context.exception("Expected wildcard");
        }
        auto newVal = Wildcard{};
        *val = std::move(newVal);
    }

    virtual void printJsonTyped(const Wildcard * val,
                                JsonPrintingContext & context) const
    {
        context.startObject();
        context.startMember("wc");
        context.writeString("_");
        context.endObject();
    }

    virtual bool isDefaultTyped(const Wildcard * val) const
    {
        return false;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(Wildcard, LispWildcardDescription);

struct LispEllipsisDescription: public ValueDescriptionT<Ellipsis> {
    virtual void parseJsonTyped(Ellipsis * val,
                                JsonParsingContext & context) const
    {
        auto j = context.expectJson();
        if (j.toStringUtf8() != "{\"wc\":\"...\"}") {
            context.exception("Expected ellipsis");
        }
        auto newVal = Ellipsis{};
        *val = std::move(newVal);
    }

    virtual void printJsonTyped(const Ellipsis * val,
                                JsonPrintingContext & context) const
    {
        context.startObject();
        context.startMember("wc");
        context.writeString("...");
        context.endObject();
    }

    virtual bool isDefaultTyped(const Ellipsis * val) const
    {
        return false;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(Ellipsis, LispEllipsisDescription);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(List)
{
    addParentAsField<std::vector<Value>>("list", "");
}

struct LispValueDescription: public ValueDescriptionT<Value> {
    virtual void parseJsonTyped(Value * val,
                                JsonParsingContext & context) const
    {
        auto newVal = Value::fromJson(getCurrentContext(), context);
        *val = std::move(newVal);
    }

    virtual void printJsonTyped(const Value * val,
                                JsonPrintingContext & context) const
    {
        val->toJson(context);
    }

    virtual bool isDefaultTyped(const Value * val) const
    {
        return false;
    }


};

DEFINE_VALUE_DESCRIPTION_NS(Value, LispValueDescription);

void
Value::
toJson(JsonPrintingContext & context) const
{
    if (!context_) {
        context.writeNull();
        return;
    }

#define OBJ(x) context.startMember(x)

    context.startObject();

    LambdaVisitor visitor {
        [&] (const Value & val) -> void // first is for unmatched values
        {
            static const auto desc = getDefaultDescriptionSharedT<Any>();
            OBJ("any");
            desc->printJsonTyped(&val.value_, context);
            //MLDB_THROW_UNIMPLEMENTED(("JSON print of lisp value with type " + demangle(val.value_.type())).c_str());
        },
        [&] (bool b)               { OBJ("atom");  context.writeBool(b); },
        [&] (int64_t i)            { OBJ("atom");  context.writeLongLong(i); },
        [&] (uint64_t i)           { OBJ("atom");  context.writeUnsignedLongLong(i); },
        [&] (double d)             { OBJ("atom");  context.writeDouble(d); },
        [&] (const Utf8String & s) { OBJ("atom");  context.writeStringUtf8(s); },
        [&] (Null)                 { OBJ("atom");  context.writeNull(); },
        [&] (Wildcard)             { OBJ("wc");    context.writeString("_"); },
        [&] (Ellipsis)             { OBJ("wc");    context.writeString("..."); },
        [&] (const Symbol& s)      { OBJ("sym");   context.writeStringUtf8(s.sym.toUtf8String()); },
        [&] (const List & l)
        {
            OBJ("list");
            context.startArray();
            for (auto & i: l) {
                context.newArrayElement();
                i.toJson(context);
            }
            context.endArray();
        }
    };

    visit(visitor, *this);

    if (hasMetadata()) {
        OBJ("md");
        getMetadata().toJson(context);
    }

    if (quotes_) {
        OBJ("q");
        context.writeUnsignedLongLong(quotes_);
    }

    context.endObject();
}

Value
Value::
fromJson(Context & lcontext, JsonParsingContext & pcontext)
{
    auto make = [&] (auto && val) -> Value
    {
        return Value(lcontext, std::move(val));
    };

    JsonLambdaVisitor atomVisitor {
        ExceptionOnUnknownJsonReturning<Value>(pcontext),
        [&] (bool b)               { return make(b); },
        [&] (int64_t i)            { return make(i); },
        [&] (uint64_t i)           { return make(i); },
        [&] (double d)             { return make(d); },
        [&] (const Utf8String & s) { return make(s); },
        [&] (JsonNullTag)          { return lcontext.null(); }
    };

    Value result;
    Value md;
    int quotes = 0;

    auto onMember = [&] ()
    {
        if (pcontext.inField("md")) {
            if (md.isInitialized())
                pcontext.exception("md field is repeated");
            md = Value::fromJson(lcontext, pcontext);
            return;
        }
        else if (pcontext.inField("q")) {
            if (quotes != 0)
                pcontext.exception("quote field is repeated");
            quotes = pcontext.expectUnsignedLongLong();
            return;
        }
        ExcAssert(result.isUninitialized());
        if (pcontext.inField("atom")) {
            result = visit(atomVisitor, pcontext);
        }
        else if (pcontext.inField("wc")) {
            auto s = pcontext.expectStringUtf8();
            if (s == "_") { result = make(Wildcard{}); }
            else if (s == "...") { result = make(Ellipsis{}); }
            else pcontext.exception("expected '_' or '...' for wildcard");
        }
        else if (pcontext.inField("sym")) {
            auto name = pcontext.expectStringUtf8();
            result = make(Symbol{std::move(name)});
        }
        else if (pcontext.inField("list")) {
            List list;
            pcontext.forEachElement([&] () { list.emplace_back(fromJson(lcontext, pcontext)); });
            result = make(list);
        }
        else {
            pcontext.exception("Unknown field in Lisp value: " + pcontext.fieldName());
        }
    };
    pcontext.forEachMember(onMember);

    result.setQuotes(quotes);

    if (md.isInitialized())
        result.addMetadata(std::move(md));
    return result;
}

struct StrAdd {
    template<typename Str, typename ToAdd> void operator () (Str & s, ToAdd && toAdd) const { s += toAdd; }
};

Utf8String
Value::
print() const
{
    if (!context_)
        return {};

    LambdaVisitor visitor {
        [] (const Value & val) -> Utf8String // first is for unmatched values
        {
            MLDB_THROW_UNIMPLEMENTED(("print of lisp value with type " + demangle(val.value_.type())).c_str());
        },
        [] (bool b)               { return b ? "true" : "false"; },
        [] (int64_t i)            { return std::to_string(i); },
        [] (uint64_t i)           { return std::to_string(i); },
        [] (double d)             { return std::to_string(d); },
        [] (const Utf8String & s) { return jsonEncodeStr(s); },
        [] (Null)                 { return "null"; },
        [] (Wildcard)             { return "_"; },
        [] (Ellipsis)             { return "..."; },
        [] (const Symbol& s)      { return s.sym.toUtf8String(); },
        [] (const List & l)       { return l.fold<Utf8String>(StrAdd(), std::mem_fn(&Value::print), "(", " ", ")"); }
    };

    Utf8String result = string(quotes_, '\'');
    result += visit(visitor, *this);

    if (hasMetadata()) {
        result += ":" + getMetadata().print();
    }
    return result;
}

Utf8String
Value::
asString() const
{
    if (!context_)
        MLDB_THROW_UNIMPLEMENTED();

    LambdaVisitor visitor {
        [] (const Value & val) -> Utf8String // first is for unmatched values
        {
            MLDB_THROW_UNIMPLEMENTED(("print of lisp value with type " + demangle(val.value_.type())).c_str());
        },
        [] (bool b)               { return b ? "true" : "false"; },
        [] (int64_t i)            { return std::to_string(i); },
        [] (uint64_t i)           { return std::to_string(i); },
        [] (double d)             { return std::to_string(d); },
        [] (const Utf8String & s) { return s; },
        [] (Null)                 { return "null"; },
        [] (Wildcard)             { return "_"; },
        [] (Ellipsis)             { return "..."; },
        [] (const Symbol& s)      { return s.sym.toUtf8String(); },
        [] (const Function& f)    { return f.sym; },
        [] (const List & l)       { return l.fold<Utf8String>(StrAdd(), std::mem_fn(&Value::asString), "(", " ", ")"); }
    };

    return visit(visitor, *this);
}

Utf8String
Value::
getErrorMessageString(const char * msg) const
{
    // TODO: don't print the whole thing if it's too long
    return Utf8String(msg) + " (value is " + print() + " of type " + demangle(value_.type()) + ")";
}

optional<Value>
Value::
match(Context & lcontext, ParseContext & pcontext)
{
    auto res = match_recursive(lcontext, pcontext,
                               [] (Context & lc, ParseContext & pc) { return matchAtom(lc, pc); },
                               [] (Context & lc, ParseContext & pc) { return Value::match(lc, pc); },
                               [] (Context & lc, ParseContext & pc) { return match(lc, pc); });
    if (!res)
        return nullopt;

    Value result = std::move(*res);
    return std::move(result);
}

Value
Value::
parse(Context & lcontext, ParseContext & pcontext)
{
    auto result = parse_recursive(lcontext, pcontext,
                                  [] (Context & lc, ParseContext & pc) { return parseAtom(lc, pc); },
                                  [] (Context & lc, ParseContext & pc) { return parse(lc, pc); },
                                  [] (Context & lc, ParseContext & pc) { return parse(lc, pc); });
    return result;
}

Value
Value::
parseAtom(Context & lcontext, ParseContext & pcontext)
{
    return parse_from_matcher([] (Context & lc, ParseContext & pc) { return matchAtom(lc, pc); },
                              [&] () { pcontext.exception("expected Lisp atom"); },
                              lcontext, pcontext);
}

std::optional<Value>
Value::
matchAtom(Context & lcontext, ParseContext & pcontext)
{
    ParseContext::Revert_Token token(pcontext);
    pcontext.skip_whitespace();
    Value result;

    auto make = [&] (auto && val) -> Value
    {
        token.ignore();
        return Value(lcontext, std::move(val));
    };

    if (auto str = match_delimited_string(pcontext, '\'')) {
        result = make(std::move(*str));
    }
    else if (auto str = match_delimited_string(pcontext, '\"')) {
        result = make(std::move(*str));
    }
    else if (pcontext.match_literal("true")) {
        result = make(true);
    }
    else if (pcontext.match_literal("false")) {
        result = make(false);
    }
    else if (pcontext.match_literal("null")) {
        result = make(Null{});
    }
    else {
        JsonNumber num;
        if (matchJsonNumber(pcontext, num)) {
            switch (num.type) {
            case JsonNumber::FLOATING_POINT:    result = make(num.fp);   break;
            case JsonNumber::SIGNED_INT:        result = make(num.sgn);  break;
            case JsonNumber::UNSIGNED_INT:      result = make(num.uns);  break;
            default:                            MLDB_THROW_LOGIC_ERROR();
            }
        }
        else if (pcontext.match_literal('_')) {
            result = make(Wildcard{});
        }
        else if (pcontext.match_literal("...")) {
            result = make(Ellipsis{});
        }
        // Has to go after number & wildcard
        else if (auto symName = match_symbol_name(pcontext)) {
            result = make(Symbol{std::move(*symName)});
        }
        else {
            return nullopt;
        }
    }

    return std::move(result);
}

std::optional<Value>
Value::
matchRecursive(Context & lcontext, ParseContext & pcontext,
               const std::function<std::optional<Value>(Context &, ParseContext &)> & matchAtom,
               const std::function<std::optional<Value>(Context &, ParseContext &)> & matchMetadata,
               const std::function<std::optional<Value>(Context &, ParseContext &)> & recurse)
{
    return match_recursive(lcontext, pcontext, matchAtom, matchMetadata, recurse);
}

Value
Value::
parseRecursive(Context & lcontext, ParseContext & pcontext,
               const std::function<Value(Context &, ParseContext &)> & parseAtom,
               const std::function<Value(Context &, ParseContext &)> & parseMetadata,
               const std::function<Value(Context &, ParseContext &)> & recurse)
{
    return parse_recursive(lcontext, pcontext, parseAtom, parseMetadata, recurse);
}

Value
Value::
parse(Context & lcontext, const Utf8String & val)
{
    ParseContext pcontext("<<<internal string>>>", val.rawData(), val.rawLength());
    return parse(lcontext, pcontext);
}

bool
Value::
hasMetadata() const
{
    return !md_.empty();
}

void
Value::
addMetadata(Value md)
{
    md_ = std::move(md);
}

Value
Value::
getMetadata() const
{
    if (md_.empty())
        return getContext().null();
    else
        return md_.as<Value>();
}

void
Value::
throwUnexpectedValueTypeException(const char * msg, const std::type_info & found) const
{
    throw MLDB::Exception(Utf8String(msg) + " (on converting value " + print() + " to type " + demangle(found) + ")");
}

Value
Value::
toContext(Context & otherContext) const
{
    if (&otherContext != context_)
        MLDB_THROW_UNIMPLEMENTED("toContext switch contexts");
    return *this;
}

PathElement
Value::
getSymbolName() const
{
    return as<Symbol>().sym;
}

} // namespace Lisp
} // namespace MLDB
