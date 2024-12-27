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

DEFINE_ENUM_DESCRIPTION_INLINE(MetadataType)
{
    addValue("SOURCE_LOCATION", MetadataType::SOURCE_LOCATION, "Location in source code where we're executing");
    addValue("TYPE_INFO", MetadataType::TYPE_INFO, "Type information about the expression");
}

std::string metadataTypeToString(MetadataType tp)
{
    switch (tp) {
        case MetadataType::SOURCE_LOCATION: return "loc";
        case MetadataType::TYPE_INFO: return "type";
        default:
            MLDB_THROW_LOGIC_ERROR("unexpected value of MetadataType %d", tp);
    }
}

MetadataType stringToMetadataType(std::string_view mdType)
{
    if (mdType == "loc")
        return MetadataType::SOURCE_LOCATION;
    else if (mdType == "type")
        return MetadataType::TYPE_INFO;
    else
        MLDB_THROW_RUNTIME_ERROR("unknown MetadataType string encountered: '%s'", string(mdType).c_str());
}

Value metadataTypeToValue(Context & lcontext, MetadataType tp)
{
    return lcontext.sym(metadataTypeToString(tp));
}

MetadataType valueToMetadataType(const Value & mdType)
{
    if (!mdType.is<Symbol>())
        MLDB_THROW_RUNTIME_ERROR("MetadataType type must be string; got %s", mdType.print().c_str());
    return stringToMetadataType(mdType.as<Symbol>().sym.toUtf8String().rawString());
}


/*******************************************************************************/
/* LISP VALUE                                                                  */
/*******************************************************************************/

List::List(ListBuilder builder)
{
    size_t n = builder.size();
    items.vals = std::make_shared<std::vector<Value>>(std::move(builder));
    items.start = 0;
    items.end = n;
}

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

ListBuilder List::steal()
{
    // Create a moveable view of the values, stealing them if we're the only reference
    // or copying them otherwise
    ListBuilder result;
    if (items.vals.use_count() == 1) {
        // Only reference?  We can have mutable access
        if (items.start == 0 && items.end == items.vals->size()) {
            result = std::move(*items.vals);
            items.start = items.end = 0;
        }
        else {
            result = ListBuilder{std::make_move_iterator(items.vals->begin() + items.start),
                                 std::make_move_iterator(items.vals->end() + items.end)};
        }
    }
    else {
        result = ListBuilder{items.vals->begin() + items.start,
                             items.vals->end() + items.end};
    }

    return result;
}

ListBuilder List::steal() const
{
    ListBuilder result(begin(), end());
    return result;
}

const Value & List::front() const
{
    return items.vals->at(items.start);
}

const Value & List::back() const
{
    return items.vals->at(items.end - 1);
}

const Value & List::at(size_t n) const
{
    //cerr << "at " << n << " size " << items.size() << " start " << items.start << " end " << items.end << endl;
    size_t i = items.nToI(n);
    //cerr << " i = " << i << endl;
    return items.vals->at(i);
}

const Value & List::operator [] (size_t n) const
{
    return at(n);
}

size_t List::size() const
{
    return items.end - items.start;
}

bool List::empty() const
{
    return items.end == items.start;
}

ListIterator List::begin() const
{
    return { items.vals->data() + items.start };
}

ListIterator List::end() const
{
    return { items.vals->data() + items.end };
}

List List::tail(size_t n) const
{
    if (n >= size()) {
        return List();
    }

    List result = *this;
    result.items.start += n;
    //cerr << "tail n=" << n << " start " << items.start << " end " << items.end << endl;
    return result;
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

Value::
Value(Context & context, SourceLocation loc)
    : context_(&context), value_(std::move(loc))
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
    //cerr << "Value::operator=: " << *this << " vs " << other << endl;

    if (isNumeric() && other.isNumeric()) {
        //cerr << "  *** value numerical comparison" << endl;
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
        else {
            MLDB_THROW_LOGIC_ERROR("isNumeric should be exhaustively handled: comparing " + print() + " with " + other.print());
        }
    }

    if (type() != other.type()) {
        //cerr << "  *** different types (false): " << endl;
        return false;
    }

    //cerr << "comparing values of type " << demangle(type()) << " and " << demangle(other.type()) << endl;
    LambdaVisitor visitor {
        // Default case here handles everything that's equality comparable via the value description
        [&] (const Value & val)    { return val.desc().compareEquality(val.getBytes().data(), other.getBytes().data()); },

        // Exceptions go here
        [&] (const List & l1)      { const List & l2 = other.as<List>();
                                     if (l1.size() != l2.size()) return false;
                                     for (size_t i = 0;  i < l1.size();  ++i) {
                                        if (l1[i] != l2[i]) { return false; }
                                     }
                                     return true; },
        [&] (Null)                 { return true; },
        [&] (Ellipsis)             { return true; },
        [&] (Wildcard)             { return true; },
        [&] (const Symbol & s)     { return s.sym == other.as<Symbol>().sym; }
        // ... TODO LOTS of others...
    };

    return visit(visitor, *this);
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

struct ListDescription
    : public ValueDescriptionI<List, ValueKind::ARRAY, ListDescription> {

    std::shared_ptr<const ValueDescriptionT<Value> > inner;

    ListDescription(ValueDescriptionT<Value> * inner)
        : inner(inner)
    {
    }

    ListDescription(std::shared_ptr<const ValueDescriptionT<Value> > inner
                       = MLDB::getDefaultDescriptionShared((Value *)0))
        : inner(std::move(inner))
    {
    }

    // Constructor to create a partially-evaluated span description.
    ListDescription(ConstructOnly)
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        List * val2 = reinterpret_cast<List *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(List * val, JsonParsingContext & context) const override
    {
        ListBuilder list;
        auto onElement = [&] ()
        {
            Value val;
            inner->parseJsonTyped(&val, context);
            list.emplace_back(val);
        };

        context.forEachElement(onElement);

        *val = std::move(list);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(const List * val, JsonPrintingContext & context) const override
    {
        size_t sz = val->size();
        context.startArray(sz);

        auto it = val->begin();
        for (size_t i = 0;  i < sz;  ++i, ++it) {
            ExcAssert(it != val->end());
            context.newArrayElement();
            Value v(*it);
            inner->printJsonTyped(&v, context);
        }
        
        context.endArray();
    }

    virtual bool isDefault(const void * val) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        return isDefaultTyped(val2);
    }

    virtual bool isDefaultTyped(const List * val) const override
    {
        return val->empty();
    }

    virtual size_t getArrayLength(void * val) const override
    {
        List * val2 = reinterpret_cast<List *>(val);
        return val2->size();
    }

    virtual LengthModel getArrayLengthModel() const override
    {
        return LengthModel::VARIABLE;
    }

    virtual OwnershipModel getArrayIndirectionModel() const override
    {
        return OwnershipModel::UNIQUE;
    }
    
    virtual void * getArrayElement(void * val, uint32_t element) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        return (void *)&val2->at(element);
    }

    virtual const void * getArrayElement(const void * val, uint32_t element) const override
    {
        const List * val2 = reinterpret_cast<const List *>(val);
        ExcAssertLess(element, val2->size());
        return &val2[element];
    }

    virtual void setArrayLength(void * val, size_t newLength) const override
    {
    }
    
    virtual const ValueDescription & contained() const override
    {
        return *this->inner;
    }

    virtual std::shared_ptr<const ValueDescription> containedPtr() const override
    {
        return this->inner;
    }

    virtual void initialize() override
    {
        this->inner = getDefaultDescriptionSharedT<Value>();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(List, ListDescription);


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
#define OBJ(x) context.startMember(x)

    if (!context_) {
        context.startObject();
        OBJ("uninitialized");
        context.writeNull();
        context.endObject();
        return;
    }

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
            for (const auto & i: l) {
                context.newArrayElement();
                i.toJson(context);
            }
            context.endArray();
        },
        [&] (const SourceLocation & loc) { OBJ("loc");  context.writeStringUtf8(loc.print()); },
    };

    visit(visitor, *this);

    if (!md_.empty()) {
        OBJ("md");
        context.startObject();
        for (const auto & [mdType, mdValue]: md_) {
            context.startMember(metadataTypeToString(mdType));
            mdValue.toJson(context);
        }
        context.endObject();
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
    std::map<MetadataType, Value> md;
    bool mdDone = false;
    int quotes = 0;

    auto onMember = [&] ()
    {
        if (pcontext.inField("md")) {
            if (mdDone)
                pcontext.exception("md field is repeated");
            auto onMember = [&] ()
            {
                MetadataType mdType = stringToMetadataType(pcontext.fieldNameView());
                Value mdValue = Value::fromJson(lcontext, pcontext);
                if (!md.emplace(mdType, std::move(mdValue)).second)
                    pcontext.exception("duplicate metadata field");
            };
            pcontext.forEachMember(onMember);
            mdDone = true;
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
        else if (pcontext.inField("loc")) {
            auto loc = pcontext.expectStringUtf8();
            result = make(SourceLocation::parse(loc));
        }
        else if (pcontext.inField("list")) {
            ListBuilder list;
            pcontext.forEachElement([&] () { list.emplace_back(fromJson(lcontext, pcontext)); });
            result = make(list);
        }
        else if (pcontext.inField("uninitialized")) {
            pcontext.expectNull();
            result = Value();
        }
        else {
            pcontext.exception("Unknown field in Lisp value: " + pcontext.fieldName());
        }
    };
    pcontext.forEachMember(onMember);

    result.setQuotes(quotes);

    if (!md.empty()) {
        for (auto & [mdType, mdValue]: md) {
            result.addMetadata(mdType, std::move(mdValue));
        }
    }

    return result;
}

struct StrAdd {
    template<typename Str, typename ToAdd> void operator () (Str & s, ToAdd && toAdd) const { s += toAdd; }
};

Utf8String
Value::
print(const std::set<MetadataType> & mdToPrint) const
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
        [] (Null)                 { return "nil"; },
        [] (Wildcard)             { return "_"; },
        [] (Ellipsis)             { return "..."; },
        [] (const Symbol& s)      { return s.sym.toUtf8String(); },
        [=] (const List & l)      { return l.fold<Utf8String>(StrAdd(), [=] (const Value & v) { return v.print(mdToPrint); }, "(", " ", ")"); },
        [] (const SourceLocation & l) { return l.print(); },
    };

    Utf8String result = string(quotes_, '\'');
    result += visit(visitor, *this);

    if (!mdToPrint.empty() && !md_.empty()) {
        bool found = false;
        int i = 0;
        for (auto & [mdType, mdValue]: md_) {
            if (!mdToPrint.count(mdType))
                continue;
            if (!found) {
                result += ":(";
                found = true;
            }
            if (i++ != 0) result += " ";
            result += metadataTypeToString(mdType) + " " + mdValue.print();
        }
        if (found)
            result += ")";
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
        [] (Null)                 { return "nil"; },
        [] (Wildcard)             { return "_"; },
        [] (Ellipsis)             { return "..."; },
        [] (const Symbol& s)      { return s.sym.toUtf8String(); },
        [] (const Function& f)    { return f.sym; },
        [] (const List & l)       { return l.fold<Utf8String>(StrAdd(), std::mem_fn(&Value::asString), "(", " ", ")"); },
        [] (const SourceLocation & l) { return l.print(); },
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
    skipLispWhitespace(pcontext);
    Value result;
    SourceLocation loc = getSourceLocation(pcontext);

    auto make = [&] (auto && val) -> Value
    {
        token.ignore();
        Value result(lcontext, std::move(val));
        addSourceLocation(result, loc);
        return result;
    };

    if (auto str = match_delimited_string(pcontext, '\'')) {
        result = make(std::move(*str));
    }
    else if (auto str = match_delimited_string(pcontext, '\"')) {
        result = make(std::move(*str));
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
        else if (pcontext.match_literal("...")) {
            result = make(Ellipsis{});
        }
        // Has to go after number & wildcard
        else if (auto symName = match_symbol_name(pcontext)) {
            if (symName->stringEqual("true") || symName->stringEqual("t")) {
                result = make(true);
            }
            else if (symName->stringEqual("false") || symName->stringEqual("f")) {
                result = make(false);
            }
            else if (symName->stringEqual("nil")) {
                result = make(Null{});
            }
            else if (symName->stringEqual("_")) {
                result = make(Wildcard{});
            }
            else {
                result = make(Symbol{std::move(*symName)});
            }
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
parse(Context & lcontext, const Utf8String & val, const SourceLocation & loc)
{
    ParseContext pcontext(loc.file().c_str(), val.rawData(), val.rawLength(), loc.line(), loc.column());
    Value result = parse(lcontext, pcontext);
    skipLispWhitespace(pcontext);
    pcontext.expect_eof();
    return result;
}

bool
Value::
hasMetadata(MetadataType mdType) const
{
    return md_.count(mdType);
}

void
Value::
addMetadata(MetadataType mdType, Value md)
{
    if (!md_.emplace(mdType, md).second) {  // todo: std::move(md)
        throw MLDB::Exception("attempt to add metadata that already exists on value: type "
                              + metadataTypeToString(mdType) + " existing " + md_[mdType].print()
                              + " added " + md.print());
    }
}

Value
Value::
getMetadata(MetadataType mdType) const
{
    auto it = md_.find(mdType);
    if (it != md_.end())
        return it->second;
    else
        return getContext().null();
}

const Value &
Value::
getExistingMetadata(MetadataType mdType) const
{
    auto it = md_.find(mdType);
    if (it != md_.end())
        return it->second;
    throw MLDB::Exception("expected metadata " + metadataTypeToString(mdType) + " not found on value " + print());
}

bool
Value::
removeMetadata(MetadataType tp)
{
    return md_.erase(tp);
}

bool
Value::
clearMetadata()
{
    bool result = !md_.empty();
    md_.clear();
    return result;
}

bool
Value::
removeMetadataRecursive(MetadataType tp)
{
    bool result = removeMetadata(tp);
    if (is<List>()) {
        ListBuilder newList;
        const List & l = as<List>();
        for (Value v: l) {
            bool removed = v.removeMetadataRecursive(tp);
            result = result || removed;
            newList.emplace_back(std::move(v));
        }
        value_ = List{std::move(newList)};
    }
    return result;
}

bool
Value::
clearMetadataRecursive()
{
    bool result = clearMetadata();
    if (is<List>()) {
        ListBuilder newList;
        const List & l = as<List>();
        for (Value v: l) {
            bool removed = v.clearMetadataRecursive();
            result = result || removed;
            newList.emplace_back(std::move(v));
        }
        value_ = List{std::move(newList)};
    }
    return result;
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

bool
Value::
truth() const
{
    if (!context_)
        MLDB_THROW_UNIMPLEMENTED();

    LambdaVisitor visitor {
        [] (const Value & val) { return true; },
        [] (bool b)            { return b; },
        [] (Null)              { return false; },
        [] (const List & l)    { return !l.empty(); }
    };

    return visit(visitor, *this);
}

bool
Value::
isNumeric() const
{
    LambdaVisitor visitor {
        []  (const Value & val) { return false; },  // default: not numeric
        [&] (double d)          { return !is<bool>(); },   // convertible to double: numeric unless bool
    };

    return visit(visitor, *this);
}

SourceLocation getSourceLocation(const Value & val)
{
    if (val.hasMetadata(MetadataType::SOURCE_LOCATION))
        return val.getExistingMetadata(MetadataType::SOURCE_LOCATION).as<SourceLocation>();
    else return {};
}

SourceLocation getSourceLocation(const ParseContext & pcontext)
{
    return getRawSourceLocation(pcontext.get_filename(), pcontext.get_line(), pcontext.get_col());
}

SourceLocation getRawSourceLocation(Utf8String file, int line, int column)
{
    SourceLocationEntry result;
    result.file = file;
    result.line = line;
    result.column = column;

    return { { result } };
}

SourceLocation getSourceLocation(const Value & val, Utf8String file, int line, int column, Utf8String function)
{
    SourceLocationEntry entry;
    entry.file = file;
    entry.line = line;
    entry.column = column;
    entry.name = std::move(function);

    SourceLocation result = getSourceLocation(val);
    result.locations.emplace_back(std::move(entry));
    return result;
}

Utf8String SourceLocationEntry::print() const
{
    
    Utf8String result = "[" + file + ":" + std::to_string(line) + ":" + std::to_string(column);
    if (!name.empty())
        result += "@" + name;
    result += "]";
    return result;
}

std::optional<SourceLocationEntry> SourceLocationEntry::parse(ParseContext & pcontext)
{
    ParseContext::Revert_Token token(pcontext);

    SourceLocationEntry result;

    auto success = [&] () -> std::optional<SourceLocationEntry> { token.ignore(); return std::move(result); };
    auto failure = [&] () { return std::nullopt; };

    if (!pcontext.match_literal('['))
        return failure();
    std::string s;
    if (!pcontext.match_text(s, ':'))
        return failure();
    result.file = std::move(s);
    if (!pcontext.match_literal(':') || !pcontext.match_numeric(result.line))
        return failure();
    if (pcontext.match_literal(':')) {
        if (!pcontext.match_numeric(result.column))
            return failure();
    }
    if (pcontext.match_literal('@')) {
        if (!pcontext.match_text(s, ']'))
            return failure();
        result.name = std::move(s);
    }
    if (!pcontext.match_literal(']'))
        return failure();
    return success();
}

Utf8String SourceLocation::file() const
{
    return this->locations.at(0).file;
}

int SourceLocation::line() const
{
    return this->locations.at(0).line;
}

int SourceLocation::column() const
{
    return this->locations.at(0).column;
}

Utf8String SourceLocation::print() const
{
    Utf8String result;
    if (locations.empty())
        return result;
    result = locations[0].print();
    if (locations.size() > 1) {
        result += " (";
        for (size_t i = 1;  i < locations.size();  ++i)
            result += " from " + locations[i].print();
        result += ")";
    }
    return result;
}

SourceLocation SourceLocation::parse(const Utf8String & str)
{
    ParseContext pcontext("<<DATA>>", str.rawData(), str.rawLength());

    SourceLocation loc;

    while (pcontext) {
        auto entry = SourceLocationEntry::parse(pcontext);
        if (!entry)
            break;
        loc.locations.emplace_back(std::move(*entry));
    }

    pcontext.expect_eof("extra characters at end of source location");

    return loc;
}

void addSourceLocation(Value & val, SourceLocation loc)
{
    val.addMetadata(MetadataType::SOURCE_LOCATION, val.getContext().loc(std::move(loc)));
}

DEFINE_ENUM_DESCRIPTION_INLINE(SourceLocationKind)
{
    addValue("NONE", SourceLocationKind::NONE, "");
    addValue("USER_PROVIDED", SourceLocationKind::USER_PROVIDED, "");
    addValue("AUTO_GENERATED", SourceLocationKind::USER_PROVIDED, "");
}

DEFINE_ENUM_DESCRIPTION_INLINE(SourceLocationType)
{
    addValue("NONE", SourceLocationType::NONE, "");
    addValue("FUNCTION", SourceLocationType::FUNCTION, "");
    addValue("MACRO", SourceLocationType::MACRO, "");
    addValue("DEFINITION", SourceLocationType::DEFINITION, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(SourceLocationEntry)
{
    addAuto("file", &SourceLocationEntry::file, "File in which it's defined");
    addAuto("line", &SourceLocationEntry::line, "Line number in file");
    addAuto("column", &SourceLocationEntry::column, "Column number in line");
    addAuto("kind", &SourceLocationEntry::kind, "???");
    addAuto("type", &SourceLocationEntry::type, "???");
    addAuto("name", &SourceLocationEntry::name, "Name of function, macro, etc");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(SourceLocation)
{
    addField("locations", &SourceLocation::locations, "Locations associated with this frame");
}


} // namespace Lisp
} // namespace MLDB
