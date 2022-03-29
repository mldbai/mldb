/* lisp.cc                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp.h"
#include "lisp_lib.h"
#include "mldb/types/json_parsing.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/scope.h"
#include "mldb/types/any_impl.h"

using namespace std;

namespace MLDB {

// in json_parsing.cc
int getEscapedJsonCharacterPointUtf8(ParseContext & context);

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
    if (!front().is<Function>())
        throw MLDB::Exception("head of list is not a function: " + front().print());
    return front().as<Function>().fn;
}

Value::
Value()
{
}

Value::
Value(const Value & other)
    : context_(other.context_), value_(other.value_)
{
}

Value::
Value(Value && other)
    : context_(other.context_), value_(std::move(other.value_))
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
    return *this;
}

Value &
Value::
operator = (Value && other)
{
    Value oldVal = std::move(*this);
    context_ = other.context_;
    value_ = std::move(other.value_);
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
Value(Context & context, Variable var)
    : context_(&context), value_(std::move(var))
{
}

Value::
Value(Context & context, Function fn)
    : context_(&context), value_(std::move(fn))
{
}

Value::
Value(Context & context, Wildcard)
    : context_(&context), value_(Wildcard{})
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

#if 0
Value Value::list(PathElement type, std::vector<Value> args)
{
    List result;
    result.emplace_back(Function{type});
    result.insert(result.end(), std::move_iterator(args.begin()), std::move_iterator(args.end()));
    return { std::move(result) };
}

Value Value::path(Path path)
{
    return Path{path};
}

Value Value::var(PathElement name)
{
    return Variable{std::move(name)};
}

Value Value::str(Utf8String str)
{
    return std::move(str);
}

Value Value::boolean(bool b)
{
    Value result;
    result.atom = b;
    return result;
}

Value Value::i64(int64_t i)
{
    Value result;
    result.atom = i;
    return result;
}

Value Value::u64(uint64_t i)
{
    Value result;
    result.atom = i;
    return result;
}

Value Value::f64(double d)
{
    Value result;
    result.atom = d;
    return result;
}

Value Value::null()
{
    Value result;
    result.atom = Null{};
    return result;
}

Utf8String Value::print() const
{
    Utf8String result;
    if (type.null()) {
        result = jsonEncodeStr(this->atom);
    }
    else {
        result = "(" + type.toUtf8String();
        for (auto & a: args)
            result += " " + a.print();
        result += ")";
    }
    return result;
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

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Variable)
{
    addField("var", &Variable::var, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Function)
{
    addField("fn", &Function::fn, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Symbol)
{
    addField("sym", &Symbol::sym, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Null)
{
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
    if (value_.is<Null>()) {
        context.writeNull();
        return;
    }
    else if (value_.is<List>()) {
        auto & list = value_.as<List>();
        context.startArray();
        for (auto & l: list) {
            context.newArrayElement();
            l.toJson(context);
        }
        context.endArray();
        return;
    }
    static const auto desc = getBareAnyDescription();
    desc->printJsonTyped(&value_, context);
}

Value
Value::
fromJson(Context & lcontext, JsonParsingContext & pcontext)
{
    auto make = [&] (auto && val) -> Value
    {
        return Value(lcontext, std::move(val));
    };

    if (pcontext.isNull()) {
        pcontext.expectNull();
        return make(Null());
    }
    else if (pcontext.isBool()) {
        return make(pcontext.expectBool());
    }
    else if (pcontext.isUnsigned()) {
        return make(pcontext.expectUnsignedLongLong());
    }
    else if (pcontext.isNumber()) {
        Json::Value j = pcontext.expectJson();
        if (j.isInt())
            return make(j.asInt());
        else if (j.isUInt())
            return make(j.asUInt());
        else if (j.isDouble())
            return make(j.asDouble());
        else MLDB_THROW_LOGIC_ERROR();
    }
    else if (pcontext.isString()) {
        return make(pcontext.expectStringUtf8());
    }
    else if (pcontext.isObject()) {
        Value result;
        bool first = true;
        auto onMember = [&] ()
        {
            if (!first)
                pcontext.exception("multi-field objects don't occur in JSON parsing of LISP Values");

            auto fname = pcontext.fieldNameView();
            if (fname == "var") {
                auto name = pcontext.expectStringUtf8();
                result = { lcontext, Variable{std::move(name)} };
            }
            else if (fname == "sym") {
                auto name = pcontext.expectStringUtf8();
                result = { lcontext, Symbol{std::move(name)} };
            }
            else if (fname == "wc") {
                auto name = pcontext.expectStringUtf8();
                if (name != "_")
                    pcontext.exception("Wildcard name must be '_'");
                result = { lcontext, Wildcard{} };
            }
            else if (fname == "fn") {
                auto name = pcontext.expectStringUtf8();
                result = { lcontext, Function{PathElement{std::move(name)}, nullptr} };
            }
            else {
                MLDB_THROW_UNIMPLEMENTED(("fname " + string(fname)).c_str());
            }
            first = false;
        };
        pcontext.forEachMember(onMember);
        return result;
    }
    else if (pcontext.isArray()) {
        List list;
        auto onElement = [&] ()
        {
            Value val = fromJson(lcontext, pcontext);
            list.emplace_back(std::move(val));
        };
        pcontext.forEachElement(onElement);
        return make(std::move(list));
    }
    else {
        MLDB_THROW_LOGIC_ERROR();
    }
}

Utf8String
Value::
print() const
{
    if (!context_)
        return {};
    else if (value_.is<bool>()) {
        return value_.as<bool>() ? "true" : "false";
    }
    else if (value_.is<int64_t>()) {
        return std::to_string(value_.as<int64_t>());
    }
    else if (value_.is<uint64_t>()) {
        return std::to_string(value_.as<uint64_t>());
    }
    else if (value_.is<Utf8String>()) {
        return jsonEncodeStr(value_.as<Utf8String>());
    }
    else if (value_.is<Null>()) {
        return "null";
    }
    else if (value_.is<List>()) {
        Utf8String result = "(";
        bool first = true;
        for (auto & i: value_.as<List>()) {
            if (!first) {
                result += " ";
            }
            first = false;
            result += i.print();
        }
        result += ")";
        return result;
    }
    else if (value_.is<Variable>()) {
        return value_.as<Variable>().var.toUtf8String();
    }
    else if (value_.is<Function>()) {
        return value_.as<Function>().fn.toUtf8String();
    }
    else if (value_.is<Wildcard>()) {
        return "_";
    }
    else if (value_.is<Symbol>()) {
        return "`" + value_.as<Symbol>().sym.toUtf8String();
    }

    MLDB_THROW_UNIMPLEMENTED(("print of lisp value with type " + demangle(value_.type())).c_str());
}

std::optional<PathElement>
match_rule_name(ParseContext & context)
{
    std::string segment;
    segment.reserve(18);
    context.skip_whitespace();
    if (!context) {
        return nullopt;
    }
    else {
        char c = *context;
        // Rules start with a capital letter or an @
        if (!isupper(c) && c != '@')
            return nullopt;
        segment += c;  c = *(++context);
        while (isalpha(c) || c == '_' || (!segment.empty() && isnumber(c))) {
            segment += c;
            ++context;
            if (context.eof())
                break;
            c = *context;
        }
        if (segment.empty())
            return nullopt;
        else return PathElement(segment);
    }
}

// Current character is included if include is true
std::optional<PathElement>
match_rest_of_name(ParseContext & context, bool includeFirst, ParseContext::Revert_Token * token = nullptr)
{
    std::string segment;
    segment.reserve(18);

    //cerr << "rest of name: first = " << *context << endl;

    if (includeFirst)
        segment += *context;
    ++context;

    while (context) {
        char c = *context;
        //cerr << "rest of name: next = " << *context << endl;
        if (!isalpha(c) && c != '_' && (segment.empty() || !isnumber(c)))
            break;
        segment += c;
        ++context;
    }
    if (segment.empty())
        return nullopt;
    if (token)
        token->ignore();
    return PathElement(segment);
}

std::optional<PathElement>
match_variable_name(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    if (!context) {
        return nullopt;
    }

    char c = *context;
    // Variables start with a lowercase letter or an $
    if (!islower(c) && c != '$')
        return nullopt;
    return match_rest_of_name(context, c != '$', &token);
}

std::optional<PathElement>
match_symbol_name(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    if (!context || *context != '`')
        return nullopt;

    //cerr << "Matching rest of symbol name" << endl;
    return match_rest_of_name(context, false /* includeFirst */, &token);
}

static constexpr std::array operators{'+', '-', '*', '/', '%'};

std::optional<PathElement>
match_operator_name(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    context.skip_whitespace();
    for (char op: operators) {
        if (context.match_literal(op)) {
            token.ignore();
            return PathElement(std::string(1, op));
        }
    }
    return nullopt;
}

optional<Utf8String> match_delimited_string(ParseContext & context, char delim)
{
    ParseContext::Revert_Token token(context);

    context.skip_whitespace();

    if (!context.match_literal(delim))
        return nullopt;

    try {
        char internalBuffer[4096];

        char * buffer = internalBuffer;
        size_t bufferSize = 4096;
        size_t pos = 0;
        Scope_Exit(if (buffer != internalBuffer) delete[] buffer);

        // Keep expanding until it fits
        while (!context.match_literal(delim)) {
            if (context.eof())
                return nullopt;

            // We need up to 4 characters to add a new UTF-8 code point
            if (pos >= bufferSize - 4) {
                size_t newBufferSize = bufferSize * 8;
                char * newBuffer = new char[newBufferSize];
                std::copy(buffer, buffer + bufferSize, newBuffer);
                if (buffer != internalBuffer)
                    delete[] buffer;
                buffer = newBuffer;
                bufferSize = newBufferSize;
            }

            int c = *context;
            
            //cerr << "c = " << c << " " << (char)c << endl;

            if (c < 0 || c > 127) {
                // Unicode
                c = context.expect_utf8_code_point();

                // 3.  Write the decoded character to the buffer
                char * p1 = buffer + pos;
                char * p2 = p1;
                pos += utf8::append(c, p2) - p1;

                continue;
            }
            ++context;

            if (c == '\\') {
                c = getEscapedJsonCharacterPointUtf8(context);
            }

            if (c < ' ' || c >= 127) {
                char * p1 = buffer + pos;
                char * p2 = p1;
                pos += utf8::append(c, p2) - p1;
            }
            else buffer[pos++] = c;
        }

        Utf8String result(string(buffer, buffer + pos));
        
        token.ignore();
        return result;
    } catch (const MLDB::Exception & exc) {
        return nullopt;
    }
}

optional<Value>
Value::
match(Context & lcontext, ParseContext & pcontext)
{
    auto make = [&] (auto && val) -> Value
    {
        return Value(lcontext, std::move(val));
    };

    Value result;
    pcontext.skip_whitespace();
    if (pcontext.match_literal('(')) {
        //cerr << "after bracket, ofs = " << pcontext.get_offset() << endl;
        pcontext.match_whitespace();
        //cerr << "after whitespace, ofs = " << pcontext.get_offset() << endl;
        std::optional<Value> arg;
        List list;
        while ((arg = match(lcontext, pcontext))) {
            //cerr << "matched " << *arg << endl;
            list.emplace_back(std::move(*arg));
            if (list.size() == 1 && list.back().is<Variable>()) {
                list.back() = { lcontext, Function{list.back().as<Variable>().var, nullptr} };
            }
            if (!pcontext.match_whitespace())
                break;
        }
        //cerr << "finished matching" << endl;
        //if (pcontext)
        //    cerr << "*context = " << *pcontext << endl;
        //else
        //    cerr << "EOF" << endl;
        result = make(std::move(list));
        //cerr << "result = " << result << endl;
        pcontext.skip_whitespace();
        pcontext.expect_literal(')', "expected ')' to close list");
    }
    else if (auto str = match_delimited_string(pcontext, '\'')) {
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
    else if (auto varName = match_variable_name(pcontext)) {
        result = make(Variable{std::move(*varName)});
    }
    else if (auto symName = match_symbol_name(pcontext)) {
        result = make(Symbol{std::move(*symName)});
    }
    else {
        JsonNumber num;
        if (matchJsonNumber(pcontext, num)) {
            switch (num.type) {
            case JsonNumber::FLOATING_POINT:    result = make(num.fp);  break;
            case JsonNumber::SIGNED_INT:        result = make(num.sgn);  break;
            case JsonNumber::UNSIGNED_INT:      result = make(num.uns);  break;
            default:                            MLDB_THROW_LOGIC_ERROR();
            }
        }
        else if (auto opName = match_operator_name(pcontext)) {
            // Has to go after number
            result = make(Function{*opName, nullptr});
        }
        else if (pcontext.match_literal('_')) {
            result = make(Wildcard{});
        }
        else {
            return nullopt;
        }
    }

    return std::move(result);
}

Value
Value::
parse(Context & lcontext, const Utf8String & val)
{
    ParseContext pcontext("<<<internal string>>>", val.rawData(), val.rawLength());
    return parse(lcontext, pcontext);
}

Value
Value::
parse(Context & lcontext, ParseContext & pcontext)
{
    auto matched = match(lcontext, pcontext);
    if (!matched)
        pcontext.exception("expected Lisp value");
    return std::move(*matched);
}

} // namespace Lisp
} // namespace MLDB
