// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* json_parsing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#include "json_parsing.h"
#include "json_parsing_impl.h"
#include "string.h"
#include "value_description.h"
#include <boost/algorithm/string.hpp>
#include "mldb/base/parse_context.h"


using namespace std;
using namespace ML;

namespace Datacratic {


bool expectJsonBool(Parse_Context & context)
{
    if (context.match_literal("true"))
        return true;
    else if (context.match_literal("false"))
        return false;
    context.exception("expected bool (true or false)");
}

/** Expect a JSON number.  This function is written in this strange way
    because JsonCPP is not a require dependency of jml, but the function
    needs to be out-of-line.
*/
JsonNumber expectJsonNumber(Parse_Context & context);

/** Match a JSON number. */
bool matchJsonNumber(Parse_Context & context, JsonNumber & num);


/*****************************************************************************/
/* JSON UTILITIES                                                            */
/*****************************************************************************/

void skipJsonWhitespace(Parse_Context & context)
{
    // Fast-path for the usual case for not EOF and no whitespace
    if (JML_LIKELY(!context.eof())) {
        char c = *context;
        if (c > ' ') {
            return;
        }
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r')
            return;
    }

    while (!context.eof()
           && (context.match_whitespace() || context.match_eol()));
}

bool matchJsonString(Parse_Context & context, std::string & str)
{
    Parse_Context::Revert_Token token(context);

    skipJsonWhitespace(context);
    if (!context.match_literal('"')) return false;

    std::string result;

    while (!context.match_literal('"')) {
        if (context.eof()) return false;
        int c = *context++;
        //if (c < 0 || c >= 127)
        //    context.exception("invalid JSON string character");
        if (c != '\\') {
            result.push_back(c);
            continue;
        }
        c = *context++;
        switch (c) {
        case 't': result.push_back('\t');  break;
        case 'n': result.push_back('\n');  break;
        case 'r': result.push_back('\r');  break;
        case 'f': result.push_back('\f');  break;
        case 'b': result.push_back('\b');  break;
        case '/': result.push_back('/');   break;
        case '\\':result.push_back('\\');  break;
        case '"': result.push_back('"');   break;
        case 'u': {
            int code = context.expect_hex4();
            if (code<0 || code>255)
            {
                return false;
            }
            result.push_back(code);
            break;
        }
        default:
            return false;
        }
    }

    token.ignore();
    str = result;
    return true;
}

std::string expectJsonStringAsciiPermissive(Parse_Context & context, char sub)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    // Try multiple times to make it fit
    while (!context.match_literal('"')) {
        int c = *context++;
        if (c == '\\') {
            c = *context++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                int code = context.expect_hex4();
                c = code;
                break;
            }
            default:
                context.exception("invalid escaped char");
            }
        }
        if (c < ' ' || c >= 127)
            c = sub;
        if (pos == bufferSize) {
            size_t newBufferSize = bufferSize * 8;
            char * newBuffer = new char[newBufferSize];
            std::copy(buffer, buffer + bufferSize, newBuffer);
            if (buffer != internalBuffer)
                delete[] buffer;
            buffer = newBuffer;
            bufferSize = newBufferSize;
        }
        buffer[pos++] = c;
    }

    string result(buffer, buffer + pos);
    if (buffer != internalBuffer)
        delete[] buffer;
    
    return result;
}

ssize_t expectJsonStringAscii(Parse_Context & context, char * buffer, size_t maxLength)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    size_t bufferSize = maxLength - 1;
    size_t pos = 0;

    // Try multiple times to make it fit
    while (!context.match_literal('"')) {
        int c = *context++;
        if (c == '\\') {
            c = *context++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                int code = context.expect_hex4();
                if (code<0 || code>255) {
                    context.exception(format("non 8bit char %d", code));
                }
                c = code;
                break;
            }
            default:
                context.exception("invalid escaped char");
            }
        }
        if (c < 0 || c >= 127)
           context.exception("invalid JSON ASCII string character");
        if (pos == bufferSize) {
            return -1;
        }
        buffer[pos++] = c;
    }

    buffer[pos] = 0; // null terminator

    return pos;
}

std::string expectJsonStringAscii(Parse_Context & context)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    // Try multiple times to make it fit
    while (!context.match_literal('"')) {

#if 0 // attempt to do it a block at a time
        char * bufferEnd = cbuffer + bufferSize;

        int charsMatched
            = context.match_text(buffer + pos, buffer + bufferSize,
                                 "\"\\");
        pos += charsMatched;
#endif

        int c = *context++;
        if (c == '\\') {
            c = *context++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                int code = context.expect_hex4();
                if (code<0 || code>255) {
                    context.exception(format("non 8bit char %d", code));
                }
                c = code;
                break;
            }
            default:
                context.exception("invalid escaped char");
            }
        }
        if (c < 0 || c >= 127)
           context.exception("invalid JSON ASCII string character");
        if (pos == bufferSize) {
            size_t newBufferSize = bufferSize * 8;
            char * newBuffer = new char[newBufferSize];
            std::copy(buffer, buffer + bufferSize, newBuffer);
            if (buffer != internalBuffer)
                delete[] buffer;
            buffer = newBuffer;
            bufferSize = newBufferSize;
        }
        buffer[pos++] = c;
    }
    
    string result(buffer, buffer + pos);
    if (buffer != internalBuffer)
        delete[] buffer;
    
    return result;
}

bool
matchJsonNull(Parse_Context & context)
{
    skipJsonWhitespace(context);
    return context.match_literal("null");
}

void
expectJsonArray(Parse_Context & context,
                const std::function<void (int, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return;

    context.expect_literal('[');
    skipJsonWhitespace(context);
    if (context.match_literal(']')) return;

    for (int i = 0;  ; ++i) {
        skipJsonWhitespace(context);

        onEntry(i, context);

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    context.expect_literal(']');
}

void
expectJsonObject(Parse_Context & context,
                 const std::function<void (const std::string &, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return;

    context.expect_literal('{');

    skipJsonWhitespace(context);

    if (context.match_literal('}')) return;

    for (;;) {
        skipJsonWhitespace(context);

        string key = expectJsonStringAscii(context);

        skipJsonWhitespace(context);

        context.expect_literal(':');

        skipJsonWhitespace(context);

        onEntry(key, context);

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    context.expect_literal('}');
}

void
expectJsonObjectAscii(Parse_Context & context,
                      const std::function<void (const char *, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return;

    context.expect_literal('{');

    skipJsonWhitespace(context);

    if (context.match_literal('}')) return;

    for (;;) {
        skipJsonWhitespace(context);

        char keyBuffer[1024];

        ssize_t done = expectJsonStringAscii(context, keyBuffer, 1024);
        if (done == -1)
            context.exception("JSON key is too long");

        skipJsonWhitespace(context);

        context.expect_literal(':');

        skipJsonWhitespace(context);

        onEntry(keyBuffer, context);

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    context.expect_literal('}');
}

bool
matchJsonObject(Parse_Context & context,
                const std::function<bool (const std::string &, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return true;

    if (!context.match_literal('{')) return false;
    skipJsonWhitespace(context);
    if (context.match_literal('}')) return true;

    for (;;) {
        skipJsonWhitespace(context);

        string key = expectJsonStringAscii(context);

        skipJsonWhitespace(context);
        if (!context.match_literal(':')) return false;
        skipJsonWhitespace(context);

        if (!onEntry(key, context)) return false;

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    if (!context.match_literal('}')) return false;

    return true;
}

JsonNumber expectJsonNumber(Parse_Context & context)
{
    JsonNumber result;

    std::string number;
    number.reserve(32);

    bool negative = false;
    bool doublePrecision = false;

    if (context.match_literal('-')) {
        number += '-';
        negative = true;
    }

    // EXTENSION: accept NaN and positive or negative infinity
    if (context.match_literal('N')) {
        context.expect_literal("aN");
        result.fp = negative ? -NAN : NAN;
        result.type = JsonNumber::FLOATING_POINT;
        return result;
    }
    else if (context.match_literal('n')) {
        context.expect_literal("an");
        result.fp = negative ? -NAN : NAN;
        result.type = JsonNumber::FLOATING_POINT;
        return result;
    }
    else if (context.match_literal('I') || context.match_literal('i')) {
        context.expect_literal("nf");
        result.fp = negative ? -INFINITY : INFINITY;
        result.type = JsonNumber::FLOATING_POINT;
        return result;
    }

    while (context && isdigit(*context)) {
        number += *context++;
    }

    if (context.match_literal('.')) {
        doublePrecision = true;
        number += '.';

        while (context && isdigit(*context)) {
            number += *context++;
        }
    }

    char sci = context ? *context : '\0';
    if (sci == 'e' || sci == 'E') {
        doublePrecision = true;
        number += *context++;

        char sign = context ? *context : '\0';
        if (sign == '+' || sign == '-') {
            number += *context++;
        }

        while (context && isdigit(*context)) {
            number += *context++;
        }
    }

    try {
        JML_TRACE_EXCEPTIONS(false);
        if (number.empty())
            context.exception("expected number");

        if (doublePrecision) {
            char * endptr = 0;
            errno = 0;
            result.fp = strtod(number.c_str(), &endptr);
            if (errno || endptr != number.c_str() + number.length())
                context.exception(ML::format("failed to convert '%s' to long long",
                                             number.c_str()));
            result.type = JsonNumber::FLOATING_POINT;
        } else if (negative) {
            char * endptr = 0;
            errno = 0;
            result.sgn = strtol(number.c_str(), &endptr, 10);
            if (errno || endptr != number.c_str() + number.length())
                context.exception(ML::format("failed to convert '%s' to long long",
                                             number.c_str()));
            result.type = JsonNumber::SIGNED_INT;
        } else {
            char * endptr = 0;
            errno = 0;
            result.uns = strtoull(number.c_str(), &endptr, 10);
            if (errno || endptr != number.c_str() + number.length())
                context.exception(ML::format("failed to convert '%s' to unsigned long long",
                                             number.c_str()));
            result.type = JsonNumber::UNSIGNED_INT;
        }
    } catch (const std::exception & exc) {
        context.exception("expected number");
    }

    return result;
}


/*****************************************************************************/
/* JSON PATH ENTRY                                                           */
/*****************************************************************************/

JsonPathEntry::
JsonPathEntry(int index)
    : index(index), keyStr(0), keyPtr(0), fieldNumber(0)
{
}
    
JsonPathEntry::
JsonPathEntry(const std::string & key)
    : index(-1), keyStr(new std::string(key)), keyPtr(keyStr->c_str()),
      fieldNumber(0)
{
}
    
JsonPathEntry::
JsonPathEntry(const char * keyPtr)
    : index(-1), keyStr(nullptr), keyPtr(keyPtr), fieldNumber(0)
{
}

JsonPathEntry::
JsonPathEntry(JsonPathEntry && other) noexcept
{
    *this = std::move(other);
}

JsonPathEntry &
JsonPathEntry::
operator = (JsonPathEntry && other) noexcept
{
    index = other.index;
    keyPtr = other.keyPtr;
    keyStr = other.keyStr;
    fieldNumber = other.fieldNumber;
    other.keyStr = nullptr;
    other.keyPtr = nullptr;
    return *this;
}

JsonPathEntry::
~JsonPathEntry()
{
    if (keyStr)
        delete keyStr;
}

std::string
JsonPathEntry::
fieldName() const
{
    return keyStr ? *keyStr : std::string(keyPtr);
}

const char *
JsonPathEntry::
fieldNamePtr() const
{
    return keyPtr;
}


/*****************************************************************************/
/* JSON PATH                                                                 */
/*****************************************************************************/

JsonPath::
JsonPath()
{
}

std::string
JsonPath::
print() const
{
    std::string result;
    for (auto & e: *this) {
        if (e.index == -1)
            result += "." + std::string(e.fieldName());
        else result += '[' + std::to_string(e.index) + ']';
    }
    return result;
}

std::string
JsonPath::
fieldName() const
{
    return this->back().fieldName();
}

const char *
JsonPath::
fieldNamePtr() const
{
    return this->back().fieldNamePtr();
}

void
JsonPath::
push(JsonPathEntry entry, int fieldNum)
{
    entry.fieldNumber = fieldNum;
    this->emplace_back(std::move(entry));
}

void
JsonPath::
replace(JsonPathEntry entry)
{
    int newFieldNumber = this->back().fieldNumber + 1;
    this->back() = std::move(entry);
    this->back().fieldNumber = newFieldNumber;
}

void
JsonPath::
pop()
{
    this->pop_back();
}


/*****************************************************************************/
/* JSON PARSING CONTEXT                                                      */
/*****************************************************************************/

JsonParsingContext::
JsonParsingContext()
{
    path.reset(new JsonPath());
}

JsonParsingContext::
~JsonParsingContext()
{
}

size_t
JsonParsingContext::
pathLength() const
{
    return path->size();
}

const JsonPathEntry &
JsonParsingContext::
pathEntry(int n) const
{
    return path->at(n);
}

std::string
JsonParsingContext::
printPath() const
{
    return path->print();
}

std::string
JsonParsingContext::
fieldName() const
{
    return path->fieldName();
}

const char *
JsonParsingContext::
fieldNamePtr() const
{
    return path->fieldNamePtr();
}

void
JsonParsingContext::
pushPath(JsonPathEntry entry, int memberNumber)
{
    path->push(std::move(entry), memberNumber);
}

void
JsonParsingContext::
replacePath(JsonPathEntry entry)
{
    path->replace(std::move(entry));
}

void
JsonParsingContext::
popPath()
{
    path->pop();
}

void
JsonParsingContext::
onUnknownField(const ValueDescription * desc)
{
    if (!onUnknownFieldHandlers.empty())
        onUnknownFieldHandlers.back()(desc);
    else {
        std::string typeNameStr = desc ? "parsing " + desc->typeName + " ": "";
        exception("unknown field " + typeNameStr + printPath());
    }
}



/*****************************************************************************/
/* STREAMING JSON PARSING CONTEXT                                            */
/*****************************************************************************/

StreamingJsonParsingContext::
StreamingJsonParsingContext()
    : context(nullptr)
{
}

StreamingJsonParsingContext::
StreamingJsonParsingContext(ML::Parse_Context & context)
    : context(nullptr)
{
    init(context);
}
    
StreamingJsonParsingContext::
StreamingJsonParsingContext(const std::string & filename)
    : context(nullptr)
{
    init(filename);
}
    
StreamingJsonParsingContext::
StreamingJsonParsingContext(const std::string & filename, std::istream & stream,
                            unsigned line, unsigned col,
                            size_t chunk_size)
    : context(nullptr)
{
    init(filename, stream, line, col, chunk_size);
}

StreamingJsonParsingContext::
StreamingJsonParsingContext(const std::string & filename, const char * start,
                            const char * finish, unsigned line, unsigned col)
    : context(nullptr)
{
    init(filename, start, finish, line, col);
}
    
StreamingJsonParsingContext::
StreamingJsonParsingContext(const std::string & filename, const char * start,
                            size_t length, unsigned line, unsigned col)
    : context(nullptr)
{
    init(filename, start, length, line, col);
}

StreamingJsonParsingContext::
~StreamingJsonParsingContext()
{
}

void
StreamingJsonParsingContext::
init(ML::Parse_Context & context)
{
    this->context = &context;
    ownedContext.reset();
}

void
StreamingJsonParsingContext::
init(const std::string & filename)
{
    ownedContext.reset(new ML::Parse_Context(filename));
    context = ownedContext.get();
}
    
void
StreamingJsonParsingContext::
init(const std::string & filename, const char * start,
     const char * finish, unsigned line, unsigned col)
{
    ownedContext.reset(new ML::Parse_Context(filename, start, finish, line, col));
    context = ownedContext.get();
}

void
StreamingJsonParsingContext::
init(const std::string & filename, const char * start,
     size_t length, unsigned line, unsigned col)
{
    ownedContext.reset(new ML::Parse_Context(filename, start, length, line, col));
    context = ownedContext.get();
}

void
StreamingJsonParsingContext::
init(const std::string & filename, std::istream & stream,
     unsigned line, unsigned col,
     size_t chunk_size)
{
    ownedContext.reset(new ML::Parse_Context(filename, stream, line, col, chunk_size));
    context = ownedContext.get();
}

void
StreamingJsonParsingContext::
forEachMember(const std::function<void ()> & fn)
{
    return forEachMember<std::function<void ()> >(fn);
}

void
StreamingJsonParsingContext::
forEachElement(const std::function<void ()> & fn)
{
    return forEachElement<std::function<void ()> >(fn);
}

void
StreamingJsonParsingContext::
skip()
{
    Datacratic::expectJson(*context);
}

int
StreamingJsonParsingContext::
expectInt()
{
    return context->expect_int();
}

unsigned int
StreamingJsonParsingContext::
expectUnsignedInt()
{
    return context->expect_unsigned();
}

long
StreamingJsonParsingContext::
expectLong()
{
    return context->expect_long();
}

unsigned long
StreamingJsonParsingContext::
expectUnsignedLong()
{
    return context->expect_unsigned_long();
}

long long
StreamingJsonParsingContext::
expectLongLong()
{
    return context->expect_long_long();
}

unsigned long long
StreamingJsonParsingContext::
expectUnsignedLongLong()
{
    return context->expect_unsigned_long_long();
}

float
StreamingJsonParsingContext::
expectFloat()
{
    return context->expect_float();
}

double
StreamingJsonParsingContext::
expectDouble()
{
    return context->expect_double();
}

bool
StreamingJsonParsingContext::
expectBool()
{
    return Datacratic::expectJsonBool(*context);
}

void
StreamingJsonParsingContext::
expectNull()
{
    context->expect_literal("null");
}

bool
StreamingJsonParsingContext::
matchUnsignedLongLong(unsigned long long & val)
{
    return context->match_unsigned_long_long(val);
}

bool
StreamingJsonParsingContext::
matchLongLong(long long & val)
{
    return context->match_long_long(val);
}

bool
StreamingJsonParsingContext::
matchDouble(double & val)
{
    return context->match_double(val);
}

std::string
StreamingJsonParsingContext::
expectStringAscii()
{
    return expectJsonStringAscii(*context);
}

ssize_t
StreamingJsonParsingContext::
expectStringAscii(char * value, size_t maxLen)
{
    return expectJsonStringAscii(*context, value, maxLen);
}

bool
StreamingJsonParsingContext::
isObject() const
{
    skipJsonWhitespace(*context);
    char c = *(*context);
    return c == '{';
}

bool
StreamingJsonParsingContext::
isString() const
{
    skipJsonWhitespace(*context);
    char c = *(*context);
    return c == '\"';
}

bool
StreamingJsonParsingContext::
isArray() const
{
    skipJsonWhitespace(*context);
    char c = *(*context);
    return c == '[';
}

bool
StreamingJsonParsingContext::
isBool() const
{
    skipJsonWhitespace(*context);
    char c = *(*context);
    return c == 't' || c == 'f';
        
}

bool
StreamingJsonParsingContext::
isInt() const
{
    skipJsonWhitespace(*context);

    // Find the offset at which an integer finishes
    size_t offset1;
    {
        ML::Parse_Context::Revert_Token token(*context);
        long long v;
        if (!context->match_long_long(v))
            return false;
        offset1 = context->get_offset();
    }

    // It's an integer only if a double only matches the same.
    // Otherwise it's surely a double.
    ML::Parse_Context::Revert_Token token(*context);
    double d;
    if (!context->match_double(d))
        return false;
    return context->get_offset() == offset1;
}
    
bool
StreamingJsonParsingContext::
isUnsigned() const
{
    skipJsonWhitespace(*context);

    // Find the offset at which an integer finishes
    size_t offset1;
    {
        ML::Parse_Context::Revert_Token token(*context);
        unsigned long long v;
        if (!context->match_unsigned_long_long(v))
            return false;
        offset1 = context->get_offset();
    }

    // It's an integer only if a double only matches the same.
    // Otherwise it's surely a double.
    ML::Parse_Context::Revert_Token token(*context);
    double d;
    if (!context->match_double(d))
        return false;
    return context->get_offset() == offset1;
}
    
bool
StreamingJsonParsingContext::
isNumber() const
{
    skipJsonWhitespace(*context);
    ML::Parse_Context::Revert_Token token(*context);
    double d;
    if (context->match_double(d))
        return true;
    return false;
}

bool
StreamingJsonParsingContext::
isNull() const
{
    skipJsonWhitespace(*context);
    ML::Parse_Context::Revert_Token token(*context);
    if (context->match_literal("null"))
        return true;
    return false;
}

#if 0    
bool
StreamingJsonParsingContext::
isNumber() const
{
    char c = *(*context);
    if (c >= '0' && c <= '9')
        return true;
    if (c == '.' || c == '+' || c == '-')
        return true;
    if (c == 'N' || c == 'I')  // NaN or Inf
        return true;
    return false;
}
#endif

void
StreamingJsonParsingContext::
exception(const std::string & message)
{
    context->exception("at " + printPath() + ": " + message);
}

std::string
StreamingJsonParsingContext::
getContext() const
{
    return context->where() + " at " + printPath();
}

Json::Value
StreamingJsonParsingContext::
expectJson()
{
    return Datacratic::expectJson(*context);
}

std::string
StreamingJsonParsingContext::
printCurrent()
{
    try {
        ML::Parse_Context::Revert_Token token(*context);
        return boost::trim_copy(expectJson().toString());
    } catch (const std::exception & exc) {
        ML::Parse_Context::Revert_Token token(*context);
        return context->expect_text("\n");
    }
}

ssize_t
StreamingJsonParsingContext::
expectStringUtf8(char * buffer, size_t maxLen)
{
    ML::Parse_Context::Revert_Token token(*context);

    skipJsonWhitespace((*context));
    context->expect_literal('"');

    size_t pos = 0;

    while (!context->match_literal('"')) {
        // We need up to 4 characters to add a new UTF-8 code point
        if (pos >= maxLen - 5) {
            return -1;
        }

        int c = *(*context);
        
        //cerr << "c = " << c << " " << (char)c << endl;

        if (c < 0 || c > 127) {
            // Unicode
            c = utf8::unchecked::next(*context);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;

            continue;
        }
        ++(*context);

        if (c == '\\') {
            c = *(*context)++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                int code = context->expect_hex4();
                c = code;
                break;
            }
            default:
                context->exception("invalid escaped char");
            }
        }

        if (c < ' ' || c >= 127) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;
        }
        else buffer[pos++] = c;
    }

    buffer[pos] = 0;

    token.ignore();

    return pos;
}

Utf8String
StreamingJsonParsingContext::
expectStringUtf8()
{
    skipJsonWhitespace((*context));
    context->expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    // Keep expanding until it fits
    while (!context->match_literal('"')) {
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

        int c = *(*context);
        
        //cerr << "c = " << c << " " << (char)c << endl;

        if (c < 0 || c > 127) {
            // Unicode
            c = utf8::unchecked::next(*context);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;

            continue;
        }
        ++(*context);

        if (c == '\\') {
            c = *(*context)++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                int code = context->expect_hex4();
                c = code;
                break;
            }
            default:
                context->exception("invalid escaped char");
            }
        }

        if (c < ' ' || c >= 127) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;
        }
        else buffer[pos++] = c;
    }

    Utf8String result(string(buffer, buffer + pos));
    if (buffer != internalBuffer)
        delete[] buffer;
    
    return result;
}

void
StreamingJsonParsingContext::
expectJsonObjectUtf8(const std::function<void (const char *, size_t)> & onEntry)
{
    Datacratic::skipJsonWhitespace(*context);

    if (context->match_literal("null"))
        return;

    context->expect_literal('{');

    Datacratic::skipJsonWhitespace(*context);

    if (context->match_literal('}')) return;

    for (;;) {
        Datacratic::skipJsonWhitespace(*context);

        char keyBuffer[1024];

        ssize_t done = expectStringUtf8(keyBuffer, 1024);
        if (done != -1) {
            skipJsonWhitespace(*context);

            context->expect_literal(':');

            skipJsonWhitespace(*context);

            onEntry(keyBuffer, done);

            skipJsonWhitespace(*context);

        }
        else {
            Utf8String name = expectStringUtf8();
            
            skipJsonWhitespace(*context);
            
            context->expect_literal(':');
            
            skipJsonWhitespace(*context);

            onEntry(name.rawData(), name.rawLength());
        }

        skipJsonWhitespace(*context);

        if (!context->match_literal(',')) break;
    }

    skipJsonWhitespace(*context);
    context->expect_literal('}');
}


Json::Value
expectJson(Parse_Context & context)
{
    context.skip_whitespace();
    if (*context == '"')
        return expectJsonStringAscii(context);
    else if (context.match_literal("null"))
        return Json::Value();
    else if (context.match_literal("true"))
        return Json::Value(true);
    else if (context.match_literal("false"))
        return Json::Value(false);
    else if (*context == '[') {
        Json::Value result(Json::arrayValue);
        expectJsonArray(context,
                        [&] (int i, Parse_Context & context)
                        {
                            result[i] = expectJson(context);
                        });
        return result;
    } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        expectJsonObject(context,
                         [&] (const std::string & key, Parse_Context & context)
                         {
                             result[key] = expectJson(context);
                         });
        return result;
    } else {
        JsonNumber number = expectJsonNumber(context);
        switch (number.type) {
        case JsonNumber::UNSIGNED_INT:
            return number.uns;
        case JsonNumber::SIGNED_INT:
            return number.sgn;
        case JsonNumber::FLOATING_POINT:
            return number.fp;
        default:
            throw ML::Exception("logic error in expectJson");
        }
    }
}

Json::Value
expectJsonAscii(Parse_Context & context)
{
    context.skip_whitespace();
    if (*context == '"')
        return expectJsonStringAscii(context);
    else if (context.match_literal("null"))
        return Json::Value();
    else if (context.match_literal("true"))
        return Json::Value(true);
    else if (context.match_literal("false"))
        return Json::Value(false);
    else if (*context == '[') {
        Json::Value result(Json::arrayValue);
        expectJsonArray(context,
                        [&] (int i, Parse_Context & context)
                        {
                            result[i] = expectJsonAscii(context);
                        });
        return result;
    } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        expectJsonObjectAscii(context,
                         [&] (const char * key, Parse_Context & context)
                         {
                             result[key] = expectJsonAscii(context);
                         });
        return result;
    } else {
        JsonNumber number = expectJsonNumber(context);
        switch (number.type) {
        case JsonNumber::UNSIGNED_INT:
            return number.uns;
        case JsonNumber::SIGNED_INT:
            return number.sgn;
        case JsonNumber::FLOATING_POINT:
            return number.fp;
        default:
            throw ML::Exception("logic error in expectJson");
        }
    }
}


/*****************************************************************************/
/* STRUCTURED JSON PARSING CONTEXT                                           */
/*****************************************************************************/

StructuredJsonParsingContext::
StructuredJsonParsingContext(const Json::Value & val)
    : current(&val), top(&val)
{
}

void
StructuredJsonParsingContext::
exception(const std::string & message)
{
    //using namespace std;
    //cerr << *current << endl;
    //cerr << *top << endl;
    throw ML::Exception("At path " + printPath() + ": "
                        + message + " parsing "
                        + boost::trim_copy(top->toString()));
}
    
std::string
StructuredJsonParsingContext::
getContext() const
{
    return printPath();
}

int
StructuredJsonParsingContext::
expectInt()
{
    return current->asInt();
}

unsigned int
StructuredJsonParsingContext::
expectUnsignedInt()
{
    return current->asUInt();
}

long
StructuredJsonParsingContext::
expectLong()
{
    return current->asInt();
}

unsigned long
StructuredJsonParsingContext::
expectUnsignedLong()
{
    return current->asUInt();
}

long long
StructuredJsonParsingContext::
expectLongLong()
{
    return current->asInt();
}

unsigned long long
StructuredJsonParsingContext::
expectUnsignedLongLong()
{
    return current->asUInt();
}

float
StructuredJsonParsingContext::
expectFloat()
{
    return current->asDouble();
}

double
StructuredJsonParsingContext::
expectDouble()
{
    return current->asDouble();
}

bool
StructuredJsonParsingContext::
expectBool()
{
    return current->asBool();
}

void
StructuredJsonParsingContext::
expectNull()
{
    if (!current->isNull())
        exception("expected null value");
}

bool
StructuredJsonParsingContext::
matchUnsignedLongLong(unsigned long long & val)
{
    if (current->isIntegral()) {
        val = current->asUInt();
        return true;
    }
    if (current->isNumeric()) {
        unsigned long long v = current->asDouble();
        if (v == current->asDouble()) {
            val = v;
            return true;
        }
    }
    return false;
}

bool
StructuredJsonParsingContext::
matchLongLong(long long & val)
{
    if (current->isIntegral()) {
        val = current->asInt();
        return true;
    }
    if (current->isNumeric()) {
        long long v = current->asDouble();
        if (v == current->asDouble()) {
            val = v;
            return true;
        }
    }
    return false;
}

bool
StructuredJsonParsingContext::
matchDouble(double & val)
{
    if (current->isNumeric()) {
        val = current->asDouble();
        return true;
    }
    return false;
}

std::string
StructuredJsonParsingContext::
expectStringAscii()
{
    return current->asString();
}

ssize_t
StructuredJsonParsingContext::
expectStringAscii(char * value, size_t maxLen)
{
    const std::string & strValue = current->asString();
    ssize_t realSize = strValue.size();
    if (realSize >= maxLen) {
        return -1;
    }
    memcpy(value, strValue.c_str(), realSize);
    value[realSize] = '\0';
    return realSize;
}

Utf8String
StructuredJsonParsingContext::
expectStringUtf8()
{
    return Utf8String(current->asString());
}

ssize_t
StructuredJsonParsingContext::
expectStringUtf8(char * value, size_t maxLen)
{
    const Utf8String & strValue = current->asStringUtf8();
    ssize_t realSize = strValue.rawLength();
    if (realSize >= maxLen) {
        return -1;
    }
    memcpy(value, strValue.rawData(), realSize);
    value[realSize] = '\0';
    return realSize;
}

Json::Value
StructuredJsonParsingContext::
expectJson()
{
    return *current;
}

bool
StructuredJsonParsingContext::
isObject() const
{
    return current->type() == Json::objectValue;
}

bool
StructuredJsonParsingContext::
isString() const
{
    return current->type() == Json::stringValue;
}

bool
StructuredJsonParsingContext::
isArray() const
{
    return current->type() == Json::arrayValue;
}

bool
StructuredJsonParsingContext::
isBool() const
{
    return current->type() == Json::booleanValue;
}

bool
StructuredJsonParsingContext::
isInt() const
{
    return current->isIntegral();
}

bool
StructuredJsonParsingContext::
isUnsigned() const
{
    return current->isUInt();
}

bool
StructuredJsonParsingContext::
isNumber() const
{
    return current->isNumeric();
}

bool
StructuredJsonParsingContext::
isNull() const
{
    return current->isNull();
}

void
StructuredJsonParsingContext::
skip()
{
}

void
StructuredJsonParsingContext::
forEachMember(const std::function<void ()> & fn)
{
    if (!isObject())
        exception("expected an object");

    const Json::Value * oldCurrent = current;
    int memberNum = 0;

    for (auto it = current->begin(), end = current->end();
         it != end;  ++it) {

        // This structure takes care of pushing and popping our
        // path entry.  It will make sure the member is always
        // popped no matter what
        struct PathPusher {
            PathPusher(const std::string & memberName,
                       int memberNum,
                       StructuredJsonParsingContext * context)
                : context(context)
            {
                context->pushPath(memberName, memberNum);
            }

            ~PathPusher()
            {
                context->popPath();
            }

            StructuredJsonParsingContext * const context;
        } pusher(it.memberName(), memberNum++, this);
            
        current = &(*it);
        fn();
    }
        
    current = oldCurrent;
}

void
StructuredJsonParsingContext::
forEachElement(const std::function<void ()> & fn)
{
    if (!isArray())
        exception("expected an array");

    const Json::Value * oldCurrent = current;

    for (unsigned i = 0;  i < oldCurrent->size();  ++i) {
        if (i == 0)
            pushPath(i);
        else replacePath(i);

        current = &(*oldCurrent)[i];

        fn();
    }

    if (oldCurrent->size() != 0)
        popPath();
        
    current = oldCurrent;
}

std::string
StructuredJsonParsingContext::
printCurrent()
{
    return boost::trim_copy(current->toString());
}


/*****************************************************************************/
/* STRING JSON PARSING CONTEXT                                               */
/*****************************************************************************/

StringJsonParsingContext::
StringJsonParsingContext(std::string str_,
                         const std::string & filename)
    : str(std::move(str_))
{
    init(filename, str.c_str(), str.c_str() + str.size());
}


}  // namespace Datacratic
