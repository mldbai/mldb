// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_parsing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

*/

#include "json_parsing.h"
#include "json_parsing_impl.h"
#include "string.h"
#include "value_description.h"
#include "mldb/base/parse_context.h"
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/types/string.h"
#include "mldb/base/scope.h"
#include "mldb/utils/safe_clamp.h"
#include <errno.h>

using namespace std;


namespace {

static constexpr const size_t MAX_JSON_NESTING = 1000;

/* This is a duplicate of MLDB::trim, reimplemented here in order to avoid a
 * circular dependency between libvalue_description and libutils. */

string
trim(const string & other)
{
    size_t len = other.size();

    size_t start;
    for (start = 0; start < len; start++) {
        if (other[start] != ' ' && other[start] != '\t') {
            break;
        }
    }

    size_t end;
    for (end = len; end > start; end--) {
        if (other[end-1] != ' ' && other[end-1] != '\t') {
            break;
        }
    }

    if (start == 0 && end == len) {
        return other;
    }

    string result;
    if (start != end) {
        result = other.substr(start, end-start);
    }

    return result;
}

}


namespace MLDB {


bool expectJsonBool(ParseContext & context)
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
JsonNumber expectJsonNumber(ParseContext & context);

/** Match a JSON number. */
bool matchJsonNumber(ParseContext & context, JsonNumber & num);

/*****************************************************************************/
/* JSON NUMBER                                                               */
/*****************************************************************************/

Json::Value JsonNumber::toJson() const
{
    switch (type) {
    case JsonNumber::UNSIGNED_INT:
        return uns;
    case JsonNumber::SIGNED_INT:
        return sgn;
    case JsonNumber::FLOATING_POINT:
        return fp;
    default:
        throw MLDB::Exception("logic error in expectJson");
    }
}

bool JsonNumber::isExactUnsigned() const
{
    switch (type) {
    case JsonNumber::UNSIGNED_INT:
        return true;
    case JsonNumber::SIGNED_INT:
        return sgn >= 0;
    case JsonNumber::FLOATING_POINT:
        return safe_clamp<uint64_t>(fp) == fp;
    default:
        MLDB_THROW_LOGIC_ERROR();
    }

}

bool JsonNumber::isExactSigned() const
{
    switch (type) {
    case JsonNumber::UNSIGNED_INT:
        return sgn <= std::numeric_limits<int64_t>::max();
    case JsonNumber::SIGNED_INT:
        return true;
    case JsonNumber::FLOATING_POINT:
        return safe_clamp<int64_t>(fp) == fp;
    default:
        MLDB_THROW_LOGIC_ERROR();
    }
}

bool JsonNumber::isNegative() const
{
    switch (type) {
    case JsonNumber::UNSIGNED_INT:
        return false;
    case JsonNumber::SIGNED_INT:
        return std::signbit(sgn);
    case JsonNumber::FLOATING_POINT:
        return std::signbit(fp);
    default:
        MLDB_THROW_LOGIC_ERROR();
    }
}


/*****************************************************************************/
/* JSON UTILITIES                                                            */
/*****************************************************************************/

bool skipJsonWhitespace(ParseContext & context)
{
    // Fast-path for the usual case for not EOF and no whitespace
    if (MLDB_LIKELY(!context.eof())) {
        char c = *context;
        if (c > ' ') {
            return false;
        }
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r')
            return false;
    }

    bool result = false;
    while (!context.eof()) {
        if (*context == '\n')
            result = true;

        if (context.match_whitespace() || context.match_eol())
            continue;
        break;
    }

    ExcAssert(context.eof() || !isspace(*context));

    return result;
}

bool matchJsonString(ParseContext & context, std::string & str)
{
    ParseContext::Revert_Token token(context);

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

std::string expectJsonStringAsciiPermissive(ParseContext & context, char sub)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;
    Scope_Exit(if (buffer != internalBuffer) delete[] buffer);

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
    return result;
}

ssize_t expectJsonStringAscii(ParseContext & context, char * buffer, size_t maxLength)
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

std::string expectJsonStringAscii(ParseContext & context)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;
    Scope_Exit(if (buffer != internalBuffer) delete[] buffer);

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
    return result;
}

int getEscapedJsonCharacterPointUtf8(ParseContext & context)
{
    int c = *context++;

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

        if (code >= 0xd800 && code <= 0xdfff) {
            // Take the surrogate pair
            int high = code - 0xd800;
                    
            context.expect_literal("\\u");
            int low = context.expect_hex4();
                    
            if (low < 0xdc00 || low > 0xdfff) {
                context.exception("invalid UTF-16 surrogate pair in JSON string");
            }
            c = high << 10 | (low - 0xdc00);
        }
        else {
            c = code;
        }
        break;
    }
    default:
        context.exception("invalid escaped char");
    }

    return c;
}

Utf8String expectJsonStringUtf8(ParseContext & context)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;
    Scope_Exit(if (buffer != internalBuffer) delete[] buffer);

    // Keep expanding until it fits
    while (!context.match_literal('"')) {
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
    return result;
}

bool
matchJsonNull(ParseContext & context)
{
    skipJsonWhitespace(context);
    return context.match_literal("null");
}

struct JsonNestingTracker {
    static thread_local int nesting;
    JsonNestingTracker()
    {
        ++nesting;
        if (nesting == MAX_JSON_NESTING) {
            throw MLDB::Exception("Maximum JSON nesting exceeded");
        }
    }

    ~JsonNestingTracker() noexcept(false)
    {
        --nesting;
        ExcAssertGreaterEqual(nesting, 0);
    }
};

thread_local int JsonNestingTracker::nesting = 0;

void
expectJsonArray(ParseContext & context,
                const std::function<void (int, ParseContext &)> & onEntry)
{
    JsonNestingTracker tracker;
    
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
expectJsonObject(ParseContext & context,
                 const std::function<void (const std::string &, ParseContext &)> & onEntry)
{
    JsonNestingTracker tracker;

    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return;

    context.expect_literal('{');

    skipJsonWhitespace(context);

    if (context.match_literal('}')) return;

    for (;;) {
        skipJsonWhitespace(context);

        string key = expectJsonStringUtf8(context).rawString();

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
expectJsonObjectAscii(ParseContext & context,
                      const std::function<void (const char *, ParseContext &)> & onEntry)
{
    JsonNestingTracker tracker;

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
matchJsonObject(ParseContext & context,
                const std::function<bool (const std::string &, ParseContext &)> & onEntry)
{
    JsonNestingTracker tracker;

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

JsonNumber expectJsonNumber(ParseContext & context)
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
        if (number.empty() || (number.size() == 1 && number[0] == '.')) {
            context.exception("Expected number before exponential");
        }
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

    auto parseAsDouble = [&] ()
        {
            char * endptr = 0;
            errno = 0;
            result.fp = strtod(number.c_str(), &endptr);
            if ((errno && errno != ERANGE) || endptr != number.c_str() + number.length())
                context.exception(MLDB::format("failed to convert '%s' to double",
                                             number.c_str()));
            result.type = JsonNumber::FLOATING_POINT;
        };

    try {
        MLDB_TRACE_EXCEPTIONS(false);
        if (number.empty())
            context.exception("expected number");

        if (doublePrecision) {
            parseAsDouble();
        } else if (negative) {
            char * endptr = 0;
            errno = 0;
            result.sgn = strtoll(number.c_str(), &endptr, 10);
            if (errno == ERANGE && endptr == number.c_str() + number.length()) {
                parseAsDouble();
            }
            else if (errno || endptr != number.c_str() + number.length()) {
                context.exception(MLDB::format("failed to convert '%s' to long long",
                                               number.c_str()));
            }
            result.type = JsonNumber::SIGNED_INT;
        } else {
            char * endptr = 0;
            errno = 0;
            result.uns = strtoull(number.c_str(), &endptr, 10);
            if (errno == ERANGE && endptr == number.c_str() + number.length()) {
                parseAsDouble();
            }
            else if (errno || endptr != number.c_str() + number.length())
                context.exception(MLDB::format("failed to convert '%s' to unsigned long long",
                                               number.c_str()));
            result.type = JsonNumber::UNSIGNED_INT;
        }
    } catch (const std::exception & exc) {
        context.exception("expected number");
    }

    return result;
}

bool matchJsonNumber(ParseContext & context, JsonNumber & result)
{
    result = JsonNumber();
    ParseContext::Revert_Token token(context);

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
        if (!context.match_literal("aN"))
            return false;
        result.fp = negative ? -NAN : NAN;
        result.type = JsonNumber::FLOATING_POINT;
        token.ignore();
        return true;
    }
    else if (context.match_literal('n')) {
        if (!context.match_literal("an"))
            return false;
        result.fp = negative ? -NAN : NAN;
        result.type = JsonNumber::FLOATING_POINT;
        token.ignore();
        return true;
    }
    else if (context.match_literal('I') || context.match_literal('i')) {
        if (!context.match_literal("nf"))
            return false;
        result.fp = negative ? -INFINITY : INFINITY;
        result.type = JsonNumber::FLOATING_POINT;
        token.ignore();
        return true;
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
        if (number.empty() || (number.size() == 1 && number[0] == '.')) {
            return false;
        }
        doublePrecision = true;
        if (!context)
            return false;
        if (!context)
            return false;
        number += *context++;

        char sign = context ? *context : '\0';
        if (sign == '+' || sign == '-') {
            if (!context)
                return false;
            number += *context++;
        }

        while (context && isdigit(*context)) {
            number += *context++;
        }
    }

    auto parseAsDouble = [&] () -> bool
        {
            char * endptr = 0;
            errno = 0;
            result.fp = strtod(number.c_str(), &endptr);
            if ((errno && errno != ERANGE) || endptr != number.c_str() + number.length())
                return false;
            result.type = JsonNumber::FLOATING_POINT;
            token.ignore();
            return true;
        };

    if (number.empty())
        return false;

    if (doublePrecision) {
        return parseAsDouble();
    } else if (negative) {
        char * endptr = 0;
        errno = 0;
        result.sgn = strtoll(number.c_str(), &endptr, 10);
        if (errno == ERANGE && endptr == number.c_str() + number.length()) {
            return parseAsDouble();
        }
        else if (errno || endptr != number.c_str() + number.length()) {
            return false;
        }
        result.type = JsonNumber::SIGNED_INT;
    } else {
        char * endptr = 0;
        errno = 0;
        result.uns = strtoull(number.c_str(), &endptr, 10);
        if (errno == ERANGE && endptr == number.c_str() + number.length()) {
            return parseAsDouble();
        }
        else if (errno || endptr != number.c_str() + number.length())
            return false;
        result.type = JsonNumber::UNSIGNED_INT;
    }

    token.ignore();
    return true;
}

/*****************************************************************************/
/* JSON PATH ENTRY                                                           */
/*****************************************************************************/

JsonPathEntry::
JsonPathEntry(int index)
    : index(index), keyStr(0), keyPtr(0), keyLength(0), fieldNumber(0)
{
}
    
JsonPathEntry::
JsonPathEntry(std::string key)
    : index(-1), keyStr(new std::string(std::move(key))), keyPtr(keyStr->c_str()),
      keyLength(keyStr->size()), fieldNumber(0)
{
}
    
JsonPathEntry::
JsonPathEntry(std::string_view keyView)
    : index(-1), keyStr(nullptr), keyPtr(keyView.data()), keyLength(keyView.length()),
      fieldNumber(0)
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
    keyLength = other.keyLength;
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
    return keyStr ? *keyStr : string(fieldNameView());
}

std::string_view
JsonPathEntry::
fieldNameView() const
{
    return {keyPtr, keyLength};
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
print(bool includeLeadingDot) const
{
    std::string result;
    for (auto & e: *this) {
        if (e.index == -1) {
            if (includeLeadingDot || !result.empty())
                result += "." + std::string(e.fieldName());
            else result = e.fieldName();
        }
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

std::string_view
JsonPath::
fieldNameView() const
{
    return this->back().fieldNameView();
}

int
JsonPath::
fieldNumber() const
{
    return this->back().fieldNumber;
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
printPath(bool includeLeadingDot) const
{
    return path->print(includeLeadingDot);
}

std::string
JsonParsingContext::
fieldName() const
{
    return path->fieldName();
}

std::string_view
JsonParsingContext::
fieldNameView() const
{
    return path->fieldNameView();
}

int
JsonParsingContext::
fieldNumber() const
{
    return path->fieldNumber();
}

bool
JsonParsingContext::
inField(const char * fieldName)
{
    return fieldNameView() == string_view(fieldName, strlen(fieldName));
}

bool
JsonParsingContext::
inField(const std::string & fieldName)
{
    return fieldNameView() == string_view(fieldName);
}

bool
JsonParsingContext::
inField(const Utf8String & fieldName)
{
    return fieldNameView() == fieldName.rawView();
}


void
JsonParsingContext::
pushPath(JsonPathEntry entry, int memberNumber)
{
    path->push(std::move(entry), memberNumber);
    if (path->size() > MAX_JSON_NESTING)
        exception("JSON nested too deep");
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

void
JsonParsingContext::
expectEof() const
{
    if (!eof())
        exception("unexpected characters at end of input");
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
StreamingJsonParsingContext(ParseContext & context)
    : context(nullptr)
{
    init(context);
}
    
StreamingJsonParsingContext::
StreamingJsonParsingContext(const Utf8String & filename)
    : context(nullptr)
{
    init(filename);
}
    
StreamingJsonParsingContext::
StreamingJsonParsingContext(const Utf8String & filename, std::istream & stream,
                            unsigned line, unsigned col,
                            size_t chunk_size)
    : context(nullptr)
{
    init(filename, stream, line, col, chunk_size);
}

StreamingJsonParsingContext::
StreamingJsonParsingContext(const Utf8String & filename, const char * start,
                            const char * finish, unsigned line, unsigned col)
    : context(nullptr)
{
    init(filename, start, finish, line, col);
}
    
StreamingJsonParsingContext::
StreamingJsonParsingContext(const Utf8String & filename, const char * start,
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
init(ParseContext & context)
{
    this->context = &context;
    ownedContext.reset();
}

void
StreamingJsonParsingContext::
init(const Utf8String & filename)
{
    ownedContext.reset(new ParseContext(filename));
    context = ownedContext.get();
}
    
void
StreamingJsonParsingContext::
init(const Utf8String & filename, const char * start,
     const char * finish, unsigned line, unsigned col)
{
    ownedContext.reset(new ParseContext(filename, start, finish, line, col));
    context = ownedContext.get();
}

void
StreamingJsonParsingContext::
init(const Utf8String & filename, const char * start,
     size_t length, unsigned line, unsigned col)
{
    ownedContext.reset(new ParseContext(filename, start, length, line, col));
    context = ownedContext.get();
}

void
StreamingJsonParsingContext::
init(const Utf8String & filename, std::istream & stream,
     unsigned line, unsigned col,
     size_t chunk_size)
{
    ownedContext.reset(new ParseContext(filename, stream, line, col, chunk_size));
    context = ownedContext.get();
}

void
StreamingJsonParsingContext::
skipJsonWhitespace() const
{
    bool newNewlines = MLDB::skipJsonWhitespace(*context);
    hasEmbeddedNewlines = hasEmbeddedNewlines || newNewlines;
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
    MLDB::expectJson(*context);
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
    return MLDB::expectJsonBool(*context);
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
    skipJsonWhitespace();
    char c = *(*context);
    return c == '{';
}

bool
StreamingJsonParsingContext::
isString() const
{
    skipJsonWhitespace();
    char c = *(*context);
    return c == '\"';
}

bool
StreamingJsonParsingContext::
isArray() const
{
    skipJsonWhitespace();
    char c = *(*context);
    return c == '[';
}

bool
StreamingJsonParsingContext::
isBool() const
{
    skipJsonWhitespace();
    char c = *(*context);
    return c == 't' || c == 'f';
        
}

bool
StreamingJsonParsingContext::
isInt() const
{
    skipJsonWhitespace();

    // Short circuit for EOF or not a digit or negative sign
    if (!context || (!isdigit(*context) && (char(*context) != '-')))
        return false;

    // Find the offset at which an integer finishes
    size_t offset1;
    {
        ParseContext::Revert_Token token(*context);
        long long v;
        if (!context->match_long_long(v))
            return false;
        offset1 = context->get_offset();
    }

    // It's an integer only if a double only matches the same.
    // Otherwise it's surely a double.
    ParseContext::Revert_Token token(*context);
    double d;
    if (!context->match_double(d))
        return false;
    return context->get_offset() == offset1;
}
    
bool
StreamingJsonParsingContext::
isUnsigned() const
{
    skipJsonWhitespace();

    // Find the offset at which an integer finishes
    size_t offset1;
    {
        ParseContext::Revert_Token token(*context);
        unsigned long long v;
        if (!context->match_unsigned_long_long(v))
            return false;
        offset1 = context->get_offset();
    }

    // It's an integer only if a double only matches the same.
    // Otherwise it's surely a double.
    ParseContext::Revert_Token token(*context);
    double d;
    if (!context->match_double(d))
        return false;
    return context->get_offset() == offset1;
}
    
bool
StreamingJsonParsingContext::
isNumber() const
{
    skipJsonWhitespace();
    ParseContext::Revert_Token token(*context);
    double d;
    if (context->match_double(d))
        return true;
    return false;
}

JsonNumber
StreamingJsonParsingContext::
expectNumber()
{
    return expectJsonNumber(*context);
}

bool
StreamingJsonParsingContext::
isNull() const
{
    skipJsonWhitespace();
    ParseContext::Revert_Token token(*context);
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
exception(const Utf8String & message) const
{
    context->exception("at " + printPath() + ": " + message);
}

Utf8String
StreamingJsonParsingContext::
getContext() const
{
    return context->where() + " at " + printPath();
}

Json::Value
StreamingJsonParsingContext::
expectJson()
{
    return MLDB::expectJson(*context);
}

std::string
StreamingJsonParsingContext::
printCurrent()
{
    try {
        ParseContext::Revert_Token token(*context);
        return trim(expectJson().toString());
    } catch (const std::exception & exc) {
        ParseContext::Revert_Token token(*context);
        return context->expect_text("\n");
    }
}

ssize_t
StreamingJsonParsingContext::
expectStringUtf8(char * buffer, size_t maxLen)
{
    ParseContext::Revert_Token token(*context);

    skipJsonWhitespace();
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
            c = context->expect_utf8_code_point();

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;

            continue;
        }

        ++(*context);

        if (c == '\\') {
            c = getEscapedJsonCharacterPointUtf8(*context);
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
    return MLDB::expectJsonStringUtf8(*context);
}

void
StreamingJsonParsingContext::
expectJsonObjectUtf8(const std::function<void (std::string_view)> & onEntry)
{
    skipJsonWhitespace();

    if (context->match_literal("null"))
        return;

    context->expect_literal('{');

    skipJsonWhitespace();

    if (context->match_literal('}')) return;

    for (;;) {
        skipJsonWhitespace();

        char keyBuffer[1024];
        ssize_t done = expectStringUtf8(keyBuffer, 1024);

        if (done != -1) {
            skipJsonWhitespace();

            context->expect_literal(':');

            skipJsonWhitespace();

            onEntry(string_view{keyBuffer, (size_t)done});

            skipJsonWhitespace();
        }
        else {
            Utf8String name = expectStringUtf8();
            
            skipJsonWhitespace();
            
            context->expect_literal(':');
            
            skipJsonWhitespace();

            onEntry(string_view{name.rawData(), name.rawLength()});
        }

        skipJsonWhitespace();

        if (!context->match_literal(',')) break;
    }

    skipJsonWhitespace();
    context->expect_literal('}');
}

bool
StreamingJsonParsingContext::
eof() const
{
    skipJsonWhitespace();
    return context->eof();
}

std::any
StreamingJsonParsingContext::
savePosition()
{
    auto token = std::make_shared<ParseContext::Rewind_Token>(*context);
    return token;
}

void
StreamingJsonParsingContext::
restorePosition(const std::any & atoken)
{
    cerr << "streaming token type is " << demangle(atoken.type().name()) << endl;
    auto token = std::any_cast<std::shared_ptr<ParseContext::Rewind_Token>>(atoken);
    ExcAssert(token);
    token->apply();
}

Json::Value
expectJson(ParseContext & context)
{
    skipJsonWhitespace(context);
    if (*context == '"')
        return expectJsonStringUtf8(context);
    else if (context.match_literal("null"))
        return Json::Value();
    else if (context.match_literal("true"))
        return Json::Value(true);
    else if (context.match_literal("false"))
        return Json::Value(false);
    else if (*context == '[') {
        Json::Value result(Json::arrayValue);
        expectJsonArray(context,
                        [&] (int i, ParseContext & context)
                        {
                            result[i] = expectJson(context);
                        });
        return result;
    } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        expectJsonObject(context,
                         [&] (const std::string & key, ParseContext & context)
                         {
                             result[key] = expectJson(context);
                         });
        return result;
    } else {
        JsonNumber number = expectJsonNumber(context);
        return number.toJson();
    }
}

// Returns true if there was a valid record, or false if there wasn't
bool skipJson(ParseContext & context);

bool skipJsonNumber(ParseContext & context)
{
    if (context.match_literal('-')) {
    }

    if (context.match_literal('N')) {
        if (!context.match_literal("aN"))
            return false;
        return true;
    }
    else if (context.match_literal('n')) {
        if (!context.match_literal("an"))
            return false;
        return true;
    }
    else if (context.match_literal('I') || context.match_literal('i')) {
        if (!context.match_literal("nf"))
            return false;
        return true;
    }

    bool hasDigit = false;
    while (context && isdigit(*context)) {
        context++;
        hasDigit = true;
    }

    if (context.match_literal('.')) {
        while (context && isdigit(*context)) {
            context++;
            hasDigit = true;
        }
    }

    if (!hasDigit)
        return false;

    char sci = context ? *context : '\0';
    if (sci == 'e' || sci == 'E') {
        context++;

        char sign = context ? *context : '\0';
        if (sign == '+' || sign == '-') {
            context++;
        }

        while (context && isdigit(*context)) {
            context++;
        }
    }

    return true;
}

inline bool skipEscapedJsonCharacterPointUtf8(ParseContext & context)
{
    if (!context)
        return false;

    int c = *context++;

    switch (c) {
    case 't':
    case 'n':
    case 'r':
    case 'f':
    case 'b':
    case '/':
    case '\\':
    case '"':
        return true;
    case 'u':
        return context.match_hex4(c);
    default:
        return false;
    }

    return true;
    return c;
}

bool skipJsonStringUtf8(ParseContext & context)
{
    skipJsonWhitespace(context);
    if (!context.match_literal('"'))
        return false;

    for (;;) {
        if (!context)
            return false;
        if (context.match_literal('"'))
            return true;

        int c = *context;
        if (c < 0 || c > 127) {
            // Unicode
            context.expect_utf8_code_point();
            continue;
        }
        ++context;

        if (c == '\\') {
            if (!skipEscapedJsonCharacterPointUtf8(context))
                return false;
        }
    }
}

bool skipJsonArray(ParseContext & context)
{
    skipJsonWhitespace(context);
    if (context.match_literal("null"))
        return true;
    if (!context.match_literal('['))
        return false;
    skipJsonWhitespace(context);
    if (context.match_literal(']')) return true;

    for (;;) {
        skipJsonWhitespace(context);
        if (!skipJson(context))
            return false;
        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    return context.match_literal(']');
}

bool
skipJsonObject(ParseContext & context)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return true;

    if (!context.match_literal('{'))
        return false;

    skipJsonWhitespace(context);

    if (context.match_literal('}')) return true;

    for (;;) {
        skipJsonWhitespace(context);
        if (!skipJsonStringUtf8(context))
            return false;
        skipJsonWhitespace(context);
        if (!context.match_literal(':'))
            return false;
        skipJsonWhitespace(context);
        if (!skipJson(context))
            return false;
        skipJsonWhitespace(context);
        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    return context.match_literal('}');
}

bool
skipJson(ParseContext & context)
{
    skipJsonWhitespace(context);
    if (!context)
        return false;
    if (*context == '"')
        return skipJsonStringUtf8(context);
    else if (context.match_literal("null"))
        return true;
    else if (context.match_literal("true"))
        return true;
    else if (context.match_literal("false"))
        return true;
    else if (*context == '[') {
        return skipJsonArray(context);
    } else if (*context == '{') {
        return skipJsonObject(context);
    } else {
        return skipJsonNumber(context);
    }
}

Json::Value
expectJsonAscii(ParseContext & context)
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
                        [&] (int i, ParseContext & context)
                        {
                            result[i] = expectJsonAscii(context);
                        });
        return result;
    } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        expectJsonObjectAscii(context,
                         [&] (const char * key, ParseContext & context)
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
            throw MLDB::Exception("logic error in expectJson");
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
reset(const Json::Value & val)
{
    current = &val;
    top = &val;
    path.reset(new JsonPath());
}

void
StructuredJsonParsingContext::
exception(const Utf8String & message) const
{
    //using namespace std;
    //cerr << *current << endl;
    //cerr << *top << endl;
    throw MLDB::Exception("At path " + printPath() + ": "
                          + message + " parsing "
                          + trim(top->toString()));
}
    
Utf8String
StructuredJsonParsingContext::
getContext() const
{
    return printPath();
}

int
StructuredJsonParsingContext::
expectInt()
{
    if (current->isNull() || !current->isConvertibleTo(Json::intValue))
        exception("Integer was expected; instead got '" + current->toStringNoNewLine() + "'");
    return current->asInt();
}

unsigned int
StructuredJsonParsingContext::
expectUnsignedInt()
{
    if (current->isNull() || !current->isConvertibleTo(Json::uintValue))
        exception("Unsigned integer was expected; instead got '" + current->toStringNoNewLine() + "'");
    return current->asUInt();
}

long
StructuredJsonParsingContext::
expectLong()
{
    if (current->isNull() || !current->isConvertibleTo(Json::intValue))
        exception("Integer was expected; instead got '" + current->toStringNoNewLine() + "'");
    return current->asInt();
}

unsigned long
StructuredJsonParsingContext::
expectUnsignedLong()
{
    if (current->isNull() || !current->isConvertibleTo(Json::uintValue))
        exception("Unsigned integer was expected; instead got '" + current->toStringNoNewLine() + "'");
    return current->asUInt();
}

long long
StructuredJsonParsingContext::
expectLongLong()
{
    if (current->isNull() || !current->isConvertibleTo(Json::intValue))
        exception("Integer was expected; instead got '" + current->toStringNoNewLine() + "'");
    return current->asInt();
}

unsigned long long
StructuredJsonParsingContext::
expectUnsignedLongLong()
{
    if (current->isNull() || !current->isConvertibleTo(Json::uintValue))
        exception("Unsigned integer was expected; instead got '" + current->toStringNoNewLine() + "'");
    return current->asUInt();
}

float
StructuredJsonParsingContext::
expectFloat()
{
    if (current->isNull())
        exception("NULL found where floating point value expected");
    return current->asDouble();
}

double
StructuredJsonParsingContext::
expectDouble()
{
    if (current->isNull())
        exception("NULL found where floating point value expected");
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
    return current->isIntegral() && current->isConvertibleTo(Json::intValue);
}

bool
StructuredJsonParsingContext::
isUnsigned() const
{
    return current->isIntegral() && current->isConvertibleTo(Json::uintValue);
}

bool
StructuredJsonParsingContext::
isNumber() const
{
    return current->isNumeric();
}

JsonNumber
StructuredJsonParsingContext::
expectNumber()
{
    JsonNumber result;
    if (current->isInt()) {
        result.type = JsonNumber::SIGNED_INT;
        result.sgn = current->asInt();
    }
    else if (current->isUInt()) {
        result.type = JsonNumber::UNSIGNED_INT;
        result.uns = current->asUInt();
    }
    else {
        result.type = JsonNumber::FLOATING_POINT;
        result.fp = current->asDouble();
    }
    return result;
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
        
    current = oldCurrent == top ? nullptr: oldCurrent;
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

    if (oldCurrent->size() != 0) {
        popPath();
        current = oldCurrent;
    }
    else current = nullptr;
}

std::string
StructuredJsonParsingContext::
printCurrent()
{
    return trim(current->toString());
}

bool
StructuredJsonParsingContext::
eof() const
{
    return current == nullptr;
}

std::any
StructuredJsonParsingContext::
savePosition()
{
    return std::tuple<const Json::Value *, const Json::Value *>(top, current);
}

void
StructuredJsonParsingContext::
restorePosition(const std::any & atoken)
{
    cerr << "structured token type is " << demangle(atoken.type().name()) << endl;
    auto [newTop, newCurrent] = std::any_cast<std::tuple<const Json::Value *, const Json::Value *>>(atoken);
    ExcAssertEqual(top, newTop);
    current = newCurrent;
}


/*****************************************************************************/
/* STRING JSON PARSING CONTEXT                                               */
/*****************************************************************************/

StringJsonParsingContext::
StringJsonParsingContext(Utf8String str_,
                         const Utf8String & filename)
    : str(str_.stealRawString())
{
    init(filename, str.c_str(), str.c_str() + str.size());
}

StringJsonParsingContext::
StringJsonParsingContext(std::string str_,
                         const Utf8String & filename)
    : str(std::move(str_))
{
    init(filename, str.c_str(), str.c_str() + str.size());
}

StringJsonParsingContext::
StringJsonParsingContext(const char * str,
                         size_t len,
                         const Utf8String & filename)
{
    init(filename, str, len);
}


/*****************************************************************************/
/* UTF8 STRING JSON PARSING CONTEXT                                          */
/*****************************************************************************/

Utf8StringJsonParsingContext::
Utf8StringJsonParsingContext(Utf8String str_,
                             const std::string & filename)
    : str(new Utf8String(std::move(str_)))
{
    init(filename, str->rawData(), str->rawData() + str->rawLength());
}

Utf8StringJsonParsingContext::
Utf8StringJsonParsingContext(const char * str,
                             size_t len,
                             const std::string & filename)
{
    init(filename, str, len);
}

Utf8StringJsonParsingContext::
~Utf8StringJsonParsingContext()
{
}

}  // namespace MLDB
