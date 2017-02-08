/* json_printing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Functionality to print JSON values.
*/

#include "mldb/base/exc_assert.h"

#include "json_printing.h"
#include "dtoa.h"
#include <cmath>
#include <iostream>
#include "mldb/ext/jsoncpp/value.h"


using namespace std;


namespace MLDB {

namespace {

static char * BUFFER_TOO_SMALL = reinterpret_cast<char *>(0);
static char * NO_ESCAPING = reinterpret_cast<char *>(1);

static char hexDigit(uint32_t c)
{
    if (c < 10)
        return '0' + c;
    else return 'a' + c - 10;
}

/** Escape JSON in an existing buffer.  Will return BUFFER_TOO_SMALL if the
    underlying buffer is too small.  Will return NO_ESCAPING 1 (as a char *)
    if the output was identical to the input, in other words no escaping was
    required.  Otherwise returns the pointer of the end of the buffer
    which will contain the JSON escaped version of the character.

    UTF-8 characters are passed through as their UTF-8 encoded equivalent.
*/
char * jsonEscapeCore(const char * str, size_t strLen, char * p, char * end)
{
    bool anyEscaped = false;
    for (unsigned i = 0;  i < strLen;  ++i) {
        if (p + 4 >= end)
            return BUFFER_TOO_SMALL;

        char c = str[i];
        if (c >= ' ' && c < 127 && c != '\"' && c != '\\')
            *p++ = c;
        else {
            anyEscaped = true;
            *p++ = '\\';
            switch (c) {
            case '\t': *p++ = ('t');  break;
            case '\n': *p++ = ('n');  break;
            case '\r': *p++ = ('r');  break;
            case '\f': *p++ = ('f');  break;
            case '\b': *p++ = ('b');  break;
            case '/':
            case '\\':
            case '\"': *p++ = (c);  break;
            default:
                if (c > 0 && c < 32) {
                    // ASCII control code
                    if (p + 6 >= end)
                        return BUFFER_TOO_SMALL;
                    *p++ = '\\';
                    *p++ = 'u';
                    *p++ = '0';
                    *p++ = '0';
                    *p++ = hexDigit((c >> 4) & 15);
                    *p++ = hexDigit((c >> 0) & 15);
                    break;
                }
                else if (c == 0) {
                    throw MLDB::Exception("JSON strings cannot contain null characters");
                }
                else {
                    for (auto & c: string(str, str + strLen))
                        cerr << "char " << (int)c << " " << c << endl;
                    throw MLDB::Exception("Invalid character in JSON string %d: %s", (int)c,
                                        str);
                }
            }
        }
    }

    return anyEscaped ? p : NO_ESCAPING;
}

static constexpr size_t MAX_STACK_CHARS = 16384;

} // file scope

bool isJsonValidAscii(char c)
{
    return (c > 0 && c < 127);
}

std::string
jsonEscape(const std::string & str)
{
    // No character can expand to more than two, so this should be
    // enough.
    size_t sz = str.size() * 2 + 4;

    if (sz <= MAX_STACK_CHARS) {
        char buf[sz];
        char * p = buf, * end = buf + sz;

        p = jsonEscapeCore(str.data(), str.length(), p, end);
        
        if (p == BUFFER_TOO_SMALL)
            throw MLDB::Exception("To fix: logic error in JSON escaping");
        else if (p == NO_ESCAPING)
            return str;
        return string(buf, p);
    }
    else {
        std::string heap_buf(sz, 0);
        char * p = (char *)heap_buf.data(), * end = p + sz;

        p = jsonEscapeCore(str.data(), str.length(), p, end);
        
        if (p == BUFFER_TOO_SMALL)
            throw MLDB::Exception("To fix: logic error in JSON escaping");
        else if (p == NO_ESCAPING)
            return str;
        heap_buf.resize(p - heap_buf.data());
        return heap_buf;
    }
}

void jsonEscape(const std::string & str, std::ostream & stream)
{
    // No character can expand to more than two, so this should be
    // enough.
    size_t sz = str.size() * 2 + 4;

    if (sz <= MAX_STACK_CHARS) {
        char buf[sz];
        char * p = buf, * end = buf + sz;

        p = jsonEscapeCore(str.data(), str.length(), p, end);

        if (p == BUFFER_TOO_SMALL)
            throw MLDB::Exception("To fix: logic error in JSON escaping");
        else if (p == NO_ESCAPING)
            p = buf + str.size();

        stream.write(buf, p - buf);
    }
    else {
        // We need an allocation anyway, so use the simple solution
        stream << jsonEscape(str);
    }
}

void jsonEscape(const char * str, size_t len, std::ostream & stream)
{
    // No character can expand to more than two, so this should be
    // enough.
    size_t sz = len * 2 + 4;

    if (sz <= MAX_STACK_CHARS) {
        char buf[sz];
        char * p = buf, * end = buf + sz;

        p = jsonEscapeCore(str, len, p, end);

        if (p == BUFFER_TOO_SMALL)
            throw MLDB::Exception("To fix: logic error in JSON escaping");
        else if (p == NO_ESCAPING)
            p = buf + len;

        stream.write(buf, p - buf);
    }
    else {
        // We need an allocation anyway, so use the simple solution
        stream << jsonEscape(string(str, str + len));
    }
}

void jsonEscape(const std::string & str, std::string & out)
{
    // No character can expand to more than two, so this should be
    // enough.
    size_t sz = str.size() * 2 + 4;

    if (sz <= MAX_STACK_CHARS) {
        char buf[sz];
        char * p = buf, * end = buf + sz;

        p = jsonEscapeCore(str.data(), str.length(), p, end);

        if (p == BUFFER_TOO_SMALL)
            throw MLDB::Exception("To fix: logic error in JSON escaping");
        else if (p == NO_ESCAPING)
            p = buf + str.size();

        out.append(buf, p - buf);
    } else {
        // We need an allocation anyway, so use the simple solution
        if (out.empty())
            out = jsonEscape(str);
        else out += jsonEscape(str);
    }
}

void jsonEscape(const char * str, size_t len, std::string & out)
{
    // No character can expand to more than two, so this should be
    // enough.
    size_t sz = len * 2 + 4;

    if (sz <= MAX_STACK_CHARS) {
        char buf[sz];
        char * p = buf, * end = buf + sz;

        p = jsonEscapeCore(str, len, p, end);

        if (p == BUFFER_TOO_SMALL)
            throw MLDB::Exception("To fix: logic error in JSON escaping");
        else if (p == NO_ESCAPING)
            p = buf + len;

        out.append(buf, p - buf);
    } else {
        // We need an allocation anyway, so use the simple solution
        if (out.empty())
            out = jsonEscape(string(str, str + len));
        else out += jsonEscape(string(str, str + len));
    }
}

void
StreamJsonPrintingContext::
writeStringUtf8(const Utf8String & s)
{
    stream << '\"';

    for (auto it = s.begin(), end = s.end();  it != end;  ++it) {
        int c = *it;
        if (c >= ' ' && c < 127 && c != '\"' && c != '\\')
            stream << (char)c;
        else {
            switch (c) {
            case '\0':
                throw MLDB::Exception("JSON strings may not contain embedded nulls");
            case '\t': stream << "\\t";  break;
            case '\n': stream << "\\n";  break;
            case '\r': stream << "\\r";  break;
            case '\b': stream << "\\b";  break;
            case '\f': stream << "\\f";  break;
            case '/':
            case '\\':
            case '\"': stream << '\\' << (char)c;  break;
            default:
                if (writeUtf8) {
                    char buf[4];
                    char * p = utf8::unchecked::append(c, buf);
                    stream.write(buf, p - buf);
                }
                else {
                    ExcAssert(c > 0 && c < 65536);
                    stream << MLDB::format("\\u%04x", (unsigned)c);
                }
            }
        }
    }
    
    stream << '\"';
}

void
StreamJsonPrintingContext::
writeStringUtf8(const char * p, size_t len)
{
    stream << '\"';

    typedef utf8::iterator<const char *> It;

    for (It it = It(p, p, p + len), end = It(p + len, p, p + len);  it != end;  ++it) {
        int c = *it;
        if (c >= ' ' && c < 127 && c != '\"' && c != '\\')
            stream << (char)c;
        else {
            switch (c) {
            case '\0':
                throw MLDB::Exception("JSON strings may not contain embedded nulls");
            case '\t': stream << "\\t";  break;
            case '\n': stream << "\\n";  break;
            case '\r': stream << "\\r";  break;
            case '\b': stream << "\\b";  break;
            case '\f': stream << "\\f";  break;
            case '/':
            case '\\':
            case '\"': stream << '\\' << (char)c;  break;
            default:
                if (writeUtf8) {
                    char buf[4];
                    char * p = utf8::unchecked::append(c, buf);
                    stream.write(buf, p - buf);
                }
                else {
                    ExcAssert(c > 0 && c < 65536);
                    stream << MLDB::format("\\u%04x", (unsigned)c);
                }
            }
        }
    }
    
    stream << '\"';
}


/*****************************************************************************/
/* STREAM JSON PRINTING CONTEXT                                              */
/*****************************************************************************/

StreamJsonPrintingContext::
StreamJsonPrintingContext(std::ostream & stream)
    : stream(stream), writeUtf8(true)
{
}

void
StreamJsonPrintingContext::
startObject()
{
    path.push_back(true /* isObject */);
    stream << "{";
}

void
StreamJsonPrintingContext::
startMember(const Utf8String & memberName)
{
    ExcAssert(path.back().isObject);
    //path.back().memberName = memberName;
    ++path.back().memberNum;
    if (path.back().memberNum != 0)
        stream << ",";
    writeStringUtf8(memberName);
    stream << ":";
}

void
StreamJsonPrintingContext::
startMember(const char * memberNameStr, size_t memberNameLen)
{
    ExcAssert(path.back().isObject);
    //path.back().memberName = memberName;
    ++path.back().memberNum;
    if (path.back().memberNum != 0)
        stream << ",";
    writeStringUtf8(memberNameStr, memberNameLen);
    stream << ":";
}

void
StreamJsonPrintingContext::
endObject()
{
    ExcAssert(path.back().isObject);
    path.pop_back();
    stream << "}";
}

void
StreamJsonPrintingContext::
startArray(int knownSize)
{
    path.push_back(false /* isObject */);
    stream << "[";
}

void
StreamJsonPrintingContext::
newArrayElement()
{
    ExcAssert(!path.back().isObject);
    ++path.back().memberNum;
    if (path.back().memberNum != 0)
        stream << ",";
}

void
StreamJsonPrintingContext::
endArray()
{
    ExcAssert(!path.back().isObject);
    path.pop_back();
    stream << "]";
}
    
void
StreamJsonPrintingContext::
skip()
{
    stream << "null";
}

void
StreamJsonPrintingContext::
writeNull()
{
    stream << "null";
}

void
StreamJsonPrintingContext::
writeInt(int i)
{
    stream << i;
}

void
StreamJsonPrintingContext::
writeUnsignedInt(unsigned int i)
{
    stream << i;
}

void
StreamJsonPrintingContext::
writeLong(long int i)
{
    stream << i;
}

void
StreamJsonPrintingContext::
writeUnsignedLong(unsigned long int i)
{
    stream << i;
}

void
StreamJsonPrintingContext::
writeLongLong(long long int i)
{
    stream << i;
}

void
StreamJsonPrintingContext::
writeUnsignedLongLong(unsigned long long int i)
{
    stream << i;
}

void
StreamJsonPrintingContext::
writeFloat(float f)
{
    if (std::isfinite(f))
        stream << MLDB::dtoa(f);
    else stream << "\"" << f << "\"";
}

void
StreamJsonPrintingContext::
writeDouble(double d)
{
    if (std::isfinite(d))
        stream << MLDB::dtoa(d);
    else stream << "\"" << d << "\"";
}

void
StreamJsonPrintingContext::
writeString(const std::string & s)
{
    stream << '\"';
    jsonEscape(s, stream);
    stream << '\"';
}

void
StreamJsonPrintingContext::
writeString(const char * p, size_t len)
{
    stream << '\"';
    jsonEscape(p, len, stream);
    stream << '\"';
}

void
StreamJsonPrintingContext::
writeJson(const Json::Value & val)
{
    stream << val.toStringNoNewLine();
}

void
StreamJsonPrintingContext::
writeBool(bool b)
{
    stream << (b ? "true": "false");
}



/*****************************************************************************/
/* STRING JSON PRINTING CONTEXT                                              */
/*****************************************************************************/

void
StringJsonPrintingContext::
write(char c)
{
    str += c;
}

void
StringJsonPrintingContext::
write(char c1, char c2)
{
    str += c1;
    str += c2;
}

void
StringJsonPrintingContext::
write(const char * s)
{
    str += s;
}

void
StringJsonPrintingContext::
write(const char * s, int len)
{
    str.append(s, len);
}

void
StringJsonPrintingContext::
write(const std::string & s)
{
    str.append(s);
}

void
StringJsonPrintingContext::
writeStringUtf8(const Utf8String & s)
{
    write('"');

    for (auto it = s.begin(), end = s.end();  it != end;  ++it) {
        wchar_t c = *it;
        if (c >= ' ' && c < 127 && c != '\"' && c != '\\')
            write((char)c);
        else {
            switch (c) {
            case '\t': write('\\', 't');  break;
            case '\n': write('\\', 'n');  break;
            case '\r': write('\\', 'r');  break;
            case '\b': write('\\', 'b');  break;
            case '\f': write('\\', 'f');  break;
            case '/':
            case '\\':
            case '\"': write('\\', (char)c);  break;
            default:
                if (writeUtf8) {
                    char buf[4];
                    char * p = utf8::unchecked::append(c, buf);
                    write(buf, p - buf);
                }
                else {
                    ExcAssert(c >= 0 && c < 65536);
                    write(MLDB::format("\\u%04x", (unsigned)c));
                }
            }
        }
    }
    
    write('"');
}

void
StringJsonPrintingContext::
writeStringUtf8(const char * p, size_t len)
{
    write('"');

    typedef utf8::iterator<const char *> It;

    for (It it = It(p, p, p + len), end = It(p + len, p, p + len);  it != end;  ++it) {
        wchar_t c = *it;
        if (c >= ' ' && c < 127 && c != '\"' && c != '\\')
            write((char)c);
        else {
            switch (c) {
            case '\t': write('\\', 't');  break;
            case '\n': write('\\', 'n');  break;
            case '\r': write('\\', 'r');  break;
            case '\b': write('\\', 'b');  break;
            case '\f': write('\\', 'f');  break;
            case '/':
            case '\\':
            case '\"': write('\\', (char)c);  break;
            default:
                if (writeUtf8) {
                    char buf[4];
                    char * p = utf8::unchecked::append(c, buf);
                    write(buf, p - buf);
                }
                else {
                    ExcAssert(c >= 0 && c < 65536);
                    write(MLDB::format("\\u%04x", (unsigned)c));
                }
            }
        }
    }
    
    write('"');
}

StringJsonPrintingContext::
StringJsonPrintingContext(std::string & str)
    : str(str), writeUtf8(true)
{
}

void
StringJsonPrintingContext::
startObject()
{
    path.push_back(true /* isObject */);
    write('{');
}

void
StringJsonPrintingContext::
startMember(const Utf8String & memberName)
{
    ExcAssert(path.back().isObject);
    //path.back().memberName = memberName;
    ++path.back().memberNum;
    if (path.back().memberNum != 0)
        write(',');
    writeStringUtf8(memberName);
    write(':');
}

void
StringJsonPrintingContext::
startMember(const char * memberNameStr, size_t memberNameLen)
{
    ExcAssert(path.back().isObject);
    //path.back().memberName = memberName;
    ++path.back().memberNum;
    if (path.back().memberNum != 0)
        write(',');
    writeStringUtf8(memberNameStr, memberNameLen);
    write(':');
}

void
StringJsonPrintingContext::
endObject()
{
    ExcAssert(path.back().isObject);
    path.pop_back();
    write('}');
}

void
StringJsonPrintingContext::
startArray(int knownSize)
{
    path.push_back(false /* isObject */);
    write('[');
}

void
StringJsonPrintingContext::
newArrayElement()
{
    ExcAssert(!path.back().isObject);
    ++path.back().memberNum;
    if (path.back().memberNum != 0)
        write(',');
}

void
StringJsonPrintingContext::
endArray()
{
    ExcAssert(!path.back().isObject);
    path.pop_back();
    write(']');
}
    
void
StringJsonPrintingContext::
skip()
{
    write("null");
}

void
StringJsonPrintingContext::
writeNull()
{
    write("null");
}

void
StringJsonPrintingContext::
writeInt(int i)
{
    char buffer[128];
    int chars = sprintf(buffer, "%i", i);
    write(buffer, chars);
}

void
StringJsonPrintingContext::
writeUnsignedInt(unsigned int i)
{
    char buffer[128];
    int chars = sprintf(buffer, "%u", i);
    write(buffer, chars);
}

void
StringJsonPrintingContext::
writeLong(long int i)
{
    char buffer[128];
    int chars = sprintf(buffer, "%li", i);
    write(buffer, chars);
}

void
StringJsonPrintingContext::
writeUnsignedLong(unsigned long int i)
{
    char buffer[128];
    int chars = sprintf(buffer, "%lu", i);
    write(buffer, chars);
}

void
StringJsonPrintingContext::
writeLongLong(long long int i)
{
    char buffer[128];
    int chars = sprintf(buffer, "%lli", i);
    write(buffer, chars);
}

void
StringJsonPrintingContext::
writeUnsignedLongLong(unsigned long long int i)
{
    char buffer[128];
    int chars = sprintf(buffer, "%llu", i);
    write(buffer, chars);
}

void
StringJsonPrintingContext::
writeFloat(float f)
{
    if (std::isfinite(f))
        str += MLDB::dtoa(f);
    else {
        write('"');
        write(std::to_string(f));
        write('"');
    }
}

void
StringJsonPrintingContext::
writeDouble(double d)
{
    if (std::isfinite(d))
        str += MLDB::dtoa(d);
    else {
        write('"');
        write(std::to_string(d));
        write('"');
    }
}

void
StringJsonPrintingContext::
writeString(const std::string & s)
{
    write('"');
    jsonEscape(s, str);
    write('"');
}

void
StringJsonPrintingContext::
writeString(const char * p, size_t len)
{
    write('"');
    jsonEscape(p, len, str);
    write('"');
}

void
StringJsonPrintingContext::
writeJson(const Json::Value & val)
{
    write(val.toStringNoNewLine());
}

void
StringJsonPrintingContext::
writeBool(bool b)
{
    write(b ? "true": "false");
}


/*****************************************************************************/
/* UTF8 STRING JSON PRINTING CONTEXT                                         */
/*****************************************************************************/

Utf8StringJsonPrintingContext::
Utf8StringJsonPrintingContext(Utf8String & str)
    : StringJsonPrintingContext(const_cast<std::string &>(str.rawString())),
      str(str)
{
}


/*****************************************************************************/
/* STRUCTURED JSON PRINTING CONTEXT                                          */
/*****************************************************************************/

StructuredJsonPrintingContext::
StructuredJsonPrintingContext(Json::Value & output)
    : output(output), current(&output)
{
    path.reserve(8);
}

void
StructuredJsonPrintingContext::
startObject()
{
    *current = Json::Value(Json::objectValue);
    path.push_back(current);
}

void
StructuredJsonPrintingContext::
startMember(const Utf8String & memberName)
{
    current = &(*path.back())[memberName];
}

void
StructuredJsonPrintingContext::
startMember(const char * memberNameStr, size_t memberNameLen)
{
    current = &(*path.back())[string(memberNameStr, memberNameStr + memberNameLen)];
}

void
StructuredJsonPrintingContext::
endObject()
{
    path.pop_back();
}

void
StructuredJsonPrintingContext::
startArray(int knownSize)
{
    *current = Json::Value(Json::arrayValue);
    path.push_back(current);
}

void
StructuredJsonPrintingContext::
newArrayElement()
{
    Json::Value & b = *path.back();
    current = &b[b.size()];
}

void
StructuredJsonPrintingContext::
endArray()
{
    path.pop_back();
}
    
void
StructuredJsonPrintingContext::
skip()
{
    *current = Json::Value();
}

void
StructuredJsonPrintingContext::
writeNull()
{
    *current = Json::Value();
}

void
StructuredJsonPrintingContext::
writeInt(int i)
{
    *current = i;
}

void
StructuredJsonPrintingContext::
writeUnsignedInt(unsigned int i)
{
    *current = i;
}

void
StructuredJsonPrintingContext::
writeLong(long int i)
{
    *current = i;
}

void
StructuredJsonPrintingContext::
writeUnsignedLong(unsigned long int i)
{
    *current = i;
}

void
StructuredJsonPrintingContext::
writeLongLong(long long int i)
{
    *current = i;
}

void
StructuredJsonPrintingContext::
writeUnsignedLongLong(unsigned long long int i)
{
    *current = i;
}

void
StructuredJsonPrintingContext::
writeFloat(float f)
{
    *current = f;
}

void
StructuredJsonPrintingContext::
writeDouble(double d)
{
    *current = d;
}

void
StructuredJsonPrintingContext::
writeString(const std::string & s)
{
    *current = s;
}

void
StructuredJsonPrintingContext::
writeString(const char * p, size_t len)
{
    *current = string(p, p + len);
}

void
StructuredJsonPrintingContext::
writeStringUtf8(const Utf8String & s)
{
    *current = s;
}

void
StructuredJsonPrintingContext::
writeStringUtf8(const char * p, size_t len)
{
    *current = Utf8String(p, p + len);
}

void
StructuredJsonPrintingContext::
writeJson(const Json::Value & val)
{
    *current = val;
}

void
StructuredJsonPrintingContext::
writeBool(bool b)
{
    *current = b;
}


} // namespace MLDB
