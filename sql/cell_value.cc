/** cell_value.cc
    Jeremy Barnes, 24 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "cell_value.h"
#include "mldb/ext/highwayhash.h"
#include "mldb/utils/json_utils.h"
#include "mldb/types/dtoa.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/itoa.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/endian.h"
#include "cell_value_impl.h"
#include "mldb/base/parse_context.h"
#include "mldb/compiler/compiler.h"
#include "interval.h"
#include <math.h>
#include "mldb/types/path.h"
#include "mldb/ext/s2/s2.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/base/less.h"
#include "mldb/jml/utils/hex_dump.h"


using namespace std;

namespace MLDB {

using ML::highest_bit;

namespace {

/* Compact encodings for integers, designed for fast decoding due to the first
   byte uniquely determining how long the encoding is.

   byte1     extra     min     max  max32bit   
   0 xxxxxxx     0    0     2^7-1
   10 xxxxxx     1    2^7   2^14-1
   110 xxxxx     2    2^14  2^21-1
   1110 xxxx     3    2^21  2^28-1
   11110 xxx     4    2^28  2^35-1  (2^32-1)
   111110 xx     5    2^35  2^42-1
   1111110 x     6    2^42  2^49-1
   11111110      7    2^49  2^56-1
   11111111      8    2^56  2^64-1
*/

int compactEncodeLength(unsigned long long val)
{
    int highest = highest_bit(val);
    int idx = highest / 7;
    int len = idx + 1;
    return len;
}

void compactEncode(char * & first, char * last, unsigned long long val)
{
    /* Length depends upon highest bit / 7 */
    int highest = highest_bit(val);
    int idx = highest / 7;
    int len = idx + 1;

    if (first + len > last)
        throw MLDB::Exception("not enough space to encode compact_size_t");

    /* Pack it into the back bytes. */
    for (int i = len-1;  i >= 0;  --i) {
        //cerr << "i = " << i << endl;
        first[i] = val & 0xff;
        val >>= 8;
    }

    /* Add the indicator to the first byte. */
    uint32_t indicator = ~((1 << (8-idx)) - 1);
    first[0] |= indicator;

    first += len;
}

int compactDecodeLength(char firstChar)
{
    uint8_t marker = firstChar;
    // no bits set=-1, so len=9 as reqd
    int len = 8 - highest_bit((char)~marker);
    return len;
}

unsigned long long compactDecode(const char * & first, const char * last)
{
    /* Find the first zero bit in the marker.  We do this by bit flipping
       and finding the first 1 bit in the result. */
    if (first >= last)
        throw Exception("not enough bytes to decode compact_size_t");
        
    int len = compactDecodeLength(*first);
    if (first + len > last)
        throw Exception("not enough bytes to decode compact_size_t");

    /* Construct our value from the bytes. */
    unsigned long long result = 0;
    for (int i = 0;  i < len;  ++i) {
        int val = first[i];
        if (val < 0) val += 256;

        result <<= 8;
        result |= val; 
        //cerr << "i " << i << " result " << result << endl;
    }

    /* Filter off the top bits, which told us the length. */
    if (len == 9) ;
    else {
        int bits = len * 7;
        //cerr << "bits = " << bits << endl;
        result &= ((1ULL << bits)-1);
        //cerr << "result = " << result << endl;
    }

    first += len;

    return result;
}

} // file scope

/*****************************************************************************/
/* CELL VALUE                                                                */
/*****************************************************************************/

CellValue::
CellValue(const Path & path)
{
    // Optimized path for when we have just one simple element
    if (path.size() == 1) {

        const char * p;
        size_t l;
        std::tie(p, l) = path.getStringView(0);

        strLength = l;
        strFlags = 1;  // length of path

        if (strLength <= INTERNAL_LENGTH) {
            std::copy(p, p + strLength, shortString);
            type = ST_SHORT_PATH;
        }
        else {
            // NOTE: once the malloc has succeeded, the rest is
            // noexcept.
            void * mem = malloc(sizeof(StringRepr) + strLength);
            if (!mem)
                throw std::bad_alloc();

            longString = new (mem) StringRepr;  // placement new noexcept
            std::copy(p, p + strLength, longString->repr); // copy char noexcept
            type = ST_LONG_PATH;
        }

        return;
    }

    if (path.size() == 0) {
        type = ST_SHORT_PATH;
        strLength = 0;
        strFlags = 0;
        return;
    }

    Utf8String u = path.toUtf8String();

    strLength = u.rawLength();
    strFlags = path.size() > 15 ? 15 : path.size();

    if (strLength <= INTERNAL_LENGTH) {
        std::copy(u.rawData(), u.rawData() + strLength, shortString);
        type = ST_SHORT_PATH;
    }
    else {
        void * mem = malloc(sizeof(StringRepr) + strLength);
        longString = new (mem) StringRepr;
        std::copy(u.rawData(), u.rawData() + strLength, longString->repr);
        type = ST_LONG_PATH;
    }
}

CellValue
CellValue::
fromMonthDaySecond( int64_t months, int64_t days, double seconds)
{
    int negative = 0;
    if (months < 0) {
        negative += 1;
        months = -months;
    }
    if (days < 0) {
        negative += 1;
        days = -days;
    }
    if (signbit(seconds)) {
        negative += 1;
        seconds = -seconds;
    }

    CellValue value;
    value.type = ST_TIMEINTERVAL;
    value.timeInterval.months = months;
    value.timeInterval.days = days;
    value.timeInterval.seconds = copysign(seconds, negative ? -1.0 : 1.0);

    if (negative > 1) {
        throw HttpReturnException(400, "Cannot create interval where more than one of months, days and seconds is negative",
                                  "months", months, "days", days, "seconds", seconds);
    }
    if (value.timeInterval.months != months) {
        throw HttpReturnException(400, "Months for time interval was out of range of -65535 to 65535",
                                  "months", months);
    }
    if (value.timeInterval.days != days) {
        throw HttpReturnException(400, "Days for time interval was out of range of -65535 to 65535",
                                  "days", days);
    }

    return value;
}

CellValue
CellValue::
blob(std::string blobContents)
{
    CellValue result;
    result.initBlob(blobContents.data(), blobContents.length());
    return result;
}

CellValue
CellValue::
blob(const char * mem, size_t sz)
{
    CellValue result;
    result.initBlob(mem, sz);
    return result;
}

void
CellValue::
initStringFromAscii(const char * val, size_t len, bool check)
{
    initString(val, len, false, check);
}

void
CellValue::
initStringFromUtf8(const char * val, size_t len, bool check)
{
    initString(val, len, true, check);
}

void
CellValue::
initString(const char * stringValue, size_t len, bool isUtf8, bool check)
{
    char * s = (char *)stringValue;
    char * e = s + len;

    e = s + len;
    strLength = len;

    bool invalidChar = hasNonAsciiChar(s, strLength) ;
    if (len <= INTERNAL_LENGTH) {
        if(invalidChar) {
            if (!isUtf8)
                throw HttpReturnException(400, "UTF-8 character detected in ASCII string");
            if (check) {
                const char * end = utf8::find_invalid(stringValue, stringValue + len);
                if (end != stringValue + len)
                    throw MLDB::Exception("Invalid sequence within utf-8 string");
            }
            type = ST_UTF8_SHORT_STRING;
        }
        else {
            type = ST_ASCII_SHORT_STRING;
        }
        std::copy(s, e, shortString);
    }
    else {
        if(invalidChar) {
            if (!isUtf8)
                throw HttpReturnException(400, "UTF-8 character detected in ASCII string");
            if (check) {
                const char * end = utf8::find_invalid(stringValue, stringValue + len);
                if (end != stringValue + len)
                    throw MLDB::Exception("Invalid sequence within utf-8 string");
            }

            type = ST_UTF8_LONG_STRING;
        }
        else {
            type = ST_ASCII_LONG_STRING;
        }
        void * mem = malloc(sizeof(StringRepr) + strLength + 1);
        longString = new (mem) StringRepr;
        std::copy(s, e, longString->repr);
        longString->repr[strLength + 1] = 0;
    }
}

void
CellValue::
initBlob(const char * data, size_t len)
{
    strLength = len;
    if (len <= INTERNAL_LENGTH) {
        std::copy(data, data + len, shortString);
        type = ST_SHORT_BLOB;
    }
    else {
        void * mem = malloc(sizeof(StringRepr) + len);
        longString = new (mem) StringRepr;
        std::copy(data, data + len, longString->repr);
        type = ST_LONG_BLOB;
    }
}

CellValue::
CellValue(const CellValue & other)
    : bits1(other.bits1), bits2(other.bits2), flags(other.flags)
{
    if (other.type == ST_ASCII_LONG_STRING
        || other.type == ST_UTF8_LONG_STRING
        || other.type == ST_LONG_BLOB
        || other.type == ST_LONG_PATH) {
        void * mem = malloc(sizeof(StringRepr) + other.strLength + 1);
        longString = new (mem) StringRepr;
        std::copy(other.longString->repr, other.longString->repr + strLength,
                  longString->repr);
        longString->repr[strLength] = 0;
    }
}

CellValue
CellValue::
parse(const std::string & str)
{
    return parse(str.data(), str.length(), STRING_UNKNOWN);
}

CellValue
CellValue::
parse(const Utf8String & str)
{
    return parse(str.rawData(), str.rawLength(), STRING_UNKNOWN);
}

/**
   This implementation is slow but correct.  If this becomes the 
   bottleneck of some scenarios (e.g. importing large csv file) consider
   rewriting it without a copy of the buffer and by combining the three
   independent parsing cases into one.  Note that any implementation
   should be able to detect correctly overflow and underflow.  Also, it
   should minimize rounding errors as it is done in strtod implementation.
   See for example https://fossies.org/dox/glibc-2.24/strtod__l_8c_source.html
   from the glic for an overview of the challenges.
*/
CellValue
CellValue::
parse(const char * s_, size_t len, StringCharacteristics characteristics)
{
    if (len == 0)
        return CellValue();

    // this ensures that our buffer is null terminated as required below
    PossiblyDynamicBuffer<char, 256> sv(len + 1);
    memcpy(sv.data(), s_, len);
    auto s = sv.data();
    s[len] = 0;

    // First try as an int
    char * e = s + len;
    int64_t intVal = strtoll(s, &e, 10);

    if (e == s + len) {
        return CellValue(intVal);
    }
    
    // TODO: only need this one if the length is long enough... optimization
    e = s + len;
    uint64_t uintVal = strtoull(s, &e, 10);

    if (e == s + len) {
        return CellValue(uintVal);
    }
    
    e = s + len;
    double floatVal = strtod(s, &e);

    if (e == s + len) {
        return CellValue(floatVal);
    }

    return CellValue(s, len, characteristics);
}

CellValue::CellType
CellValue::
cellType() const
{
    switch (type) {
    case ST_EMPTY:
        return EMPTY;
    case ST_INTEGER:
    case ST_UNSIGNED:
        return INTEGER;
    case ST_FLOAT:
        return FLOAT;
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
        return ASCII_STRING;
    case ST_UTF8_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
        return UTF8_STRING;
    case ST_TIMESTAMP:
        return TIMESTAMP;    
    case ST_TIMEINTERVAL:
        return TIMEINTERVAL;
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
        return BLOB;
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return PATH;
    }

    throw HttpReturnException(400, "unknown CellValue type");
}

Utf8String
CellValue::
toUtf8String() const
{
    switch (type) {
    case ST_UTF8_SHORT_STRING:
    case ST_ASCII_SHORT_STRING:
    case ST_SHORT_BLOB:
    case ST_SHORT_PATH:
        return Utf8String((const char *)shortString, (size_t)strLength);
    case ST_UTF8_LONG_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_LONG_BLOB:
    case ST_LONG_PATH:
        try {
            return Utf8String(longString->repr, (size_t)strLength);
        } catch (...) {
            //for (unsigned i = 0;  i < strLength;  ++i) {
            //    cerr << "char at index " << i << " of " << strLength << " is "
            //         << (int)longString->repr[i] << endl;
            //}
            throw;
        }
    default:
        return toString();
    }
}

std::basic_string<char32_t>
CellValue::
toWideString() const
{
    switch (type) {
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
        return std::basic_string<char32_t>(stringChars(), stringChars() + strLength);
        
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING: {
        Utf8String str(stringChars(), (size_t)strLength);
        return std::basic_string<char32_t>(str.begin(), str.end());
    }
    default: {
        std::string str = toString();
        return std::basic_string<char32_t>(str.begin(), str.end());
    }
    }
}

std::string
CellValue::
toString() const
{
    switch (type) {
    case ST_EMPTY:
        return "";
    case ST_INTEGER:
        return itoa(intVal);
    case ST_UNSIGNED:
        return itoa(uintVal);
    case ST_FLOAT: {
        return dtoa(floatVal);
    }
    case ST_ASCII_SHORT_STRING:
        return string(shortString, shortString + strLength);
    case ST_ASCII_LONG_STRING:
        return string(longString->repr, longString->repr + strLength);
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
        throw HttpReturnException(400, "Can't convert value '" + trimmedExceptionString() + "' of type '"
                            + to_string(cellType()) + "' to ASCII string");
    case ST_TIMESTAMP:
        return Date::fromSecondsSinceEpoch(timestamp)
            .printIso8601(-1 /* as many digits as necessary */);
    case ST_TIMEINTERVAL:
        return printInterval();
    case ST_SHORT_PATH:
        return string(shortString, shortString + strLength);
    case ST_LONG_PATH:
        return string(longString->repr, longString->repr + strLength);
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
        throw HttpReturnException(400, "Cannot call toString() on a blob");
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
}

double
CellValue::
toDoubleImpl() const
{
    switch (type) {
    case ST_INTEGER:
        return intVal;
    case ST_UNSIGNED:
        return uintVal;
    case ST_FLOAT:
        return floatVal;
    default:
        throw HttpReturnException(400, "Can't convert value '" + trimmedExceptionString() + "' of type '"
                                    + to_string(cellType()) + "' to double");
    }
}

int64_t
CellValue::
toInt() const
{
    if (type != ST_INTEGER) {
        throw HttpReturnException(400, "Can't convert value '" + trimmedExceptionString() + "' of type '"
                                  + to_string(cellType()) + "' to integer");
    }
    return intVal;
}

uint64_t
CellValue::
toUInt() const
{
    if (type == ST_INTEGER && intVal >= 0) {
        return intVal;
    }
    else if (type == ST_UNSIGNED) {
        return uintVal;
    }
        throw HttpReturnException(400, "Can't convert value '" + trimmedExceptionString() + "' of type '"
                                  + to_string(cellType()) + "' to unsigned integer");
}

Date
CellValue::
toTimestamp() const
{
    if (type != ST_TIMESTAMP)
        throw HttpReturnException(400, "Can't convert value '" + trimmedExceptionString() + "' of type '"+
                                        to_string(cellType()) +"' to timestamp", "value", *this);
    return Date::fromSecondsSinceEpoch(timestamp);
}

std::tuple<int64_t, int64_t, double>
CellValue::
toMonthDaySecond() const
{
    if (type != ST_TIMEINTERVAL) {
        throw HttpReturnException(400, "Can't convert value '" + trimmedExceptionString() + "' of type '"+ 
                                        to_string(cellType()) +"' to time interval", "value", *this);
    }
    return make_tuple(timeInterval.months, timeInterval.days, timeInterval.seconds);
}

bool
CellValue::
isInt64() const
{
    return type == ST_INTEGER;
}

bool
CellValue::
isUInt64() const
{
    return (type == ST_INTEGER && intVal >= 0)
        || type == ST_UNSIGNED;
}

CellValue
CellValue::
coerceToInteger() const
{
    switch (type) {
    case ST_EMPTY:
    case ST_INTEGER:
    case ST_UNSIGNED:
        return *this;

    case ST_FLOAT:
        if (std::isnan(floatVal))
            return CellValue();
        if (floatVal < 0)
            return (int64_t)(floatVal);
        return (uint64_t)(floatVal);

    case ST_TIMESTAMP:
        if (!isfinite(timestamp))
            return CellValue();
        return timestamp;

    default: {
        // Strings
        CellValue v = parse(toUtf8String());
        if (v.isNumber())
            return v.coerceToInteger();
        return CellValue();
    }
        
    }
}

CellValue
CellValue::
coerceToNumber() const
{
    switch (type) {
    case ST_EMPTY:
    case ST_INTEGER:
    case ST_UNSIGNED:
    case ST_FLOAT:
        return *this;

    case ST_TIMESTAMP:
        return timestamp;

    default: {
        // Strings
        CellValue v = parse(toUtf8String());
        if (v.isNumber())
            return v.coerceToNumber();
        return CellValue();
    }
        
    }
}

CellValue
CellValue::
coerceToString() const
{
    if (type == ST_EMPTY)
        return CellValue();
    return toUtf8String();
}

CellValue
CellValue::
coerceToBoolean() const
{
    if (type == ST_EMPTY)
        return CellValue();
    if (isString()) {
        if (toStringLength() == 4
            && strncasecmp(stringChars(), "true", 4) == 0)
            return true;
        if (toStringLength() == 5
            && strncasecmp(stringChars(), "false", 5) == 0)
            return false;
        auto n = coerceToNumber();
        if (!n.empty())
            return n.asBool();
        return CellValue();
    }
    if (type == ST_TIMESTAMP)
        return isfinite(timestamp);
    return asBool();
}

CellValue
CellValue::
coerceToTimestamp() const
{
    if (type == ST_EMPTY)
        return Date::notADate();
    if (isAsciiString()) {
        return Date::parseIso8601DateTime(toString());
    }
    if (type == ST_TIMESTAMP)
        return *this;
    if (isNumber()) {
        return Date::fromSecondsSinceEpoch(toDouble());
    }
    return CellValue();
}

Date
CellValue::
mustCoerceToTimestamp() const
{
    if (type == ST_EMPTY)
        return Date::notADate();
    if (isAsciiString()) {
        return Date::parseIso8601DateTime(toString());
    }
    if (type == ST_TIMESTAMP)
        return toTimestamp();
    if (isNumber()) {
        return Date::fromSecondsSinceEpoch(toDouble());
    }
    throw HttpReturnException(400, "Couldn't convert value '" + toUtf8String()
                              + "' to timestamp",
                              "value", *this);
}

CellValue
CellValue::
coerceToBlob() const
{
    if (type == ST_EMPTY)
        return CellValue();
    else if (isBlob())
        return *this;
    else if (isString()) {
        return CellValue::blob(toUtf8String().stealRawString());
    }
    return toUtf8String();
}

PathElement
CellValue::
coerceToPathElement() const
{
    if (type == ST_EMPTY)
        return PathElement();
    else return PathElement(toUtf8String());
}

Path
CellValue::
coerceToPath() const
{
    if (type == ST_EMPTY)
        return Path();
    else if (type == ST_SHORT_PATH || type == ST_LONG_PATH) {
        if (strFlags == 1) {
            // Single-element; fast path
            PathBuilder builder;
            return builder.add(stringChars(), toStringLength()).extract();
        }
        else if (strFlags == 0) {
            return Path();
        }
        return Path::parse(stringChars(), toStringLength());
    }
    else if (type == ST_ASCII_SHORT_STRING || type == ST_ASCII_LONG_STRING
             || type == ST_UTF8_SHORT_STRING || type == ST_UTF8_LONG_STRING) {
        PathBuilder builder;
        return builder.add(stringChars(), toStringLength()).extract();
    }
    else return Path(toUtf8String());
}

bool
CellValue::
asBool() const
{
    switch (type) {
    case ST_EMPTY:
        return false;
    case ST_INTEGER:
        return intVal;
    case ST_UNSIGNED:
        return uintVal;
    case ST_FLOAT:
        return floatVal;
    case ST_TIMESTAMP:
        return isfinite(timestamp);
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return strLength;
    case ST_TIMEINTERVAL:
        return timeInterval.months || timeInterval.days || timeInterval.seconds;
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
}

bool
CellValue::
isNumber() const
{
    switch (type) {
    case ST_EMPTY:
        return false;
    case ST_INTEGER:
        return true;
    case ST_UNSIGNED:
        return true;
    case ST_FLOAT:
        return true;
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
        return false;
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
        return false;
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return false;
    }

    throw HttpReturnException(400, "unknown CellValue type");
}

bool
CellValue::
isPositiveNumber() const
{
    switch (type) {
    case ST_EMPTY:
        return false;
    case ST_INTEGER:
        return intVal > 0;
    case ST_UNSIGNED:
        return uintVal > 0;
    case ST_FLOAT:
        return !std::isnan(floatVal) && floatVal > 0;
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return false;
    }

    throw HttpReturnException(400, "unknown CellValue type");
}

bool
CellValue::
isNegativeNumber() const
{
    switch (type) {
    case ST_EMPTY:
        return false;
    case ST_INTEGER:
        return intVal < 0;
    case ST_UNSIGNED:
        return false;
    case ST_FLOAT:
        return !std::isnan(floatVal) && floatVal < 0;
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return false;
    }

    throw HttpReturnException(400, "unknown CellValue type");
}

bool
CellValue::
isFalse() const
{
    switch (type) {
    case ST_EMPTY:
        return false;
    case ST_INTEGER:
        return !intVal;
    case ST_UNSIGNED:
        return !!uintVal;
    case ST_FLOAT:
        return !floatVal;
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return !strLength;
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
        return false;
    }
    throw HttpReturnException(400, "unknown CellValue type");
}

const HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

template<typename T>
static uint64_t highwayhash_bin(const T & v, const HashSeed & key)
{
    char c[sizeof(v)];
    std::memcpy(c, &v, sizeof(v));
    return highwayHash(key.u64, c, sizeof(v));
}

CellValueHash
CellValue::
hash() const
{
    switch (type) {
    case ST_ASCII_SHORT_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_SHORT_BLOB:
        return CellValueHash(highwayHash(defaultSeedStable.u64, shortString, strLength));
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_BLOB:
        if (!longString->hash) {
            longString->hash = highwayHash(defaultSeedStable.u64, longString->repr, strLength);
        }
        return CellValueHash(longString->hash);
    case ST_TIMEINTERVAL:
        return CellValueHash(highwayHash(defaultSeedStable.u64, shortString, 12));
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return CellValueHash(coerceToPath().hash());
    default:
        if (bits2 != 0)
            cerr << "hashing " << jsonEncodeStr(*this) << endl;
        ExcAssertEqual(bits2, 0);
        return CellValueHash(highwayhash_bin(bits1, defaultSeedStable));
    }
}

int
CellValue::
compare(const CellValue & other) const
{

    return (*this < other) ? -1 : (*this == other) ? 0 : 1;
}

bool
CellValue::
operator == (const CellValue & other) const
{
    if (other.type != type) {
        return false;
    }

    switch (type) {
    case ST_EMPTY:
        return true;
    case ST_INTEGER:
        return toInt() == other.toInt();
    case ST_UNSIGNED:
        return toUInt() == other.toUInt();
    case ST_FLOAT:
        return std::memcmp(&floatVal, &other.floatVal, sizeof(floatVal)) == 0;
    case ST_ASCII_SHORT_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_SHORT_BLOB:
    case ST_SHORT_PATH:
        return strLength == other.strLength
            && strFlags == other.strFlags
            && strncmp(shortString, other.shortString, strLength) == 0;
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_BLOB:
    case ST_LONG_PATH:
        return strLength == other.strLength
            && strncmp(longString->repr, other.longString->repr, strLength) == 0;
    case ST_TIMESTAMP:
        return toTimestamp() == other.toTimestamp();
    case ST_TIMEINTERVAL: {
        bool nan1 = std::isnan(timeInterval.seconds);
        bool nan2 = std::isnan(other.timeInterval.seconds);

        if (nan1 || nan2) {
            return timeInterval.months == other.timeInterval.months && 
                timeInterval.days == other.timeInterval.days && 
                nan1 == nan2;
        }
        else {
            return timeInterval.months == other.timeInterval.months && 
                timeInterval.days == other.timeInterval.days && 
                timeInterval.seconds == other.timeInterval.seconds;
        }
    }
        
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
}

bool
CellValue::
operator <  (const CellValue & other) const
{
    // Sort order:
    // 1.  EMPTY
    // 2.  INTEGER or FLOAT, compared numerically with NaN first
    // 3.  STRING or BLOB, compared lexicographically
    // 4.  Rest of types

    try {
        if (MLDB_UNLIKELY(flags == other.flags && bits1 == other.bits1 && bits2 == other.bits2))
            return false;
 
        switch (type) {

        case ST_EMPTY:
            if (other.type == ST_EMPTY)
                return false;  // equal
            return true;  // empty goes before everything

        case ST_INTEGER: {
            if (other.type == ST_EMPTY)
                return false;
            if (other.isString())
                return true;
            if (other.type == ST_INTEGER)
                return toInt() < other.toInt();
            if (other.type == ST_UNSIGNED)
                return true;
            if (other.type == ST_FLOAT) {
                double d = other.toDouble();
                if (std::isnan(d))
                    return false;
                return toInt() < d;
            }
            return type < other.type;
        }
        case ST_UNSIGNED: {
            if (other.type == ST_EMPTY)
                return false;
            if (other.isString())
                return true;
            if (other.type == ST_INTEGER)
                return false;  // ST_UNSIGNED represents out of range positive integers
            if (other.type == ST_UNSIGNED)
                return toUInt() < other.toUInt();
            if (other.type == ST_FLOAT) {
                double d = other.toDouble();
                if (std::isnan(d))
                    return false;

                return toUInt() < d;
            }
            return type < other.type;
        }
        case ST_FLOAT: {
            if (other.type == ST_EMPTY)
                return false;
            double d = toDouble();
            if (other.type == ST_FLOAT) {
                double d2 = other.toDouble();
                bool nan1 = std::isnan(d);
                bool nan2 = std::isnan(d2);
                if (nan1 != nan2) {
                    return nan1 > nan2;
                }
                return d < d2;
            }
            if (other.type == ST_INTEGER || other.type == ST_UNSIGNED) {
                if (std::isnan(d))
                    return true;
                if (other.type == ST_UNSIGNED) {
                    return d < other.toUInt();
                }
                return d < other.toInt();
            }
            return type < other.type;
        }

        case ST_ASCII_SHORT_STRING:
        case ST_ASCII_LONG_STRING:
        case ST_UTF8_SHORT_STRING:
        case ST_UTF8_LONG_STRING:
            if (isStringType((StorageType)other.type)) {
                return std::lexicographical_compare
                    (stringChars(), stringChars() + toStringLength(),
                     other.stringChars(), other.stringChars() + other.toStringLength());
            }
            return type < other.type;

        case ST_TIMESTAMP:  
            if (other.type == ST_TIMESTAMP)
                return toTimestamp() < other.toTimestamp();
            return type < other.type;
        case ST_TIMEINTERVAL:
            if (other.type == ST_TIMEINTERVAL) {
                bool sign1 = signbit(timeInterval.seconds);
                bool sign2 = signbit(other.timeInterval.seconds);

                if (sign1 && !sign2)
                    return true;
                else if (sign2 && !sign1)
                    return false;

                bool nan1 = std::isnan(timeInterval.seconds);
                bool nan2 = std::isnan(other.timeInterval.seconds);

                if (nan1 || nan2) {
                    bool smaller = less_all(timeInterval.months,
                                            other.timeInterval.months,
                                            timeInterval.days,
                                            other.timeInterval.days,
                                            nan1, nan2);

                    return sign1 ? !smaller : smaller;
                }
                else {
                    bool smaller = less_all(timeInterval.months,
                                            other.timeInterval.months,
                                            timeInterval.days,
                                            other.timeInterval.days,
                                            std::abs(timeInterval.seconds),
                                            std::abs(other.timeInterval.seconds));
                    return sign1 ? !smaller : smaller;
                }
            }
            return type < other.type;
        case ST_SHORT_BLOB:
        case ST_LONG_BLOB:
            if (isBlobType((StorageType)other.type)) {
                return std::lexicographical_compare
                    (blobData(), blobData() + blobLength(),
                     other.blobData(), other.blobData() + other.blobLength());
            }
            return type < other.type;

        case ST_SHORT_PATH:
        case ST_LONG_PATH:
            if (isPathType((StorageType)other.type)) {
                // The length zero versus length one with one empty element
                // can't be distinguished using just the string version
                if (strFlags == 0 || other.strFlags == 0)
                    return strFlags < other.strFlags;
                return std::lexicographical_compare
                    (stringChars(), stringChars() + toStringLength(),
                     other.stringChars(),
                     other.stringChars() + other.toStringLength());
            }
            
            return type < other.type;

        default:
            throw HttpReturnException(400, "unknown CellValue type");
        }

    } catch (...) {
        cerr << "comparing " << toString() << " to " << other.toString() << endl;
        throw;
    }
}

const char *
CellValue::
stringChars() const
{
    switch (type) {
    case ST_EMPTY:
    case ST_INTEGER:
    case ST_UNSIGNED:
    case ST_FLOAT:
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
        return nullptr;
    case ST_ASCII_SHORT_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_SHORT_PATH:
        return shortString;
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_PATH:
        return longString->repr;
    }
    cerr << "unknown cell value type " << endl;
    throw HttpReturnException(400, "unknown CellValue type");
}

uint32_t
CellValue::
toStringLength() const
{
    switch (type) {
    case ST_EMPTY:
        return 0;
    case ST_INTEGER:
    case ST_UNSIGNED:
    case ST_FLOAT:
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
        return toString().size();
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return strLength;
    }
    throw HttpReturnException(400, "unknown CellValue type");
}

const unsigned char *
CellValue::
blobData() const
{
    switch (type) {
    case ST_SHORT_BLOB:
        return (const unsigned char *)shortString;
    case ST_LONG_BLOB:
        return (const unsigned char *)longString->repr;
    default:
        throw HttpReturnException(400, "CellValue of type '"+to_string(cellType())+"' with value '"+
                                    trimmedExceptionString()+"' is not a blob", "value", *this);
    }
}

uint32_t
CellValue::
blobLength() const
{
    switch (type) {
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
        return strLength;
    default:
        throw HttpReturnException(400, "CellValue of type '"+to_string(cellType())+"' with value '"+
                                    trimmedExceptionString()+"' is not a blob", "value", *this);
    }
}

bool
CellValue::
isExactDouble() const
{
    switch (type) {
    case ST_EMPTY:
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_SHORT_PATH:
    case ST_LONG_PATH:
        return false;
    case ST_INTEGER:
        return int64_t(toDouble()) == toInt();
    case ST_UNSIGNED:
        return uint64_t(toDouble()) == toUInt();
    case ST_FLOAT:
        return true;
    }

    throw HttpReturnException(400, "unknown CellValue type");
}

void
CellValue::
deleteString()
{
    if (longString) {
        longString->~StringRepr();
        free(longString);
    }
    type = ST_EMPTY;
    longString = nullptr;
}

Utf8String
CellValue::
trimmedExceptionString() const
{
    try {
        auto str = toUtf8String();
        // trim if too long
        if(str.length() < 200) return str;

        return Utf8String(str, 0, 200) + "... (trimmed)";
    }
    MLDB_CATCH_ALL {
        return Utf8String("Exception trying to print string");
    }
}

std::string
CellValue::printInterval() const
{
    if (!std::isfinite(timeInterval.seconds)) {
        if (std::isnan(timeInterval.seconds)) {
            return "NaI";
        }
        else if (timeInterval.seconds > 0) {
            return "Inf";
        }
        else return "-Inf";
    }

    string result;

    if (signbit(timeInterval.seconds))
    {
        result.append("-");
    }

    double secondCount = abs(timeInterval.seconds);

    uint32_t monthsCount = timeInterval.months;

    size_t year = monthsCount / (12);
    monthsCount -= year * (12);    

    size_t hours = secondCount / (60*60);
    secondCount -= hours * (60*60);

    size_t minutes = secondCount / (60);
    secondCount -= minutes * 60;

    if (year != 0)
    {
        result.append(MLDB::format("%dY", year));
    }  

    if (monthsCount != 0)
    {
        result.append(MLDB::format("%d MONTH", monthsCount));
    } 

    if (timeInterval.days != 0)
    {
        if (year != 0 || monthsCount != 0)
            result.append(" ");

        result.append(MLDB::format("%dD", timeInterval.days));
    }   

    if (hours != 0 || minutes != 0 || secondCount != 0)
    {
        if (year != 0 || timeInterval.days != 0)
            result.append(" ");

        if (hours != 0)
            result.append(MLDB::format("%dH %dM %gS", hours, minutes, secondCount));
        else if (minutes != 0)
            result.append(MLDB::format("%dM %gS", minutes, secondCount));
        else
            result.append(MLDB::format("%gS", secondCount));
    }

    if (result.empty())
        result = "0S";

    return result;

}

void
CellValue::
extractStructuredJson(JsonPrintingContext & context) const
{
    switch (cellType()) {
    case CellValue::EMPTY:
        context.writeNull();
        return;
    case CellValue::INTEGER:
        if (isInt64())
            context.writeLong(toInt());
        else
            context.writeUnsignedLongLong(toUInt());
        return;
    case CellValue::FLOAT:
        {
            double floatVal = toDouble();
            if (std::isnan(floatVal))
                {
                    Json::Value v;
                    v["num"] = std::signbit(floatVal) ? "-NaN" : "NaN";
                    context.writeJson(v);
                }
            else if (std::isinf(floatVal))
                {
                    Json::Value v;
                    v["num"] = std::signbit(floatVal) ? "-Inf" : "Inf";
                    context.writeJson(v);
                }
            else
                {
                    context.writeDouble(toDouble());
                }
                
            return;
        }           
    case CellValue::UTF8_STRING:
    case CellValue::ASCII_STRING:
        switch (type) {
        case ST_UTF8_SHORT_STRING:
            context.writeStringUtf8((const char *)shortString, (size_t)strLength);
            return;
        case ST_ASCII_SHORT_STRING:
            context.writeString((const char *)shortString, (size_t)strLength);
            return;
        case ST_UTF8_LONG_STRING:
            context.writeStringUtf8(longString->repr, (size_t)strLength);
            return;
        case ST_ASCII_LONG_STRING:
            context.writeString(longString->repr, (size_t)strLength);
            return;
        default:
            throw HttpReturnException(400, "unknown string cell type");
            return;
        }
    case CellValue::TIMESTAMP: {
        context.startObject();
        context.startMember("ts");
        context.writeString(toString());
        context.endObject();
        return;    
    }
    case CellValue::TIMEINTERVAL: {
        context.startObject();
        context.startMember("interval");
        context.writeString(toString());
        context.endObject();
        return;         
    }
    case CellValue::BLOB: {
        context.startObject();
        context.startMember("blob");
        context.startArray();
            
        // Chunks of ASCII are written as a string; non-ASCII is
        // as integers.
        const unsigned char * p = blobData();
        const unsigned char * e = p + blobLength();

        auto isString = [] (char c)
            {
                return (c >= ' ' && c < 127)
                    || (c == '\n' || c == '\r' || c == '\t');
            };

        while (p < e) {
            const unsigned char * s = p;
            while (isString(*p))
                ++p;
            size_t len = p - s;
            //cerr << "len = " << len << endl;
            if (len == 1) {
                context.newArrayElement();
                context.writeInt(*s);
            }
            else if (len >= 2) {
                context.newArrayElement();
                context.writeString((const char *)s, len);
            }

            while (p < e && !isString(*p)) {
                context.newArrayElement();
                context.writeInt(*p++);
            }
        }
        context.endArray();
        context.endObject();
        return;
    }
    case CellValue::PATH:
        context.startObject();
        context.startMember("path");
        context.startArray();

        for (auto p: coerceToPath()) {
            context.newArrayElement();
            context.writeStringUtf8(p.toUtf8String());
        }

        context.endArray();
        context.endObject();
        return;
    default:
        throw HttpReturnException(400, "unknown cell type");
        return;
    }
}

size_t
CellValue::
memusage() const
{
    switch (type) {
    case ST_EMPTY:
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
    case ST_ASCII_SHORT_STRING:
    case ST_SHORT_BLOB:
    case ST_UTF8_SHORT_STRING:
    case ST_INTEGER:
    case ST_UNSIGNED:
    case ST_FLOAT:
    case ST_SHORT_PATH:
        return sizeof(*this);
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_BLOB:
    case ST_LONG_PATH:
        return sizeof(*this) + sizeof(StringRepr) + strLength;
    }
    throw HttpReturnException(400, "unknown CellValue type");
}

namespace {

enum CellValueCategory {
    CVC_SPECIAL = 0,
    CVC_INTEGER = 1,
    CVC_UNSIGNED = 2,
    CVC_FLOAT = 3,
    CVC_ASCII_STRING = 4,
    CVC_UTF8_STRING = 5,
    CVC_TIMESTAMP = 6,
    CVC_TIME_INTERVAL = 7,
    CVC_BLOB = 8,
    CVC_PATH = 9
};
    

enum CellValueTag {
    CVT_INVAID             = CVC_SPECIAL       * 16 +  0,
    CVT_EMPTY              = CVC_SPECIAL       * 16 +  1,
    CVT_INTEGER            = CVC_INTEGER       * 16 +  0,
    CVT_UNSIGNED_DIRECT    = CVC_UNSIGNED      * 16 +  0,
    CVT_UNSIGNED_INDIRECT  = CVC_UNSIGNED      * 16 + 15,
    CVT_DOUBLE             = CVC_FLOAT         * 16 +  0,
    CVT_TIMESTAMP_DOUBLE   = CVC_TIMESTAMP     * 16 +  0,
    CVT_TIME_INTERVAL      = CVC_TIME_INTERVAL * 16 +  0,
    CVT_ASCII_SHORT_STRING = CVC_ASCII_STRING  * 16 +  0,
    CVT_ASCII_LONG_STRING  = CVC_ASCII_STRING  * 16 + 15,
    CVT_UTF8_SHORT_STRING  = CVC_UTF8_STRING   * 16 +  0,
    CVT_UTF8_LONG_STRING   = CVC_UTF8_STRING   * 16 + 15,
    CVT_SHORT_BLOB         = CVC_BLOB          * 16 +  0,
    CVT_LONG_BLOB          = CVC_BLOB          * 16 + 15,
    CVT_NULL_PATH          = CVC_PATH          * 16 +  0,  // len 0
    CVT_SIMPLE_PATH        = CVC_PATH          * 16 +  1,  // len 1, 0-13 chars
    CVT_LONG_SIMPLE_PATH   = CVC_PATH          * 16 +  14, // len 1, 14+ chars
    CVT_COMPLEX_PATH       = CVC_PATH          * 16 +  15, // len > 1
};

template<typename T>
void serializeBinary(char * & start, const T & bits)
{
    LittleEndian<T> bitsToSerialize{bits};
    memcpy(start, &bitsToSerialize, sizeof(bitsToSerialize));
    start += sizeof(bitsToSerialize);
}

size_t serializeUnsignedLength(uint64_t bits)
{
    if (bits == 0)
        return 0;
    return 1 + highest_bit(bits, -1) / 8;
}

// TODO: maybe the other order...
void serializeUnsigned(char * & start, uint64_t bits)
{
    char * oldStart = start;
    uint64_t oldBits = bits;
    while (bits) {
        *start++ = bits;
        bits >>= 8;
    }
    ExcAssertEqual(start - oldStart, serializeUnsignedLength(oldBits));
}

template<typename T>
void reconstituteBinary(const char * & start, T & bits)
{
    LittleEndian<T> bitsToReconstitute;
    memcpy(&bitsToReconstitute, start, sizeof(bitsToReconstitute));
    bits = bitsToReconstitute;
    start += sizeof(bitsToReconstitute);
}

// TODO: maybe the other order...
uint64_t reconstituteUnsigned(const char * & start, size_t length)
{
    ExcAssertLessEqual(length, 8);
    uint64_t result = 0;
    for (int i = length - 1;  i >= 0;  --i) {
        unsigned char c = start[i];
        result = (result << 8) + c;
    }
    start += length;
    return result;
}

} // file scope

uint64_t
CellValue::
serializedBytes(bool exactBytesAvailable) const
{
    bool needToSerializeLength = !exactBytesAvailable;
    switch (type) {
    case ST_EMPTY:
        return 1;
    case ST_TIMESTAMP:
        return 9;  // TODO: detect integers, make them smaller
    case ST_TIMEINTERVAL:
        return 13;
    case ST_INTEGER:
        if (intVal < 0) {
            return 1 + serializeUnsignedLength(-(intVal + 1));
        }
        // fall through
    case ST_UNSIGNED:
        // 0 to 15 are encoded directly
        // 16+ is the number of bytes (1-8) followed by that number
        if (uintVal < 15) {
            return 1;
        }
        else {
            size_t numBytes = serializeUnsignedLength(uintVal);
            return numBytes + 1 + needToSerializeLength;
        }
    case ST_FLOAT:
        // TODO: detect when we don't need all of the bits
        return 9;
    // The rest of them are string-like
    // Strings of size less than 15 have their length encoded in the
    // first byte.  Others have a length followed by the contents.
    case ST_ASCII_SHORT_STRING:
    case ST_SHORT_BLOB:
    case ST_UTF8_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_BLOB: {
        size_t len = toStringLength();
        if (len < 15) {
            return len + 1;
        } else {
            return 1 + len
                + needToSerializeLength * compactEncodeLength(len);
        }
    }
    case ST_SHORT_PATH:
    case ST_LONG_PATH: {
        size_t len = toStringLength();

        if (strFlags == 0) {
            // Null (length 0)
            return 1;
        }
        else if (strFlags == 1 && len == 1 && stringChars()[0] == '\0') {
            // One empty string
            return 1;
        }
        else if (strFlags == 1) {
            // Length 1
            if (len < 14) {
                return len + 1;
            }
            else {
                return 1 // flag byte
                    + needToSerializeLength * compactEncodeLength(len) // len
                    + len; // characters
            }
        }
        else {
            // Length > 1,
            return 1  // flag byte
                + needToSerializeLength * compactEncodeLength(len) // len
                + len; // characters
        }
    }
    }

    throw HttpReturnException(400, "unknown CellValue type");
}

char *
CellValue::
serialize(char * start, size_t bytesAvailable,
          bool exactBytesAvailable) const
{
    size_t bytesRequired = serializedBytes(exactBytesAvailable);
    if (bytesAvailable < bytesRequired)
        throw HttpReturnException
            (500,
             "Wrong number of bytes available serializing CellValue "
             + jsonEncodeStr(*this));
    char * oldStart = start;
    

    switch (type) {
    case ST_EMPTY:
        *start++ = CVT_EMPTY;
        break;
    case ST_TIMESTAMP:
        *start++ = CVT_TIMESTAMP_DOUBLE;
        serializeBinary(start, timestamp);
        break;
        // TODO: detect integers, milliseconds, etc, make them smaller
    case ST_TIMEINTERVAL:
        *start++ = CVT_TIME_INTERVAL;
        serializeBinary(start, timeInterval.months);
        serializeBinary(start, timeInterval.days);
        serializeBinary(start, timeInterval.seconds);
        break;
    case ST_INTEGER:
        if (intVal < 0) {
            // Negate and shift into positive integer range
            uint64_t toSerialize = -(intVal + 1);
            *start++ = CVT_INTEGER + serializeUnsignedLength(toSerialize);
            serializeUnsigned(start, toSerialize);
            break;
        }
        // fall through
    case ST_UNSIGNED:
        if (uintVal < 15) {
            // 0 to 14 are encoded directly
            *start++ = CVT_UNSIGNED_DIRECT + uintVal;
        }
        else {
            // 15 is the number of bytes (1-8) followed by that number of
            // bytes representing the actual value
            *start++ = CVT_UNSIGNED_INDIRECT;
            // How many bytes?
            if (!exactBytesAvailable)
                *start++ = serializeUnsignedLength(uintVal);
            serializeUnsigned(start, uintVal);
        }
        break;
    case ST_FLOAT:
        *start++ = CVT_DOUBLE;
        serializeBinary(start, floatVal);
        break;

    // The rest of them are string-like
    // Strings of size less than 15 have their length encoded in the
    // first byte.  Others have a length followed by the contents.
    case ST_ASCII_SHORT_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_SHORT_BLOB:
    case ST_LONG_BLOB:
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING: {
        size_t len;

        switch (cellType()) {
        case ASCII_STRING:
        case UTF8_STRING:
            len = toStringLength();
            break;
        case BLOB:
            len = blobLength();
            break;
        default:
            throw HttpReturnException(500, "unknown type in CellValue serialization");
        }

        if (len < 15) {
            char typeByte = 0;
            switch (cellType()) {
            case ASCII_STRING:
                typeByte = CVT_ASCII_SHORT_STRING + len;  break;
            case UTF8_STRING:
                typeByte = CVT_UTF8_SHORT_STRING + len;  break;
            case BLOB:
                typeByte = CVT_SHORT_BLOB + len;  break;
            default:
                throw HttpReturnException(500, "unknown type in CellValue serialization");
            }
            *start++ = typeByte;
        }
        else {
            char typeByte = 0;
            switch (cellType()) {
            case ASCII_STRING:
                typeByte = CVT_ASCII_LONG_STRING;  break;
            case UTF8_STRING:
                typeByte = CVT_UTF8_LONG_STRING;  break;
            case BLOB:
                typeByte = CVT_LONG_BLOB;  break;
            default:
                throw HttpReturnException(500, "unknown type in CellValue serialization");
            }
            *start++ = typeByte;
            if (!exactBytesAvailable)
                compactEncode(start, start + len, len);
        }
        switch (cellType()) {
        case ASCII_STRING:
        case UTF8_STRING:
            memcpy(start, stringChars(), len);
            break;
        case BLOB:
            memcpy(start, blobData(), len);
            break;
        default:
            throw HttpReturnException(500, "unknown type in CellValue serialization");
            
        }
        start += len;
        break;

    case ST_SHORT_PATH:
    case ST_LONG_PATH: {
        size_t len = toStringLength();

        if (strFlags == 0) {
            *start++ = CVT_NULL_PATH;
            break;
        }
        else if (strFlags == 1) {
            // Simple encoding where we don't escape anything
            if (len == 0 || len == 1 && stringChars()[0] == '\0') {
                *start++ = CVT_SIMPLE_PATH;
                len = 0;
            }
            else if (len < 14) {
                *start++ = CVT_SIMPLE_PATH + len;
            }
            else {
                *start++ = CVT_LONG_SIMPLE_PATH;
                if (!exactBytesAvailable)
                    compactEncode(start, start + len, len);
            }
        }
        else {
            // Non-simple encoding
            *start++ = CVT_COMPLEX_PATH;
            if (!exactBytesAvailable)
                compactEncode(start, start + len, len);
        }
        memcpy(start, stringChars(), len);
        start += len;
        break;
    }
    }
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }

    if (start - oldStart != bytesRequired) {
        cerr << "error serializing " << jsonEncodeStr(*this)
             << " of type " << jsonEncodeStr(type) << endl;
    }
    ExcAssertEqual(start - oldStart, bytesRequired);

#if 0
    CellValue reconstituted;
    size_t numBytes;
    try {
        std::tie(reconstituted, numBytes)
            = reconstitute(oldStart, bytesAvailable,
                           serializationFormat(exactBytesAvailable),
                           exactBytesAvailable);

        ExcAssertEqual(numBytes, bytesRequired);
    } catch (...) {
        cerr << "trying to reconstitute " << jsonEncodeStr(*this)
             << ": " << getExceptionString() << endl;
        cerr << "bytesAvailable = " << bytesAvailable
             << " numBytes = " << numBytes << endl;
        hex_dump(oldStart, start - oldStart);
    }

    if (reconstituted != *this) {
        cerr << "should be: " << jsonEncodeStr(*this) << " type " << type << endl;
        cerr << "got: " << jsonEncodeStr(reconstituted) << " type " << reconstituted.type << endl;
        hex_dump(oldStart, start - oldStart);

        ExcAssertEqual(*this, reconstituted);
    }
#endif
    return start;
}

uint8_t
CellValue::
serializationFormat(bool exactBytesAvailable)
{
    return 1;
}

std::pair<CellValue, ssize_t>
CellValue::
reconstitute(const char * buf,
             size_t bytesAvailable,
             uint8_t serializationFormat,
             bool exactBytesAvailable)
{
    if (serializationFormat != 1) {
        throw HttpReturnException
            (500, "Attempt to reconstitute unknown CellValue format "
             + jsonEncodeStr((int)serializationFormat));
    }

    if (bytesAvailable < 1) {
        throw HttpReturnException
            (500, "Attempt to reconstitute CellValue at end of buffer");
    }

    const char * oldBuf = buf;

    unsigned char indicator = *buf++;
    
    unsigned category = indicator >> 4;

    CellValue result;
    
    switch (category) {
    case CVC_SPECIAL:
        switch (indicator) {
        case CVT_EMPTY:
            break;
        default:
            throw HttpReturnException
                (500, "Unknown CellValue special code");
        }
        break;
    case CVC_INTEGER: {
        int length = indicator - CVT_INTEGER;
        if (length > 8) {
            throw HttpReturnException
                (500, "Unknown CellValue integer code");
        }
        int64_t val = reconstituteUnsigned(buf, length);
        result = -val - 1;
        break;
    }
    case CVC_UNSIGNED: {
        int length = indicator - CVT_UNSIGNED_DIRECT;
        //cerr << "length = " << length << endl;
        if (length < 15) {
            result = length;
        }
        else {
            size_t length = exactBytesAvailable ? bytesAvailable - 1 : *buf++;
            //cerr << "length is now " << length << endl;
            result = reconstituteUnsigned(buf, length);
            //cerr << "result = " << result << endl;
        }
        break;
    }
    case CVC_FLOAT:
        switch (indicator) {
        case CVT_DOUBLE: {
            double val;
            reconstituteBinary(buf, val);
            result = val;
            break;
        }
        default:
            throw HttpReturnException
                (500, "Unknown CellValue float code "
                 + jsonEncodeStr((int)indicator));
        }
        break;
        
    case CVC_TIMESTAMP:
        switch (indicator) {
        case CVT_TIMESTAMP_DOUBLE: {
            double ts = result.timestamp;
            reconstituteBinary(buf, ts);
            result = Date::fromSecondsSinceEpoch(ts);
            break;
        }
        default:
            throw HttpReturnException
                (500, "Unknown CellValue float code");
        }
        break;

    case CVC_TIME_INTERVAL:
        switch (indicator) {
        case CVT_TIME_INTERVAL: {
            uint16_t months;
            uint16_t days;
            double   seconds;
            
            reconstituteBinary(buf, months);
            reconstituteBinary(buf, days);
            reconstituteBinary(buf, seconds);

            result = CellValue::fromMonthDaySecond(months, days, seconds);
            break;
        default:
            throw HttpReturnException
                (500, "Unknown CellValue time interval code");
        }
        }
        break;
        
    case CVC_ASCII_STRING:
    case CVC_UTF8_STRING:
    case CVC_BLOB: {
        size_t length = indicator % 16;
        if (length == 15) {
            if (exactBytesAvailable) {
                length = bytesAvailable - 1;
            }
            else {
                length = compactDecode(buf, buf + bytesAvailable - 1);
            }
        }

        switch (category) {
        case CVC_ASCII_STRING:
        case CVC_UTF8_STRING:
            result = CellValue(buf, length);
            break;
        case CVC_BLOB:
            result = CellValue::blob(buf, length);
            break;
        }
        buf += length;
        break;
    }
        
    case CVC_PATH: {
        size_t length;

        if (indicator == CVT_NULL_PATH) {
            result = Path();
            break;
        }
        else if (indicator == CVT_SIMPLE_PATH) {
            result = Path(PathElement(""));
            break;
        }
        else if (indicator == CVT_COMPLEX_PATH
                 || indicator == CVT_LONG_SIMPLE_PATH) {
            if (exactBytesAvailable) {
                length = bytesAvailable - 1;
            }
            else {
                length = compactDecode(buf, buf + bytesAvailable);
            }
        }
        else {
            length = (indicator & 15) - 1;
        }

        if (indicator == CVT_COMPLEX_PATH) {
            result = CellValue(Path::parse(buf, length));
        }
        else {
            result = CellValue(Path(PathElement(buf, length)));
        }
        buf += length;
        break;
    }   

    default:
        throw HttpReturnException
            (500, "Unknown CellValue category");
    }

    size_t bytesUsed = buf - oldBuf;
    if (exactBytesAvailable && bytesUsed != bytesAvailable) {
        cerr << "bytesUsed = " << bytesUsed << endl;
        cerr << "bytesAvailable = " << bytesAvailable << endl;
        throw HttpReturnException
            (500, "Error reconstituting CellValue: wrong bytes used");
    }
    
    return {result, buf - oldBuf};
}

struct CellValueDescription: public ValueDescriptionT<CellValue> {
    virtual void parseJsonTyped(CellValue * val,
                                JsonParsingContext & context) const
    {
        if (context.isNull()) {
            context.expectNull();
            *val = CellValue();
        }
        else if(context.isString())
            *val = CellValue(context.expectStringUtf8());
        else if (context.isInt())
            *val = CellValue(context.expectLongLong());
        else if (context.isNumber())
            *val = CellValue(context.expectDouble());
        else if (context.isObject()) {
            Json::Value v = context.expectJson();
            ExcAssertEqual(v.size(), 1);
            if (v.isMember("ts")) {
                *val = CellValue(Date::parseIso8601DateTime(v["ts"].asString()));
            }
            else if (v.isMember("num")) {

                std::string text = v["num"].asString();

                if (text == "NaN") {
                     *val = CellValue(nan(""));
                }
                else if (text == "-NaN") {
                     *val = CellValue(-nan(""));
                }
                else if (text == "Inf") {
                     *val = CellValue(std::numeric_limits<double>::infinity());
                }
                else if (text == "-Inf") {
                     *val = CellValue(-std::numeric_limits<double>::infinity());
                }
                else
                {
                    throw HttpReturnException(400, "Unknown numeric value '" + text + "'");
                }               
            }
            else if (v.isMember("interval")) {
                std::string text = v["interval"].asString();
                ParseContext context(text, text.c_str(), text.length());
                uint32_t months = 0;
                uint32_t days = 0;
                double seconds = 0.0f;
                expect_interval(context, months, days, seconds);
                *val = CellValue::fromMonthDaySecond(months, days, seconds);
            }            
            else if (v.isMember("blob")) {
                std::string contents;

                if (!v["blob"].isNull()) {
                    if (v["blob"].type() != Json::arrayValue) {
                        throw HttpReturnException(400, "JSON blob is not an array: '" + v.toStringNoNewLine() + "'");
                    }
                    for (auto & v2: v["blob"]) {
                        if (v2.isUInt() || v2.isInt()) {
                            uint64_t val = v2.asUInt();
                            if (val > 255)
                                context.exception("Invalid byte value " + std::to_string(val) + " reading blob data (must be 0-255)");
                            contents += val;
                        }
                        else if (v2.isString()) {
                            std::string part = v2.asString();
                            contents += part;
                        }
                        else context.exception
                                 ("Can't deal with unknown array entry '"
                                  + v2.toStringNoNewLine()
                                  + "' reading blob data");

                    }
                }

                *val = CellValue::blob(std::move(contents));
            }
            else if (v.isMember("path")) {
                std::vector<PathElement> result;
                for (auto & e: jsonDecode<std::vector<Utf8String> >(v["path"])) {
                    result.emplace_back(e);
                }
                *val = Path(result.data(), result.size());
            }
            else {
                throw HttpReturnException(400, "Unknown JSON CellValue '" + v.toStringNoNewLine() + "'");
            }
        }
        else if (context.isBool()) {
            *val = CellValue(context.expectBool());
        }
        else {
            Json::Value val = context.expectJson();
            cerr << "val = " << val << endl;
            throw HttpReturnException(400, "Unknown cell value",
                                      "json",
                                      val.toStringNoNewLine());
        }
    }

    virtual void printJsonTyped(const CellValue * val,
                                JsonPrintingContext & context) const
    {
        val->extractStructuredJson(context);
    }

    virtual bool isDefaultTyped(const CellValue * val) const
    {
        return val->empty();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(CellValue, CellValueDescription);

std::string to_string(const CellValue & cell)
{
    return jsonEncodeStr(cell);
}

std::string keyToString(const CellValue & cell)
{
    return jsonEncodeStr(cell);
}

CellValue stringToKey(const std::string & str, CellValue *)
{
    return jsonDecodeStr<CellValue>(str);
}

std::string keyToString(const CellValue::CellType & cell)
{
    return to_string(cell);
}

DEFINE_ENUM_DESCRIPTION_NAMED(CellTypeDescription, CellValue::CellType);

CellTypeDescription::
CellTypeDescription()
{
    addValue("EMPTY", CellValue::CellType::EMPTY, "No value in cell");
    addValue("INTEGER", CellValue::CellType::INTEGER, "Cell value is an exact integer");
    addValue("FLOAT", CellValue::CellType::FLOAT, "Cell contains a numeric value");
    addValue("ASCII_STRING", CellValue::CellType::ASCII_STRING, "Cell contains an ASCII string");
    addValue("UTF8_STRING", CellValue::CellType::UTF8_STRING, "Cell contains a utf8 string");
    addValue("TIMESTAMP", CellValue::CellType::TIMESTAMP, "Cell contains a timestamp");
    addValue("TIMEINTERVAL", CellValue::CellType::TIMEINTERVAL, "Cell contains a time interval");
    addValue("BLOB", CellValue::CellType::BLOB, "Cell contains a binary blob");
    addValue("PATH", CellValue::CellType::PATH, "Cell contains a path (row or column name)");
}

std::ostream & operator << (std::ostream & stream, const CellValue & val)
{
    if(val.cellType() == CellValue::CellType::UTF8_STRING)
        return stream << val.toUtf8String();
    else
        return stream << val.toString();
}

std::string to_string(const CellValue::CellType & type)
{
    static const CellTypeDescription desc;
    return desc.printString(type);
}

std::ostream & operator << (std::ostream & stream, const CellValue::CellType & type)
{
    return stream << to_string(type);
}

} // namespace MLDB
 

