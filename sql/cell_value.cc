/** cell_value.cc
    Jeremy Barnes, 24 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "cell_value.h"
#include "mldb/ext/siphash/csiphash.h"
#include "mldb/utils/json_utils.h"
#include "mldb/types/dtoa.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/itoa.h"
#include "cell_value_impl.h"
#include "mldb/base/parse_context.h"
#include "interval.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* VALUE                                                                     */
/*****************************************************************************/


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
                    throw ML::Exception("Invalid sequence within utf-8 string");
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
                    throw ML::Exception("Invalid sequence within utf-8 string");
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
        || other.type == ST_LONG_BLOB) {
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

CellValue
CellValue::
parse(const char * s_, size_t len, StringCharacteristics characteristics)
{
    static constexpr size_t NUMERICAL_BUFFER = 64;

    // if the string is longer than 64 characters it can't realistically
    // be a numerical value
    if (len > NUMERICAL_BUFFER)
        return CellValue(s_, len, characteristics);

    if (len == 0)
        return CellValue();

    // this ensures that our buffer is null terminated as required below
    char s[NUMERICAL_BUFFER + 1] = {0};
    memcpy(s, s_, len);

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
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
}

Utf8String
CellValue::
toUtf8String() const
{
    switch (type) {
    case ST_UTF8_SHORT_STRING:
    case ST_ASCII_SHORT_STRING:
    case ST_SHORT_BLOB:
        return Utf8String((const char *)shortString, (size_t)strLength);
    case ST_UTF8_LONG_STRING:
    case ST_ASCII_LONG_STRING:
    case ST_LONG_BLOB:
        try {
            return Utf8String(longString->repr, (size_t)strLength);
        } catch (...) {
            for (unsigned i = 0;  i < strLength;  ++i) {
                cerr << "char at index " << i << " of " << strLength << " is "
                     << (int)longString->repr[i] << endl;
            }
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
        return Datacratic::itoa(intVal);
    case ST_UNSIGNED:
        return Datacratic::itoa(uintVal);
    case ST_FLOAT: {
        return Datacratic::dtoa(floatVal);
    }
    case ST_ASCII_SHORT_STRING:
        return string(shortString, shortString + strLength);
    case ST_ASCII_LONG_STRING:
        return string(longString->repr, longString->repr + strLength);
    case ST_UTF8_SHORT_STRING:
    case ST_UTF8_LONG_STRING:
        throw HttpReturnException(400, "cannot call toString on utf8 string");
    case ST_TIMESTAMP:
        return Date::fromSecondsSinceEpoch(timestamp)
            .printIso8601(-1 /* as many digits as necessary */);
    case ST_TIMEINTERVAL:
        return printInterval();
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
        throw HttpReturnException(400, "Can't convert value '" + toUtf8String() + "' to double");
    }
}

int64_t
CellValue::
toInt() const
{
    if (type != ST_INTEGER) {
        throw HttpReturnException(400, "Can't convert value '" + toUtf8String() + "' of type '"
                                  + std::to_string(type) + "' to integer");
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
        throw HttpReturnException(400, "Can't convert value '" + toUtf8String() + "' of type '"
                                  + std::to_string(type) + "' to unsigned integer");
}

Date
CellValue::
toTimestamp() const
{
    if (type != ST_TIMESTAMP)
        throw HttpReturnException(400, "Can't convert value to timestamp",
                                  "value", *this);
    return Date::fromSecondsSinceEpoch(timestamp);
}

std::tuple<int64_t, int64_t, double>
CellValue::
toMonthDaySecond() const
{
    if (type != ST_TIMEINTERVAL) {
        throw HttpReturnException(400, "Can't convert value to time interval",
                                  "value", *this);
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
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
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
        return !strLength;
    case ST_TIMESTAMP:
    case ST_TIMEINTERVAL:
        return false;
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
}

const HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

template<typename T>
static uint64_t siphash24_bin(const T & v, HashSeed key)
{
    return ::mldb_siphash24(&v, sizeof(v), key.b);
}

CellValueHash
CellValue::
hash() const
{
    switch (type) {
    case ST_ASCII_SHORT_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_SHORT_BLOB:
        return CellValueHash(mldb_siphash24(shortString, strLength, defaultSeedStable.b));
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_BLOB:
        if (!longString->hash) {
            longString->hash = mldb_siphash24(longString->repr, strLength, defaultSeedStable.b);
        }
        return CellValueHash(longString->hash);
    case ST_TIMEINTERVAL:
        return CellValueHash(mldb_siphash24(shortString, 12, defaultSeedStable.b));
    default:
        if (bits2 != 0)
            cerr << "hashing " << jsonEncodeStr(*this) << endl;
        ExcAssertEqual(bits2, 0);
        return CellValueHash(siphash24_bin(bits1, defaultSeedStable));
    }
}

int
CellValue::
compare(const CellValue & other) const
{
    if (bits1 == other.bits1 && bits2 == other.bits2)
        return 0;

    return (*this < other) ? -1 : (*this == other) ? 0 : 1;
}

bool
CellValue::
operator == (const CellValue & other) const
{
    if (other.type != type)
    {
        return false;
    }
    if (type == other.type && bits1 == other.bits1 && bits2 == other.bits2)
    {
        return true;
    }

    switch (type) {
    case ST_EMPTY:
        return true;
    case ST_INTEGER:
        return toInt() == other.toInt();
    case ST_UNSIGNED:
        return toUInt() == other.toUInt();
    case ST_FLOAT:
        return toDouble() == other.toDouble();
    case ST_ASCII_SHORT_STRING:
    case ST_UTF8_SHORT_STRING:
    case ST_SHORT_BLOB:
        return strLength == other.strLength
            && strncmp(shortString, other.shortString, strLength) == 0;
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
    case ST_LONG_BLOB:
        return strLength == other.strLength
            && strncmp(longString->repr, other.longString->repr, strLength) == 0;
    case ST_TIMESTAMP:
        return toTimestamp() == other.toTimestamp();
    case ST_TIMEINTERVAL:
    {
        bool sign1 = signbit(timeInterval.seconds);
        bool sign2 = signbit(other.timeInterval.seconds);

        if (sign1 != sign2)
            return false;

        return timeInterval.months == other.timeInterval.months && timeInterval.days == other.timeInterval.days && timeInterval.seconds == other.timeInterval.seconds;
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
    // 2.  INTEGER or FLOAT, compared numerically
    // 3.  STRING or BLOB, compared lexicographically

    try {
        if (JML_UNLIKELY(flags == other.flags && bits1 == other.bits1 && bits2 == other.bits2))
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
            double d = other.toDouble();
            if (std::isnan(d))
                return false;

            return toInt() < d;
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
            double d = other.toDouble();
            if (std::isnan(d))
                return false;

            return toUInt() < d;
        }
        case ST_FLOAT: {
            if (other.type == ST_EMPTY)
                return false;
            if (other.isString())
                return true;
            double d = toDouble();
            if (other.type == ST_FLOAT) {
                double d2 = other.toDouble();
                if (std::isnan(d) && !std::isnan(d2))
                    return true;
                return d < d2;
            }
            if (std::isnan(d))
                return true;
            if (other.type == ST_UNSIGNED) {
                return d < other.toUInt();
            }
            return d < other.toInt();
        }
        case ST_ASCII_SHORT_STRING:
        case ST_ASCII_LONG_STRING:
        case ST_UTF8_SHORT_STRING:
        case ST_UTF8_LONG_STRING:
            if (!isStringType((StorageType)other.type))
                return false;

            return std::lexicographical_compare
                (stringChars(), stringChars() + toStringLength(),
                 other.stringChars(), other.stringChars() + other.toStringLength());
        case ST_TIMESTAMP:  
            return toTimestamp() < other.toTimestamp();
        case ST_TIMEINTERVAL:
            {
                bool sign1 = signbit(timeInterval.seconds);
                bool sign2 = signbit(other.timeInterval.seconds);

                if (sign1 && !sign2)
                    return true;
                else if (sign2 && !sign1)
                    return false;

                bool smaller = timeInterval.months < other.timeInterval.months || 
                   (timeInterval.months == other.timeInterval.months && timeInterval.days < other.timeInterval.days) ||
                   (timeInterval.months == other.timeInterval.months && timeInterval.days == other.timeInterval.days 
                    && abs(timeInterval.seconds) < abs(other.timeInterval.seconds));

                return sign1 ? !smaller : smaller;
            }
        case ST_SHORT_BLOB:
        case ST_LONG_BLOB:
            if (!isBlobType((StorageType)other.type))
                return false;

            return std::lexicographical_compare
                (blobData(), blobData() + blobLength(),
                 other.blobData(), other.blobData() + other.blobLength());
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
        return shortString;
    case ST_ASCII_LONG_STRING:
    case ST_UTF8_LONG_STRING:
        return longString->repr;
    default:
        cerr << "unknown cell value type " << endl;
        throw HttpReturnException(400, "unknown CellValue type");
    }
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
        return strLength;
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
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
        throw HttpReturnException(400, "CellValue is not a blob",
                                  "value", *this);
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
        throw HttpReturnException(400, "CellValue is not a blob",
                                  "value", *this);
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
        return false;
    case ST_INTEGER:
        return int64_t(toDouble()) == toInt();
    case ST_UNSIGNED:
        return uint64_t(toDouble()) == toUInt();
    case ST_FLOAT:
        return true;
    default:
        throw HttpReturnException(400, "unknown CellValue type");
    }
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
        result.append(ML::format("%dY", year));
    }  

    if (monthsCount != 0)
    {
        result.append(ML::format("%d MONTH", monthsCount));
    } 

    if (timeInterval.days != 0)
    {
        if (year != 0 || monthsCount != 0)
            result.append(" ");

        result.append(ML::format("%dD", timeInterval.days));
    }   

    if (hours != 0 || minutes != 0 || secondCount != 0)
    {
        if (year != 0 || timeInterval.days != 0)
            result.append(" ");

        if (hours != 0)
            result.append(ML::format("%dH %dM %gS", hours, minutes, secondCount));
        else if (minutes != 0)
            result.append(ML::format("%dM %gS", minutes, secondCount));
        else
            result.append(ML::format("%gS", secondCount));
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

        while (p < e) {
            const unsigned char * s = p;
            while (p < e && *p >= ' ' && *p < 127 && isascii(*p))
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
                
            while (p < e && (*p <= ' ' || *p >= 127 || !isascii(*p))) {
                context.newArrayElement();
                context.writeInt(*p++);
            }
        }
        context.endArray();
        context.endObject();
        return;
    }
    default:
        throw HttpReturnException(400, "unknown cell type");
        return;
    }
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
                ML::Parse_Context context(text, text.c_str(), text.length());
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
                                context.exception("Invalid byte value " + to_string(val) + " reading blob data (must be 0-255)");
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
} // namespace Datacratic 

