/** coord.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "coord.h"
#include <cstring>
#include "mldb/types/hash_wrapper.h"
#include "mldb/types/value_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/ext/siphash/csiphash.h"
#include "mldb/utils/json_utils.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* COORD                                                                     */
/*****************************************************************************/

Coord::
Coord()
    : words{0, 0, 0, 0}
{
}

Coord::
Coord(Utf8String str)
{
    initString(std::move(str));
}

Coord::
Coord(std::string str)
{
    initString(std::move(str));
}

Coord::
Coord(const char * str, size_t len)
{
    initChars(str, len);
}

Coord::
Coord(uint64_t i)
{
    initString(std::to_string(i));
}

bool
Coord::
stringEqual(const std::string & other) const
{
    return dataLength() == other.size()
        && compareString(other.data(), other.size()) == 0;
}

bool
Coord::
stringEqual(const Utf8String & other) const
{
    return dataLength() == other.rawLength()
        && compareString(other.rawData(), other.rawLength()) == 0;
}

bool
Coord::
stringEqual(const char * other) const
{
    return compareStringNullTerminated(other) == 0;
}

bool
Coord::
stringLess(const std::string & other) const
{
    return compareString(other.data(), other.size()) < 0;
}

bool
Coord::
stringLess(const Utf8String & other) const
{
    return compareString(other.rawData(), other.rawLength()) < 0;
}

bool
Coord::
stringLess(const char * other) const
{
    return compareStringNullTerminated(other) < 0;
}

bool
Coord::
stringGreaterEqual(const std::string & other) const
{
    return compareString(other.data(), other.size()) >= 0;
}

bool
Coord::
stringGreaterEqual(const Utf8String & other) const
{
    return compareString(other.rawData(), other.rawLength()) >= 0;
}

bool
Coord::
stringGreaterEqual(const char * other) const
{
    return compareStringNullTerminated(other) >= 0;
}
    
bool
Coord::
operator == (const Coord & other) const
{
    return dataLength() == other.dataLength()
        && compareString(other.data(), other.dataLength()) == 0;
}

bool
Coord::
operator != (const Coord & other) const
{
    return ! operator == (other);
}

bool
Coord::
operator <  (const Coord & other) const
{
    return compareString(other.data(), other.dataLength()) < 0;
}

Utf8String
Coord::
toUtf8String() const
{
    if (complex_)
        return getComplex();
    else return Utf8String(data(), dataLength(), true /* is valid UTF-8 */);
}

std::string
Coord::
toString() const
{
    return toUtf8String().stealRawString();
}

//constexpr HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

uint64_t
Coord::
hash() const
{
    return Id(data(), dataLength()).hash();
    //return ::mldb_siphash24(str.rawData(), str.rawLength(), defaultSeedStable.b);
}

bool
Coord::
empty() const
{
    return complex_ == 0 && simpleLen_ == 0;
}

Coord
Coord::
operator + (const Coord & other) const
{
    size_t l1 = dataLength();
    size_t l2 = other.dataLength();

    if (l1 == 0)
        return other;
    if (l2 == 0)
        return *this;

    size_t len = 1 + l1 + l2;

    Coord result;

    if (len <= 31) {
        // We can construct in-place
        result.complex_ = 0;
        result.simpleLen_ = len;
        auto d = data();
        std::copy(d, d + l1, result.bytes + 1);
        result.bytes[l1 + 1] = '.';
        d = other.data();
        std::copy(d, d + l2, result.bytes + l1 + 2);
    }
    else if (len < 4096) {
        // Construct on the stack and do just one allocation
        char str[4096];
        result.complex_ = 1;
        auto d = data();
        std::copy(d, d + l1, str);
        str[l1] = '.';
        d = other.data();
        std::copy(d, d + l2, str + l1 + 1);
        new (&result.str.str) Utf8String(str, len);
    }
    else {
        // It's long; just use the Utf8String
        result = toUtf8String() + "." + other.toUtf8String();
    }

    return result;
}

Coord
Coord::
operator + (Coord && other) const
{
    if (empty())
        return std::move(other);
    return operator + ((const Coord &)other);
}

Coord::
operator RowHash() const
{
    return RowHash(hash());
}

Coord::
operator ColumnHash() const
{
    return ColumnHash(hash());
}

void
Coord::
complexDestroy()
{
    getComplex().~Utf8String();
}

void
Coord::
complexCopyConstruct(const Coord & other)
{
    new (&str.str) Utf8String(other.getComplex());
}

void
Coord::
complexMoveConstruct(Coord && other)
{
    new (&str.str) Utf8String(std::move(other.getComplex()));
}

void
Coord::
initString(Utf8String str)
{
    ExcAssertEqual(strlen(str.rawData()), str.rawLength());
    words[0] = words[1] = words[2] = words[3] = 0;
    if (str.rawLength() <= 31) {
        complex_ = 0;
        simpleLen_ = str.rawLength();
        std::copy(str.rawData(), str.rawData() + str.rawLength(),
                  bytes + 1);
    }
    else {
        complex_ = 1;
        new (&this->str.str) Utf8String(std::move(str));
    }
}

void
Coord::
initChars(const char * str, size_t len)
{
    words[0] = words[1] = words[2] = words[3] = 0;
    if (len <= 31) {
        complex_ = 0;
        simpleLen_ = len;
        std::copy(str, str + len, bytes + 1);
    }
    else {
        complex_ = 1;
        new (&this->str.str) Utf8String(str, len);
    }
}

const char *
Coord::
data() const
{
    if (complex_)
        return getComplex().rawData();
    else return (const char *)bytes + 1;
}

size_t
Coord::
dataLength() const
{
    if (complex_)
        return getComplex().rawLength();
    else return simpleLen_;
}

int
Coord::
compareString(const char * str, size_t len) const
{
    int res = std::strncmp(data(), str, std::min(dataLength(), len));

    if (res) return res;

    // Equal for the whole common part.  Return based upon which
    // is longer
    return (ssize_t)len - (ssize_t)dataLength();
}

int
Coord::
compareStringNullTerminated(const char * str) const
{
    return compareString(str, ::strlen(str));
}

const Utf8String &
Coord::
getComplex() const
{
    ExcAssert(complex_);
    return str.str;
}

Utf8String &
Coord::
getComplex()
{
    ExcAssert(complex_);
    return str.str;
}

std::ostream & operator << (std::ostream & stream, const Coord & coord)
{
    return stream << coord.toUtf8String();
}

std::istream & operator >> (std::istream & stream, Coord & coord)
{
    std::string s;
    stream >> s;
    coord = Coord(std::move(s));
    return stream;
}


/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

struct CoordDescription 
    : public ValueDescriptionI<Coord, ValueKind::ATOM, CoordDescription> {

    virtual void parseJsonTyped(Coord * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const Coord * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const Coord * val) const;
};

DEFINE_VALUE_DESCRIPTION_NS(Coord, CoordDescription);

void
CoordDescription::
parseJsonTyped(Coord * val,
               JsonParsingContext & context) const
{
    if (context.isNull())
        *val = Coord();
    else if (context.isString())
        *val = context.expectStringUtf8();
    else {
        *val = context.expectJson().toStringNoNewLine();
    }
}

void
CoordDescription::
printJsonTyped(const Coord * val,
               JsonPrintingContext & context) const
{
    context.writeJson(jsonEncode(Id(val->toUtf8String())));
    //context.writeStringUtf8(val->toUtf8String());
}

bool
CoordDescription::
isDefaultTyped(const Coord * val) const
{
    return val->empty();
}

} // namespace MLDB
} // namespace Datacratic
