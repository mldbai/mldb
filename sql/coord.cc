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

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* COORD                                                                     */
/*****************************************************************************/


Coord::
Coord()
{
}

Coord::
Coord(const Utf8String & str)
    : str(str)
{
}

Coord::
Coord(const std::string & str)
    : str(str)
{
}

Coord::
Coord(const char * str)
    : str(str)
{
}

Coord::
Coord(const char * str, size_t len)
    : str(str, len, false /* known valid UTF8 */)
{
}

bool
Coord::
stringEqual(const std::string & other) const
{
    return str.rawString() == other;
}

bool
Coord::
stringEqual(const Utf8String & other) const
{
    return str == other;
}

bool
Coord::
stringEqual(const char * other) const
{
    return strcmp(str.rawData(), other) == 0;
}

bool
Coord::
stringLess(const std::string & other) const
{
    return str.rawString() < other;
}

bool
Coord::
stringLess(const Utf8String & other) const
{
    return str < other;
}

bool
Coord::
stringLess(const char * other) const
{
    return strcmp(str.rawData(), other) < 0;
}

bool
Coord::
stringGreaterEqual(const std::string & other) const
{
    return str.rawString() >= other;
}

bool
Coord::
stringGreaterEqual(const Utf8String & other) const
{
    return str >= other;
}

bool
Coord::
stringGreaterEqual(const char * other) const
{
    return strcmp(str.rawData(), other) >= 0;
}
    
bool
Coord::
operator == (const Coord & other) const
{
    return str == other.str;
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
    return str < other.str;
}

Utf8String
Coord::
toUtf8String() const
{
    return str;
}

std::string
Coord::
toString() const
{
    return str.rawString();
}

constexpr HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

uint64_t
Coord::
hash() const
{
    return Id(str).hash();
    return ::mldb_siphash24(str.rawData(), str.rawLength(), defaultSeedStable.b);
}

bool
Coord::
empty() const
{
    return str.empty();
}

Coord
Coord::
operator + (const Coord & other) const
{
    if (empty())
        return std::move(other);
    if (other.empty())
        return *this;
    return str + "." + other.str;
}

Coord
Coord::
operator + (Coord && other) const
{
    if (empty())
        return std::move(other);
    if (other.empty())
        return *this;
    return str + "." + std::move(other.str);
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
