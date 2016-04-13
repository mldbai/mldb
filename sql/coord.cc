/** coord.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "coord.h"
#include <cstring>
#include "mldb/types/hash_wrapper.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/ext/siphash/csiphash.h"
#include "mldb/types/itoa.h"
#include "mldb/utils/json_utils.h"
#include "mldb/ext/cityhash/src/city.h"

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
    ItoaBuf buf;
    char * begin;
    char * end;
    std::tie(begin, end) = itoa(i, buf);
    initChars(begin, end - begin);
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

bool
Coord::
startsWith(const std::string & other) const
{
    return toUtf8String().startsWith(other);
}

bool
Coord::
startsWith(const Coord & other) const
{
    return toUtf8String().startsWith(other.toUtf8String());
}

bool
Coord::
startsWith(const char * other) const
{
    return toUtf8String().startsWith(other);
}

bool
Coord::
startsWith(const Utf8String & other) const
{
    return toUtf8String().startsWith(other);
}

Utf8String
Coord::
toUtf8String() const
{
    if (complex_)
        return getComplex();
    else return Utf8String(data(), dataLength(), true /* is valid UTF-8 */);
}

Utf8String
Coord::
toEscapedUtf8String() const
{
    const char * d = data();
    size_t l = dataLength();

    auto isSimpleChar = [] (int c) -> bool
        {
            return c != '\"' && c != '.';
        };

    bool isSimple = l == 0 || isSimpleChar(d[0]);
    for (size_t i = 0;  i < l && isSimple;  ++i) {
        if (!isSimpleChar(d[i]) && d[i] != '_')
            isSimple = false;
    }
    if (isSimple)
        return toUtf8String();
    else {
        Utf8String result = "\"";
        for (auto c: toUtf8String()) {
            if (c == '"')
                result += "\"\"";
            else result += c;
        }
        result += "\"";
        return result;
    }
}

bool
Coord::
isIndex() const
{
    return toIndex() != -1;
}

ssize_t
Coord::
toIndex() const
{
    if (dataLength() > 12)
        return -1;
    uint64_t val = 0;
    const char * p = data();
    const char * e = p + dataLength();
    if (e == p)
        return false;
    for (; p != e;  ++p) {
        if (!isdigit(*p))
            return -1;
        val = 10 * val + (*p - '0');
    }
    return val;
}

bool
Coord::
hasStringView() const
{
    return true;  // currently we store as a string, so always true
}

std::pair<const char *, size_t>
Coord::
getStringView() const
{
    return { data(), dataLength() };
}

uint64_t
Coord::
oldHash() const
{
    return Id(data(), dataLength()).hash();
}

constexpr HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

uint64_t
Coord::
newHash() const
{
    return ::mldb_siphash24(data(), dataLength(), defaultSeedStable.b);
}

Coords
Coord::
operator + (const Coord & other) const
{
    Coords result(*this);
    return result + other;
}

Coords
Coord::
operator + (Coord && other) const
{
    Coords result(*this);
    return result + std::move(other);
}

Coords
Coord::
operator + (const Coords & other) const
{
    Coords result(*this);
    return result + other;
}

Coords
Coord::
operator + (Coords && other) const
{
    Coords result(*this);
    return result + std::move(other);
}

#if 0
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
#endif

#if 0
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
#endif

size_t
Coord::
memusage() const
{
    if (complex_)
        return sizeof(*this) + getComplex().rawLength();
    else return sizeof(*this);
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
    if (str.empty())
        throw HttpReturnException(400, "Attempt to create empty Coord");
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
    if (len == 0)
        throw HttpReturnException(400, "Attempt to create empty Coord");
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

#if 0
/** Compares two strings based upon natural ordering, whereby
    numbers after a dot are compared numerically not lexically.
*/
static strnverscmp(const char * s1, const char * s2, size_t len)
{
    if (len == 0)
        return 0;
    if (isdigit(*s1) && isdigit(*s2)) {
        // We are comparing numbers now
        const char * endn1 = s1 + 1;
        const char * endn2 = s2 + 1;
        size_t n = 1;

        while (n < len && isdigit(*endn1) && isdigit(*endn2)) {
            ++endn1;
            ++endn2;
        }

        // ...
    }

    if (*s1 < *s2)
        return -1;
    if (*s1 > *s2)
        return 1;
    return strnverscmp(s1 + 1, s2 + 1, len - 1);
}
#endif

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
/* COORDS                                                                    */
/*****************************************************************************/

/** A list of coordinate points that gives a full path to an entity. */

Coords::Coords()
{
}

Coords::Coords(Coord coord)
{
    if (coord.empty())
        throw HttpReturnException(400, "Attempt to create a column or row name with an empty element");
    emplace_back(std::move(coord));
}

Utf8String
Coords::
toSimpleName() const
{
    if (size() != 1)
        throw HttpReturnException(400, "Attempt to extract single name from multiple or empty coordinates: " + toUtf8String());
    return at(0).toUtf8String();
}

Utf8String
Coords::
toUtf8String() const
{
    Utf8String result;
    for (auto & c: *this) {
        if (!result.empty())
            result += '.';
        result += c.toEscapedUtf8String(); 
    }
    return result;
}

Coords
Coords::
parse(const Utf8String & val)
{
    Coords result;
    result.reserve(4);

    const char * p = val.rawData();
    const char * e = p + val.rawLength();

    if (p == e) {
        if (result.empty()) {
            throw HttpReturnException(400, "Parsing empty string for coord");
        }
    }

    auto parseOne = [&] () -> Coord
        {
            ExcAssert(p != e);

            if (*p == '\"') {
                Utf8String result;
                ++p;

                if (p == e) {
                    throw HttpReturnException(400, "Coords quoted incorrectly");
                }

                utf8::iterator<const char *> ufirst(p, p, e);
                utf8::iterator<const char *> ulast(e, p, e);

                while (ufirst != ulast) {
                    auto c = *ufirst++;
                    if (c == '\"') {
                        if (ufirst == ulast || *ufirst != '\"') {
                            p = ufirst.base();
                            if (result.empty()) {
                                throw HttpReturnException(400, "Empty quoted coord");
                            }

                            return result;
                        }
                        result += '\"';
                    }
                    else {
                        result += c;
                    }
                }
                throw HttpReturnException(400, "Coords terminated incorrectly");
            }
            else {
                const char * start = p;
                while (start < e && *start != '.') {
                    char c = *start++;
                    if (c == '\"' || c < ' ')
                        throw HttpReturnException(400, "invalid char in Coords");
                }
                size_t sz = start - p;
                if (sz == 0) {
                    throw HttpReturnException(400, "Empty coord");
                }
                Coord result(p, sz);
                p = start;
                return std::move(result);
            }
        };

    while (p < e) {
        result.emplace_back(parseOne());
        if (p < e) {
            if (*p != '.') {
                throw HttpReturnException(400, "expected '.' between elements in Coords, got " + to_string((int)*p),
                                          "position", p - val.rawData(),
                                          "val", val);
            }
            ++p;
        }
    }

    if (result.empty()) {
        throw HttpReturnException(400, "Coords were empty",
                                  "val", val);
    }
    
    return result;
}

Coords
Coords::
operator + (const Coords & other) const
{
    Coords result = *this;
    result.insert(result.end(), other.begin(), other.end());
    return result;
}

Coords
Coords::
operator + (Coords && other) const
{
    Coords result = *this;
    result.insert(result.end(),
                  std::make_move_iterator(other.begin()),
                  std::make_move_iterator(other.end()));
    return result;
}

Coords
Coords::
operator + (const Coord & other) const
{
    if (other.empty())
        throw HttpReturnException(400, "Attempt to create a column or row name with an empty element");
    Coords result = *this;
    result.push_back(other);
    return result;
}

Coords
Coords::
operator + (Coord && other) const
{
    if (other.empty())
        throw HttpReturnException(400, "Attempt to create a column or row name with an empty element");
    Coords result = *this;
    result.push_back(std::move(other));
    return result;
}

Coords::operator RowHash() const
{
    return RowHash(hash());
}

Coords::operator ColumnHash() const
{
    return ColumnHash(hash());
}

bool
Coords::
startsWith(const Coord & prefix) const
{
    if (empty())
        return false;
    return at(0) == prefix;
}

bool
Coords::
startsWith(const Coords & prefix) const
{
    if (size() < prefix.size())
        return false;
    return std::equal(begin(), begin() + prefix.size(),
                      prefix.begin());
}

Coords
Coords::
removePrefix(const Coord & prefix) const
{
    if (!startsWith(prefix))
        return *this;
    Coords result;
    result.insert(result.end(), begin() + 1, end());
    return result;
}

Coords
Coords::
removePrefix(const Coords & prefix) const
{
    if (!startsWith(prefix))
        return *this;
    return removePrefix(prefix.size());
}

Coords
Coords::
removePrefix(size_t n) const
{
    ExcAssertLessEqual(n, size());
    Coords result;
    result.insert(result.end(), begin() + n, end());
    return result;
}

Coords
Coords::
replacePrefix(const Coord & prefix, const Coords & newPrefix) const
{
    Coords result(newPrefix);
    result.insert(result.end(), begin() + 1, end());
    return result;
}

Coords
Coords::
replacePrefix(const Coords & prefix, const Coords & newPrefix) const
{
    Coords result(newPrefix);
    result.insert(result.end(), begin() + prefix.size(), end());
    return result;
}

bool
Coords::
matchWildcard(const Coords & wildcard) const
{
    if (wildcard.empty())
        return true;
    if (size() < wildcard.size())
        return false;
    for (ssize_t i = 0;  i < wildcard.size() - 1;  ++i) {
        if (at(i) != wildcard[i])
            return false;
    }
    
    return at(wildcard.size() - 1).startsWith(wildcard.back());
}

Coords
Coords::
replaceWildcard(const Coords & wildcard, const Coords & with) const
{
    if (wildcard.empty()) {
        if (with.empty())
            return *this;
        return with + *this;
    }

    if (size() < wildcard.size())
        return Coords();

    Coords result;
    for (ssize_t i = 0;  i < with.size() - 1;  ++i)
        result.push_back(with[i]);

    // The last one may be a prefix match, so we do it explicity
    Utf8String current = at(wildcard.size() - 1).toUtf8String();
    current.removePrefix(wildcard.back().toUtf8String());
    result.emplace_back(with.back().toUtf8String() + current);

    for (size_t i = wildcard.size();  i < size();  ++i)
        result.emplace_back(at(i));
    
    return result;
}   

uint64_t
Coords::
oldHash() const
{
    uint64_t result = 0;
    if (empty())
        return result;
    result = at(0).oldHash();
    for (size_t i = 1;  i < size();  ++i) {
        result = Hash128to64({result, at(i).newHash()});
    }
    return result;
}

uint64_t
Coords::
newHash() const
{
    uint64_t result = 0;
    if (empty())
        return result;
    result = at(0).newHash();
    for (size_t i = 1;  i < size();  ++i) {
        result = Hash128to64({result, at(i).newHash()});
    }
    return result;
}

size_t
Coords::
memusage() const
{
    size_t result = sizeof(*this) + (capacity() - size() * sizeof(Coord));
    for (auto & c: *this) {
        result += c.memusage();
    }
    return result;
}

std::ostream &
operator << (std::ostream & stream, const Coords & id)
{
    return stream << id.toUtf8String();
}

std::istream &
operator >> (std::istream & stream, Coords & id)
{
    throw HttpReturnException(600, "TODO: implement istream >> Coords");
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

struct CoordsDescription 
    : public ValueDescriptionI<Coords, ValueKind::ATOM, CoordsDescription> {

    virtual void parseJsonTyped(Coords * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const Coords * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const Coords * val) const;
};

DEFINE_VALUE_DESCRIPTION_NS(Coords, CoordsDescription);

void
CoordsDescription::
parseJsonTyped(Coords * val,
               JsonParsingContext & context) const
{
    if (context.isNull())
        *val = Coords();
    if (context.isInt())
        *val = Coord(context.expectInt());
    else if (context.isString())
        *val = Coords::parse(context.expectStringUtf8());
    else if (context.isArray()) {
        auto vec = jsonDecode<vector<Coord> >(context.expectJson());
        *val = Coords(std::make_move_iterator(vec.begin()),
                      std::make_move_iterator(vec.end()));
    }
    else throw HttpReturnException(400, "Unparseable JSON Coords value",
                                   "value", context.expectJson());
}

void
CoordsDescription::
printJsonTyped(const Coords * val,
               JsonPrintingContext & context) const
{
    context.writeStringUtf8(val->toUtf8String());
}

bool
CoordsDescription::
isDefaultTyped(const Coords * val) const
{
    return val->empty();
}

} // namespace MLDB
} // namespace Datacratic
