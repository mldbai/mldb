/** path.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "path.h"
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
/* PATH ELEMENT                                                              */
/*****************************************************************************/

PathElement::
PathElement()
    : words{0, 0, 0}
{
}

PathElement::
PathElement(Utf8String && str)
{
    initString(std::move(str));
}

PathElement::
PathElement(const Utf8String & str)
{
    initString(std::move(str));
}

PathElement::
PathElement(std::string str)
{
    initString(std::move(str));
}

PathElement::
PathElement(const char * str, size_t len)
{
    initChars(str, len);
}

PathElement::
PathElement(uint64_t i)
{
    ItoaBuf buf;
    char * begin;
    char * end;
    std::tie(begin, end) = itoa(i, buf);
    initChars(begin, end - begin);
}

PathElement
PathElement::
parse(const Utf8String & str)
{
    return parse(str.rawData(), str.rawLength());
}

PathElement
PathElement::
parse(const char * p, size_t l)
{
    const char * e = p + l;
    PathElement result = parsePartial(p, e);
    if (p != e)
        throw HttpReturnException(400, "PathElement had extra characters at end");
    return result;
}

std::pair<PathElement, bool>
PathElement::
tryParsePartial(const char * & p, const char * e, bool exceptions)
{
    ExcAssertLessEqual((void *)p, (void *)e);

    if (p == e) {
        return { PathElement(), true };
    }

    if (*p == '\"') {
        Utf8String result;
        ++p;

        if (p == e) {
            if (exceptions)
                throw HttpReturnException(400, "Path quoted incorrectly");
            else return { PathElement(), false };
        }

        try {
            utf8::iterator<const char *> ufirst(p, p, e);
            utf8::iterator<const char *> ulast(e, p, e);

            while (ufirst != ulast) {
                auto c = *ufirst++;
                if (c == '\"') {
                    if (ufirst == ulast || *ufirst != '\"') {
                        p = ufirst.base();
                        return { std::move(result), true };
                    }
                    result += '\"';
                    ++ufirst;  // skip the second quote
                }
                else {
                    result += c;
                }
            }
        } catch (const utf8::exception & exc) {
            if (exceptions)
                throw;
            else return { PathElement(), false };
        }

        if (exceptions)
            throw HttpReturnException(400, "PathElement terminated incorrectly");

        else return { PathElement(), false };
    }
    else {
        const char * start = p;
        while (start < e && *start != '.') {
            unsigned char c = *start++;
            if (c == '\"' || c < ' ') {
                if (c == '\"') {
                    if (exceptions) {
                        throw HttpReturnException
                            (400, "invalid char in PathElement '" + Utf8String(start, e)
                             + "'.  Quotes must be doubled.");
                    }
                    else {
                        return { PathElement(), false };
                    }
                }
                else {
                    if (exceptions) {
                        throw HttpReturnException
                            (400, "invalid char in PathElement '" + Utf8String(start, e)
                             + "'.  Special characters must be quoted.");
                    }
                    else {
                        return { PathElement(), false };
                    }
                }
            }
        }
        size_t sz = start - p;
        if (sz == 0)
            return { PathElement(), true };
        PathElement result(p, sz);
        p = start;
        return { std::move(result), true };
    }
}

PathElement
PathElement::
parsePartial(const char * & p, const char * e)
{
    return tryParsePartial(p, e, true /* exceptions */).first;
}

bool
PathElement::
stringEqual(const std::string & other) const
{
    return dataLength() == other.size()
        && compareString(other.data(), other.size()) == 0;
}

bool
PathElement::
stringEqual(const Utf8String & other) const
{
    return dataLength() == other.rawLength()
        && compareString(other.rawData(), other.rawLength()) == 0;
}

bool
PathElement::
stringEqual(const char * other) const
{
    return compareStringNullTerminated(other) == 0;
}

bool
PathElement::
stringLess(const std::string & other) const
{
    return compareString(other.data(), other.size()) < 0;
}

bool
PathElement::
stringLess(const Utf8String & other) const
{
    return compareString(other.rawData(), other.rawLength()) < 0;
}

bool
PathElement::
stringLess(const char * other) const
{
    return compareStringNullTerminated(other) < 0;
}

bool
PathElement::
stringGreaterEqual(const std::string & other) const
{
    return compareString(other.data(), other.size()) >= 0;
}

bool
PathElement::
stringGreaterEqual(const Utf8String & other) const
{
    return compareString(other.rawData(), other.rawLength()) >= 0;
}

bool
PathElement::
stringGreaterEqual(const char * other) const
{
    return compareStringNullTerminated(other) >= 0;
}
    
bool
PathElement::
operator == (const PathElement & other) const
{
    return dataLength() == other.dataLength()
        && compareString(other.data(), other.dataLength()) == 0;
}

bool
PathElement::
operator != (const PathElement & other) const
{
    return ! operator == (other);
}

bool
PathElement::
operator <  (const PathElement & other) const
{
    return compareString(other.data(), other.dataLength()) < 0;
}

bool
PathElement::
startsWith(const std::string & other) const
{
    return toUtf8String().startsWith(other);
}

bool
PathElement::
startsWith(const PathElement & other) const
{
    return toUtf8String().startsWith(other.toUtf8String());
}

bool
PathElement::
startsWith(const char * other) const
{
    return toUtf8String().startsWith(other);
}

bool
PathElement::
startsWith(const Utf8String & other) const
{
    return toUtf8String().startsWith(other);
}

Utf8String
PathElement::
toUtf8String() const
{
    if (complex_)
        return getComplex();
    else return Utf8String(data(), dataLength(), true /* is valid UTF-8 */);
}

Utf8String
PathElement::
toEscapedUtf8String() const
{
    if (empty())
        return "\"\"";

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
PathElement::
isIndex() const
{
    return toIndex() != -1;
}

ssize_t
PathElement::
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
PathElement::
hasStringView() const
{
    return true;  // currently we store as a string, so always true
}

std::pair<const char *, size_t>
PathElement::
getStringView() const
{
    return { data(), dataLength() };
}

uint64_t
PathElement::
oldHash() const
{
    return Id(data(), dataLength()).hash();
}

constexpr HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

uint64_t
PathElement::
newHash() const
{
    return ::mldb_siphash24(data(), dataLength(), defaultSeedStable.b);
}

Path
PathElement::
operator + (const PathElement & other) const
{
    Path result(*this);
    return result + other;
}

Path
PathElement::
operator + (PathElement && other) const
{
    Path result(*this);
    return result + std::move(other);
}

Path
PathElement::
operator + (const Path & other) const
{
    Path result(*this);
    return result + other;
}

Path
PathElement::
operator + (Path && other) const
{
    Path result(*this);
    return result + std::move(other);
}

std::string
PathElement::
getBytes() const
{
    if (complex_)
        return str.str.rawString();
    else return std::string(data(), data() + dataLength());
}

std::string
PathElement::
stealBytes()
{
    if (complex_)
        return str.str.stealRawString();
    else return std::string(data(), data() + dataLength());
}


#if 0
PathElement
PathElement::
operator + (const PathElement & other) const
{
    size_t l1 = dataLength();
    size_t l2 = other.dataLength();

    if (l1 == 0)
        return other;
    if (l2 == 0)
        return *this;

    size_t len = 1 + l1 + l2;

    PathElement result;

    if (len <= INTERNAL_BYTES - 1) {
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

PathElement
PathElement::
operator + (PathElement && other) const
{
    if (empty())
        return std::move(other);
    return operator + ((const PathElement &)other);
}
#endif

#if 0
PathElement::
operator RowHash() const
{
    return RowHash(hash());
}

PathElement::
operator ColumnHash() const
{
    return ColumnHash(hash());
}
#endif

size_t
PathElement::
memusage() const
{
    if (complex_)
        return sizeof(*this) + getComplex().rawLength();
    else return sizeof(*this);
}

void
PathElement::
complexDestroy()
{
    getComplex().~Utf8String();
}

void
PathElement::
complexCopyConstruct(const PathElement & other)
{
    new (&str.str) Utf8String(other.getComplex());
}

void
PathElement::
complexMoveConstruct(PathElement && other)
{
    new (&str.str) Utf8String(std::move(other.getComplex()));
}

namespace {

const char * rawData(const Utf8String & str)
{
    return str.rawData();
}

size_t rawLength(const Utf8String & str)
{
    return str.rawLength();
}

const char * rawData(const std::string & str)
{
    return str.data();
}

size_t rawLength(const std::string & str)
{
    return str.size();
}



} // file scope

template<typename T>
void
PathElement::
initString(T && str)
{
    ExcAssertEqual(strlen(rawData(str)), rawLength(str));
    initStringUnchecked(std::move(str));
}

template<typename T>
void
PathElement::
initStringUnchecked(T && str)
{
    // This method is used only for when we know we may have invalid
    // characters, for example when importing legacy files.
    words[0] = words[1] = words[2] = 0;
    if (rawLength(str) <= INTERNAL_BYTES - 1) {
        complex_ = 0;
        simpleLen_ = rawLength(str);
        std::copy(rawData(str), rawData(str) + rawLength(str),
                  bytes + 1);
    }
    else {
        complex_ = 1;
        new (&this->str.str) Utf8String(std::move(str));
    }
}

template void PathElement::initStringUnchecked<Utf8String>(Utf8String && str);
template void PathElement::initStringUnchecked<const Utf8String &>(const Utf8String & str);
template void PathElement::initStringUnchecked<std::string>(std::string && str);
template void PathElement::initStringUnchecked<const std::string &>(const std::string & str);

void
PathElement::
initChars(const char * str, size_t len)
{
    //cerr << "len = " << len << endl;
    ExcAssertLess(len, 1ULL << 32);
    words[0] = words[1] = words[2] = 0;
    if (len <= INTERNAL_BYTES - 1) {
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
PathElement::
data() const
{
    if (complex_)
        return getComplex().rawData();
    else return (const char *)bytes + 1;
}

size_t
PathElement::
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

std::pair<size_t, size_t>
countDigits(const char * p, size_t len)
{
    // Count leading zeros
    size_t lz = 0;
    size_t i = 0;
    while (i < len && p[i] == '0') {
        ++i;
        ++lz;
    }

    // If we're at the end, then we have only zeros,
    // followed by one significant figure (the zero)
    if (i == len || !isdigit(p[i])) {
        return { i - 1, 1 };
    }

    // Otherwise, count digits
    while (i < len && isdigit(p[i]))
        ++i;
                    
    return { lz, i - lz };
}

/** Compare two UTF-8 encoded strings, with numeric ranges sorting in
    natural order.
*/
int
compareNatural(const char * p1, size_t len1,
               const char * p2, size_t len2)
{
    size_t i1 = 0, i2 = 0;
    
    while (i1 < len1 && i2 < len2) {
        char c1 = p1[i1], c2 = p2[i2];

        if (isdigit(c1) && isdigit(c2)) {
            size_t lz1, digits1, lz2, digits2;
            std::tie(lz1, digits1) = countDigits(p1 + i1, len1 - i1);
            std::tie(lz2, digits2) = countDigits(p2 + i2, len2 - i2);

            // More significant non-zero digits means bigger not matter what
            if (digits1 < digits2)
                return -1;
            else if (digits1 > digits2)
                return 1;

            // Same number of significant digits; compare the strings
            int res = std::strncmp(p1 + i1 + lz1, p2 + i2 + lz2, digits1);

            // If not the same return result
            if (res)
                return res;
            
            // Finally, the one with more significant digits is smaller
            if (lz1 != lz2)
                return lz1 < lz2 ? 1 : -1;

            // Out of the run of digits... update the pointers
            ExcAssertEqual(lz1 + digits1, lz2 + digits2);
            i1 += lz1 + digits1;
            i2 += lz2 + digits2;
        }
        else if (c1 == c2) {
            // Not both digits but equal; continue
            ++i1;
            ++i2;
        }
        else {
            // Not both digits and unequal
            return c1 < c2 ? -1 : 1;
        }
    }

    if (i1 == len1 && i2 == len2) {
        ExcAssertEqual(len1, len2);
        return 0;
    }

    return len1 < len2 ? -1 : 1;
}

int
PathElement::
compareString(const char * str, size_t len) const
{
    const char * p1 = data();
    size_t len1 = dataLength();
    const char * p2 = str;
    size_t len2 = len;
    
    return compareNatural(p1, len1, p2, len2);
}

int
PathElement::
compareStringNullTerminated(const char * str) const
{
    return compareString(str, ::strlen(str));
}

const Utf8String &
PathElement::
getComplex() const
{
    ExcAssert(complex_);
    return str.str;
}

Utf8String &
PathElement::
getComplex()
{
    ExcAssert(complex_);
    return str.str;
}

std::ostream & operator << (std::ostream & stream, const PathElement & path)
{
    return stream << path.toUtf8String();
}

std::istream & operator >> (std::istream & stream, PathElement & path)
{
    std::string s;
    stream >> s;
    path = PathElement(std::move(s));
    return stream;
}


/*****************************************************************************/
/* PATH BUILDER                                                              */
/*****************************************************************************/

PathBuilder::
PathBuilder()
{
    indexes.reserve(8);
    indexes.push_back(0);
}

PathBuilder &
PathBuilder::
add(PathElement && element)
{
    if (bytes.empty()) {
        bytes = element.stealBytes();
    }
    else {
        auto v = element.getStringView();
        bytes.append(v.first, v.first + v.second);
    }
    indexes.emplace_back(bytes.size());
    return *this;
}

PathBuilder &
PathBuilder::
add(const PathElement & element)
{
    auto v = element.getStringView();
    bytes.append(v.first, v.first + v.second);
    indexes.emplace_back(bytes.size());

    return *this;
}

PathBuilder &
PathBuilder::
addRange(const Path & path, size_t first, size_t last)
{
    if (last > path.size())
        last = path.size();
    if (first > last)
        first = last;
    for (auto it = path.begin() + first, end = path.begin() + last;
         it < end;  ++it) {
        add(*it);
    }
    return *this;
}

Path
PathBuilder::
extract()
{
    Path result;
    result.bytes_ = std::move(bytes);
    result.length_ = indexes.size() - 1;

    bool isExternal = result.externalOfs();

    if (isExternal) {
        result.ofsPtr_ = new uint32_t[indexes.size()];
        std::copy(indexes.begin(), indexes.end(), result.ofsPtr_);
    }
    else {
        std::copy(indexes.begin(), indexes.end(), result.ofs_);
    }

    return result;
}


/*****************************************************************************/
/* PATH                                                                      */
/*****************************************************************************/

Path::Path(PathElement && path)
    : length_(1), ofsBits_(0)
{
    if (path.empty()) {
        length_ = 0;
        return;
    }
    bytes_ = path.stealBytes();
    if (externalOfs()) {
        ofsPtr_ = new uint32_t[2];
        ofsPtr_[0] = 0;
        ofsPtr_[1] = bytes_.size();
    }
    else {
        ofs_[0] = 0;
        ofs_[1] = bytes_.size();
    }
}

Path::Path(const PathElement & path)
    : length_(1), ofsBits_(0)
{
    if (path.empty()) {
        length_ = 0;
        return;
    }
    bytes_ = path.getBytes();
    if (externalOfs()) {
        ofsPtr_ = new uint32_t[2];
        ofsPtr_[0] = 0;
        ofsPtr_[1] = bytes_.size();
    }
    else {
        ofs_[0] = 0;
        ofs_[1] = bytes_.size();
    }
}

Path::
Path(const PathElement * start, size_t len)
    : Path(start, start + len)
{
}

Utf8String
Path::
toSimpleName() const
{
    if (size() != 1)
        throw HttpReturnException(400, "Attempt to extract single name from multiple or empty path: " + toUtf8String());
    return at(0).toUtf8String();
}

Utf8String
Path::
toUtf8String() const
{
    Utf8String result;
    bool first = true;
    for (size_t i = 0;  i < length_;  ++i) {
        if (!first)
            result += '.';
        result += at(i).toEscapedUtf8String(); 
        first = false;
    }
    return result;
}

std::pair<Path, bool>
Path::
parseImpl(const char * str, size_t len, bool exceptions)
{
    const char * p = str;
    const char * e = p + len;

    PathBuilder builder;

    if (p == e) {
        return { Path(), true };
    }

    while (p < e) {
        bool valid;
        PathElement el;
        std::tie(el, valid) = PathElement::tryParsePartial(p, e, exceptions);
        if (!valid) {
            return { Path(), false };
        }
        builder.add(std::move(el));

        if (p < e) {
            if (*p != '.') {
                if (exceptions) {
                    throw HttpReturnException
                        (400,
                         "expected '.' between elements in Path, got Unicode "
                         + to_string((int)*p),
                         "position", p - str,
                         "val", Utf8String(str, len));
                }
                else {
                    return { Path(), false };
                }
            }
            ++p;
        }
    }

    if (str != e && e[-1] == '.') {
        builder.add(PathElement());
    }

    return { builder.extract(), true };
}

std::pair<Path, bool>
Path::
tryParse(const Utf8String & str)
{
    return parseImpl(str.rawData(), str.rawLength(), false /* exceptions */);
}

Path
Path::
parse(const char * str, size_t len)
{
    return parseImpl(str, len, true /* exceptions */).first;
}

Path
Path::
parse(const Utf8String & val)
{
    return parse(val.rawData(), val.rawLength());
}

Path
Path::
tail() const
{
    if (length_ == 0)
        throw HttpReturnException(500, "Attempt to tail empty path");
    if (length_ == 1)
        return Path();

    PathBuilder result;
    return result.addRange(*this, 1, size()).extract();
}

Path
Path::
operator + (const Path & other) const
{
    PathBuilder result;
    return result
        .addRange(*this, 0, size())
        .addRange(other, 0, other.size())
        .extract();
}

Path
Path::
operator + (Path && other) const
{
    PathBuilder result;
    return result
        .addRange(*this, 0, size())
        .addRange(std::move(other), 0, other.size())
        .extract();
}

Path
Path::
operator + (const PathElement & other) const
{
    PathBuilder result;
    return result
        .addRange(*this, 0, size())
        .add(std::move(other))
        .extract();
}

Path
Path::
operator + (PathElement && other) const
{
    PathBuilder result;
    return result
        .addRange(*this, 0, size())
        .add(std::move(other))
        .extract();
}

Path::operator RowHash() const
{
    return RowHash(hash());
}

Path::operator ColumnHash() const
{
    return ColumnHash(hash());
}

bool
Path::
startsWith(const PathElement & prefix) const
{
    if (empty())
        return false;
    return at(0) == prefix;
}

bool
Path::
startsWith(const Path & prefix) const
{
    if (size() < prefix.size())
        return false;
    for (size_t i = 0;  i < prefix.size();  ++i) {
        if (!equalElement(i, prefix, i))
            return false;
    }
    return true;
}

Path
Path::
removePrefix(const PathElement & prefix) const
{
    if (!startsWith(prefix))
        return *this;
    PathBuilder result;
    result.addRange(*this, 1, size());
    return result.extract();
}

Path
Path::
removePrefix(const Path & prefix) const
{
    if (!startsWith(prefix))
        return *this;
    PathBuilder result;
    result.addRange(*this, prefix.size(), size());
    return result.extract();
}

Path
Path::
removePrefix(size_t n) const
{
    ExcAssertLessEqual(n, size());
    PathBuilder result;
    return result.addRange(*this, n, size()).extract();
}

Path
Path::
replacePrefix(const PathElement & prefix, const Path & newPrefix) const
{
    PathBuilder result;
    return result
        .addRange(newPrefix, 0, newPrefix.size())
        .addRange(*this, 1, size())
        .extract();
}

Path
Path::
replacePrefix(const Path & prefix, const Path & newPrefix) const
{
    PathBuilder result;
    return result
        .addRange(newPrefix, 0, newPrefix.size())
        .addRange(*this, prefix.size(), size())
        .extract();
}

bool
Path::
matchWildcard(const Path & wildcard) const
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

Path
Path::
replaceWildcard(const Path & wildcard, const Path & with) const
{
    if (wildcard.empty()) {
        if (with.empty())
            return *this;
        return with + *this;
    }

    if (size() < wildcard.size())
        return Path();

    PathBuilder result;
    for (ssize_t i = 0;  i < (ssize_t)(with.size()) - 1;  ++i)
        result.add(with[i]);

    // The last one may be a prefix match, so we do it explicity
    Utf8String current = at(wildcard.size() - 1).toUtf8String();
    current.removePrefix(wildcard.back().toUtf8String());
    if (!with.empty())
        result.add(with.back().toUtf8String() + current);

    for (size_t i = wildcard.size();  i < size();  ++i)
        result.add(at(i));
    
    return result.extract();
}   

uint64_t
Path::
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
Path::
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
Path::
memusage() const
{
    size_t result = sizeof(*this) + bytes_.size();  // todo: extra length bytes
    return result;
}

bool
Path::
equalElement(size_t el, const Path & other, size_t otherEl) const
{
    const char * s0;
    size_t l0;
    const char * s1;
    size_t l1;
    
    std::tie(s0, l0) = getStringView(el);
    std::tie(s1, l1) = other.getStringView(otherEl);

    if (l0 != l1)
        return false;
    return strncmp(s0, s1, l0) == 0;
}

bool
Path::
lessElement(size_t el, const Path & other, size_t otherEl) const
{
    return compareElement(el, other, otherEl) == -1;
}

int
Path::
compareElement(size_t el, const Path & other, size_t otherEl) const
{
    const char * s0;
    size_t l0;
    const char * s1;
    size_t l1;
    
    std::tie(s0, l0) = getStringView(el);
    std::tie(s1, l1) = other.getStringView(otherEl);

    return compareNatural(s0, l0, s1, l1);
}

int
Path::
compare(const Path & other) const
{
    for (size_t i = 0; i < length_ && i < other.length_; ++i) {
        int cmp = compareElement(i, other, i);
        if (cmp)
            return cmp;
    }

    return length_ - other.length_;
}

bool
Path::
operator == (const Path & other) const
{
    if (length_ != other.length_) {
        return false;
    }

    // Short circuit (currently offset(0) is always 0, so always taken).
    if (/*offset(0) == 0 && other.offset(0) == 0*/ true) {
        for (size_t i = 1;  i <= length_;  ++i) {
            if (offset(i) != other.offset(i)) {
                return false;
            }
        }
        if (bytes_.size() != other.bytes_.size())
            return false;
        return std::memcmp(bytes_.data(), other.bytes_.data(), bytes_.size()) == 0;
    }

    return compare(other) == 0;
}

bool
Path::
operator != (const Path & other) const
{
    return ! operator == (other);
}

bool
Path::
operator < (const Path & other) const
{
    return compare(other) < 0;
}

bool
Path::
operator <= (const Path & other) const
{
    return compare(other) <= 0;
}

bool
Path::
operator > (const Path & other) const
{
    return compare(other) > 0;
}

bool
Path::
operator >= (const Path & other) const
{
    return compare(other) >= 0;
}

std::ostream &
operator << (std::ostream & stream, const Path & id)
{
    return stream << id.toUtf8String();
}

std::istream &
operator >> (std::istream & stream, Path & id)
{
    throw HttpReturnException(600, "TODO: implement istream >> Path");
}


/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

struct PathElementDescription 
    : public ValueDescriptionI<PathElement, ValueKind::ATOM, PathElementDescription> {

    virtual void parseJsonTyped(PathElement * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const PathElement * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const PathElement * val) const;
};

DEFINE_VALUE_DESCRIPTION_NS(PathElement, PathElementDescription);

void
PathElementDescription::
parseJsonTyped(PathElement * val,
               JsonParsingContext & context) const
{
    if (context.isNull())
        *val = PathElement();
    else if (context.isString())
        *val = context.expectStringUtf8();
    else {
        *val = context.expectJson().toStringNoNewLine();
    }
}

void
PathElementDescription::
printJsonTyped(const PathElement * val,
               JsonPrintingContext & context) const
{
    context.writeJson(jsonEncode(Id(val->toUtf8String())));
    //context.writeStringUtf8(val->toUtf8String());
}

bool
PathElementDescription::
isDefaultTyped(const PathElement * val) const
{
    return val->empty();
}

struct PathDescription 
    : public ValueDescriptionI<Path, ValueKind::ATOM, PathDescription> {

    virtual void parseJsonTyped(Path * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const Path * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const Path * val) const;
};

DEFINE_VALUE_DESCRIPTION_NS(Path, PathDescription);

void
PathDescription::
parseJsonTyped(Path * val,
               JsonParsingContext & context) const
{
    if (context.isNull())
        *val = Path();
    if (context.isInt())
        *val = PathElement(context.expectInt());
    else if (context.isString())
        *val = Path::parse(context.expectStringUtf8());
    else if (context.isArray()) {
        auto vec = jsonDecode<vector<PathElement> >(context.expectJson());
        *val = Path(std::make_move_iterator(vec.begin()),
                      std::make_move_iterator(vec.end()));
    }
    else throw HttpReturnException(400, "Unparseable JSON Path value",
                                   "value", context.expectJson());
}

void
PathDescription::
printJsonTyped(const Path * val,
               JsonPrintingContext & context) const
{
    //vector<PathElement> v(val->begin(), val->end());
    //context.writeJson(jsonEncode(v));
    context.writeStringUtf8(val->toUtf8String());
}

bool
PathDescription::
isDefaultTyped(const Path * val) const
{
    return val->empty();
}

} // namespace MLDB
} // namespace Datacratic
