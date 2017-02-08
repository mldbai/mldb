/** path.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "path.h"
#include <cstring>
#include "mldb/types/hash_wrapper.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/ext/highwayhash.h"
#include "mldb/types/itoa.h"
#include "mldb/utils/json_utils.h"
#include "mldb/ext/cityhash/src/city.h"

using namespace std;



namespace MLDB {

namespace {
// If ever we allow the first offset of a path to be non-zero (eg, to tail
// a long path via sharing) we should remove this.
constexpr bool PATH_OFFSET_ZERO_IS_ALWAYS_ZERO = true;

inline void checkNullPathElement(bool lhsNull, bool rhsNull = false) {
    if (lhsNull) {
        throw HttpReturnException(
            500, "Cannot call operator + on null lhs PathElement");
    }
    if (rhsNull) {
        throw HttpReturnException(
            500, "Cannot call operator + on null rhs PathElement");
    }
}
} // file scope


/*****************************************************************************/
/* COMPARISON FUNCTIONS                                                      */
/*****************************************************************************/

/** Return a flag for what the mix of digits and non-digits is in a
    path element.
*/
int calcDigits(const char * begin, const char * end)
{
    bool hasDigit = false;
    bool hasNonDigit = false;
    
    for (const char * it = begin;  it != end;  ++it) {
        bool d = isdigit(*it);
        hasDigit = hasDigit || d;
        hasNonDigit = hasNonDigit || (!d);
    }

    return (hasDigit    * PathElement::DIGITS_ONLY)
        |  (hasNonDigit * PathElement::NO_DIGITS);
}

int calcDigits(const char * begin, size_t len)
{
    return calcDigits(begin, begin + len);
}

inline std::pair<size_t, size_t>
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
        // We need unsigned chars here, as this path is optimized with
        // memcmp in other places, which treats characters as unsigned.
        // Otherwise, with Unicode characters on platforms with signed
        // chars, we will get a different comparison order than the
        // optimized path, meaning that sort order is inconsistent.
        // See MLDB-1936
        unsigned char c1 = p1[i1], c2 = p2[i2];

        if (isdigit(c1) && isdigit(c2)) {
            size_t lz1, digits1, lz2, digits2;
            std::tie(lz1, digits1) = countDigits(p1 + i1, len1 - i1);
            std::tie(lz2, digits2) = countDigits(p2 + i2, len2 - i2);

            // More significant non-zero digits means bigger not matter what
            if (digits2 != digits1)
                return digits1 - digits2;

            // Same number of significant digits; compare the strings
            int res = std::strncmp(p1 + i1 + lz1, p2 + i2 + lz2, digits1);

            // If not the same return result
            if (res)
                return res;
            
            // Finally, the one with more significant digits is smaller
            if (lz1 != lz2)
                return lz2 - lz1;

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
            return (int)c1 - (int)c2;
        }
    }

    if (i1 == len1 && i2 == len2) {
        ExcAssertEqual(len1, len2);
        return 0;
    }

    return len1 - len2;
}

int
compareElements(const char * s0, size_t l0,
                const char * s1, size_t l1,
                int d0, int d1)
{
    if (MLDB_LIKELY(d0 == d1)) {
        if (d0 == PathElement::NO_DIGITS) {
            int res = std::memcmp(s0, s1, std::min(l0, l1));
            if (res == 0)
                res = l0 - l1;
            return res;
        }
        else if (d0 == PathElement::DIGITS_ONLY
                 && MLDB_LIKELY(s0[0] != '0' && s1[0] != '0')) {
            int res = 0;
            if (l0 != l1)
                res = l0 - l1;
            else {
                res = std::memcmp(s0, s1, l0);
                if (res == 0)
                    res = l0 - l1;
            }
            return res;
        }
        else if (l0 == l1 && std::memcmp(s0, s1, l0) == 0)
            return 0;
    }
    else {
        // Different digits.  They are definitely not equal.  Only 
        // need to determine which one is higher.
        // ...
    }

    return compareNatural(s0, l0, s1, l1);
}


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
PathElement(const char * str, size_t len, int digits)
{
#if 0
    if (digits != calcDigits(str, len)) {
        cerr << "for string '" << string(str, len) << "' with length " << len
             << ": digits = " << digits
             << endl;
    }
    ExcAssertEqual(digits, calcDigits(str, len));
#endif
    initChars(str, len, digits);
}

PathElement::
PathElement(uint64_t i)
{
    ItoaBuf buf;
    char * begin;
    char * end;
    std::tie(begin, end) = itoa(i, buf);
    initChars(begin, end - begin, DIGITS_ONLY);
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
        return { PathElement(""), true };
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
                else if (c == 0) {
                    if (exceptions) {
                        throw HttpReturnException
                            (400, "Paths cannot contain null characters");
                    }
                    else {
                        return { PathElement(), false };
                    }
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
                            (400, "invalid char in PathElement '"
                             + Utf8String(p, e)
                             + "'.  Quotes must be doubled.");
                    }
                    else {
                        return { PathElement(), false };
                    }
                }
                else {
                    if (exceptions) {
                        throw HttpReturnException
                            (400, "invalid char in PathElement '"
                             + Utf8String(p, e)
                             + "'.  Special characters must be quoted and "
                             "nulls are not accepted.");
                    }
                    else {
                        return { PathElement(), false };
                    }
                }
            }
        }
        size_t sz = start - p;
        if (sz == 0)
            return { PathElement(""), true };
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
    //ExcAssertEqual(digits_, calcDigits(data(), dataLength()));
    //ExcAssertEqual(other.digits_, calcDigits(other.data(), other.dataLength()));

    // We can use strcmp because the lengths and digits are the same, and we
    // are only looking for equality.
    return digits_ == other.digits_
        && dataLength() == other.dataLength()
        && strncmp(data(), other.data(), dataLength()) == 0;
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
    return compareElements(data(), dataLength(), other.data(), other.dataLength(),
                           digits_, other.digits_) < 0;
}

bool
PathElement::
operator <= (const PathElement & other) const
{
    return compareElements(data(), dataLength(), other.data(), other.dataLength(),
                           digits_, other.digits_) <= 0;
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
    if (null()) {
        return "";
    }
    if (complex_ == 1 && str.str.empty()) {
        return "\"\"";
    }

    const char * d = data();
    size_t l = dataLength();
    const char * e = d + l;

    auto isSimpleChar = [] (unsigned char c) -> bool
        {
            return c >= ' ' && c != '\"' && c != '.';
        };

    bool isSimple = l == 0 || isSimpleChar(d[0]);
    bool isUtf8 = false;
    for (size_t i = 0;  i < l && isSimple;  ++i) {
        if (d[i] & 128) {
            // high bit set; is UTF-8
            isUtf8 = true;
            break;
        }
        if (!isSimpleChar(d[i]) && d[i] != '_')
            isSimple = false;
    }

    if (isUtf8) {
        auto isSimpleUtf8 = [] (uint32_t c) -> bool
            {
                return c >= ' ' && c != '\"' && c != '.';
            };

        // Simple character detection doesn't work with UTF-8
        // Scan it UTF-8 character by UTF-8 character
        isSimple = true;
        utf8::iterator<const char *> ufirst(d, d, e);
        utf8::iterator<const char *> ulast(e, d, e);

        while (isSimple && ufirst != ulast) {
            auto c = *ufirst++;
            if (!isSimpleUtf8(c)) {
                isSimple = false;
            }
        }
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
    //ExcAssertEqual((int)digits_, calcDigits(data(), dataLength()));
    if (digits_ != DIGITS_ONLY)
        return -1;
    if (dataLength() > 12)
        return -1;
    uint64_t val = 0;
    const char * p = data();
    const char * e = p + dataLength();
    if (e == p)
        return -1;
    if (*p == '0' && dataLength() != 1)
        return -1;
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
    return CityHash64(data(), dataLength());
}

constexpr HashSeed defaultSeedStable { .i64 = { 0x1958DF94340e7cbaULL, 0x8928Fc8B84a0ULL } };

uint64_t
PathElement::
newHash() const
{
    return highwayHash(defaultSeedStable.u64, data(), dataLength());
}

Path
PathElement::
operator + (const PathElement & other) const
{
    checkNullPathElement(null(), other.null());
    PathBuilder builder;
    return builder.add(*this).add(other).extract();
}

Path
PathElement::
operator + (PathElement && other) const
{
    checkNullPathElement(null(), other.null());
    PathBuilder builder;
    return builder.add(*this).add(std::move(other)).extract();
}

Path
PathElement::
operator + (const Path & other) const
{
    checkNullPathElement(null());
    Path result(*this);
    return result + other;
}

Path
PathElement::
operator + (Path && other) const
{
    checkNullPathElement(null());
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
    digits_ = calcDigits(rawData(str), rawLength(str));
    if (rawLength(str) > 0 && rawLength(str) <= INTERNAL_BYTES - 1) {
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
initChars(const char * str, size_t len, int digits)
{
    //cerr << "str = " << string(str, str + len) << " len = " << len
    //     << " digits = " << digits << endl;
    //ExcAssert(digits != 0 || len == 0);
    //cerr << "len = " << len << endl;
    ExcAssertLess(len, 1ULL << 32);
    words[0] = words[1] = words[2] = 0;
    digits_ = digits;
    if (len > 0 && len <= INTERNAL_BYTES - 1) {
        complex_ = 0;
        simpleLen_ = len;
        std::copy(str, str + len, bytes + 1);
    }
    else {
        complex_ = 1;
        new (&this->str.str) Utf8String(str, len);
    }
}

void
PathElement::
initChars(const char * str, size_t len)
{
    return initChars(str, len, calcDigits(str, len));
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

int
PathElement::
compare(const PathElement & other) const
{
    return compareElements(data(), dataLength(),
                           other.data(), other.dataLength(),
                           digits_, other.digits_);
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
    : digits_(0)
{
    indexes.reserve(8);
    indexes.push_back(0);
}

PathBuilder &
PathBuilder::
add(PathElement && element)
{
    if (element.null()) {
        return *this;
    }

    if (bytes.empty() && element.hasExternalStorage()) {
        bytes = element.stealBytes();
    }
    else {
        auto v = element.getStringView();
        bytes.append(v.first, v.first + v.second);
    }
    
    if (indexes.size() <= 16) {
        //ExcAssertEqual(calcDigits(v.first, v.first + v.second), element.digits_);
        digits_ = digits_ | ((int)element.digits_ << (2 * (indexes.size() - 1)));
    }

    indexes.emplace_back(bytes.size());
    return *this;
}

PathBuilder &
PathBuilder::
add(const PathElement & element)
{
    if (element.null()) {
        return *this;
    }

    auto v = element.getStringView();
    bytes.append(v.first, v.first + v.second);
    if (indexes.size() <= 16) {
        //ExcAssertEqual(calcDigits(v.first, v.first + v.second), element.digits_);
        digits_ = digits_ | ((int)element.digits_ << (2 * (indexes.size() - 1)));
    }
    indexes.emplace_back(bytes.size());
    
    return *this;
}

PathBuilder &
PathBuilder::
add(const char * utf8Str, size_t charLength)
{
    bytes.append(utf8Str, utf8Str + charLength);
    if (indexes.size() <= 16) {
        digits_ = digits_ | (calcDigits(utf8Str, charLength) << (2 * (indexes.size() - 1)));
    }
    indexes.emplace_back(bytes.size());
    
    return *this;
}

PathBuilder &
PathBuilder::
addRange(const Path & path, size_t first, size_t last)
{
    ExcAssertLessEqual(first, last);
    if (path.empty() || first == last) {
        return *this;
    }

    if (last > path.size())
        last = path.size();
    if (first > last)
        first = last;
    size_t o1 = path.offset(first);
    size_t o2 = path.offset(last);
    const char * d = path.data();
    size_t beforeSize = bytes.size();
    
    // Do it all at once to reduce the amount of string manipulation
    bytes.append(d + o1, d + o2);
    for (size_t i = first;  i < last;  ++i) {
        if (indexes.size() <= 16) {
            digits_ = digits_ | (path.digits(i) << (2 * (indexes.size() - 1)));
        }
        indexes.emplace_back(path.offset(i + 1) - o1 + beforeSize);
    }
    ExcAssertEqual(indexes.back(), bytes.size());
    return *this;
}

Path
PathBuilder::
extract()
{
    // Clang has problems otherwise...
    bool isExternal = indexes.size() >= 9 || bytes.size() >= 256; 

    Path result;
    result.bytes_ = std::move(bytes);
    result.length_ = indexes.size() - 1;
    result.digits_ = digits_;

    //bool isExternal = result.externalOfs();

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
    : length_(1), digits_(path.digits_),
      ofsBits_(0)
{
    if (path.null()) {
        length_ = 0;
        return;
    }
    if (path.hasExternalStorage()) {
        bytes_ = path.stealBytes();
    }
    else {
        auto v = path.getStringView();
        bytes_.append(v.first, v.first + v.second);
    }
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
    : length_(1), digits_(path.digits_),
      ofsBits_(0)
{
    if (path.null()) {
        length_ = 0;
        return;
    }
    if (path.hasExternalStorage()) {
        bytes_ = path.getBytes();
    }
    else {
        auto v = path.getStringView();
        bytes_.append(v.first, v.first + v.second);
    }
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

ssize_t
Path::
toIndex() const
{
    if (length_ != 1 || digits(0) != PathElement::DIGITS_ONLY)
        return -1;
    return at(0).toIndex();
}

size_t
Path::
requireIndex() const
{
    ssize_t result = toIndex();
    if (result == -1)
        throw HttpReturnException(400, "Path was not an index");
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
        return { Path(""), true };
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
        builder.add(PathElement(""));
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
    checkNullPathElement(false, other.null());
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
    checkNullPathElement(false, other.null());
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
    result = oldHashElement(0);
    for (size_t i = 1;  i < size();  ++i) {
        result = Hash128to64({result, oldHashElement(i)});
    }
    return result;
}

uint64_t
Path::
newHash() const
{
    //auto startTicks = Date::now();
    uint64_t result = 0;
    if (empty())
        return result;
    result = newHashElement(0);
    for (size_t i = 1;  i < size();  ++i) {
        result = Hash128to64({result, newHashElement(i)});
    }
    //cerr << "newHash() of " << *this << " took "
    //     << Date::now().secondsSince(startTicks) * 1000000 << "us" << endl;
    return result;
}

uint64_t
Path::
oldHashElement(size_t el) const
{
    auto sv = getStringView(el);
    return CityHash64(sv.first, sv.second);
}

uint64_t
Path::
newHashElement(size_t el) const
{
    auto sv = getStringView(el);
    return highwayHash(defaultSeedStable.u64, sv.first, sv.second);
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
    return compareElement(el, other, otherEl) < 0;
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
    int d0 = digits(el);
    int d1 = other.digits(otherEl);

    return compareElements(s0, l0, s1, l1, d0, d1);
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
    if (digits_ != other.digits_)
        return false;

    // Short circuit (currently offset(0) is always 0, so always taken).
    if (PATH_OFFSET_ZERO_IS_ALWAYS_ZERO
        || (offset(0) == 0 && other.offset(0) == 0)) {
        for (size_t i = 1;  i <= length_;  ++i) {
            if (offset(i) != other.offset(i)) {
                return false;
            }
        }
        if (bytes_.size() != other.bytes_.size())
            return false;
        return std::memcmp(bytes_.data(), other.bytes_.data(), bytes_.size())
            == 0;
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
    if (val->null()) {
        context.writeNull();
        return;
    }
    int64_t index = val->toIndex();
    if (index != -1) {
        context.writeLongLong(index);
    }
    else {
        auto sv = val->getStringView();
        context.writeStringUtf8(sv.first, sv.second);
    }
}

bool
PathElementDescription::
isDefaultTyped(const PathElement * val) const
{
    return val->null();
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

