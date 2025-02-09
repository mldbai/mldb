/** path.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "mldb/types/string.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/base/exc_assert.h"
#include "mldb/utils/compact_vector.h"
#include "mldb/utils/interned_string.h"
#include <vector>
#include <cstring>
#include <iostream>
#include <utility>
#include <string_view>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

#pragma once


namespace MLDB {

struct Path;
std::ostream & operator << (std::ostream & stream, const Path & id);

template<int Domain> struct HashWrapper;

/*****************************************************************************/
/* PATH ELEMENT                                                              */
/*****************************************************************************/

/** This is a path element: a single discrete part of a path which acts
    like an atomic string (although integers can also be used, and will act
    like the string that represents them).

    A list of path elements is a path, which is used to name rows and columns
    in MLDB.

    It takes up 16 bytes.
*/

struct PathElement {
    PathElement();
    PathElement(const Utf8String & str);
    PathElement(Utf8String && str);
    PathElement(std::string str);
    PathElement(const char * str, size_t len);
    PathElement(const char * str, size_t len, int digits);

    PathElement(const std::string_view & str)
        : PathElement(str.data(), str.length())
    {
    }

    PathElement(const char * str)
        : PathElement(str, std::strlen(str))
    {
    }

    PathElement(const PathElement & other) = default;

    PathElement(PathElement && other) noexcept
        : storage_(std::move(other.storage_)), digits_(other.digits_)
    {
    }
    
    template<size_t N>
    inline PathElement(const char (&str)[N])
        : PathElement(str, (N && str[N - 1])?N:N-1)  // remove null char from end
    {
    }

    // Create as an array index, from any integral type
    template<typename T>
    PathElement(T i, typename std::enable_if<std::is_integral<T>::value>::type * = 0)
        : PathElement((uint64_t)i)
    {
    }

    PathElement(uint64_t i);

    static PathElement parse(const Utf8String & str);
    static PathElement parse(const char * p, size_t l);
    static PathElement parsePartial(const char * & p, const char * e);

    /** Attempt to parse.  Returns the element, plus a boolean flag which indicates
        whether it was successfully parsed or not.
    */
    static std::pair<PathElement, bool>
    tryParsePartial(const char * & p, const char * e, bool exceptions);

    ~PathElement()
    {
    }

    MLDB_ALWAYS_INLINE PathElement & operator = (PathElement && other) noexcept
    {
        storage_ = std::move(other.storage_);
        digits_ = other.digits_;
        return *this;
    }

    MLDB_ALWAYS_INLINE void swap(PathElement & other) noexcept
    {
        storage_.swap(other.storage_);
        std::swap(digits_, other.digits_);
    }

    PathElement & operator = (const PathElement & other) noexcept
    {
        storage_ = other.storage_;
        digits_ = other.digits_;
        return *this;
    }

    bool stringEqual(const std::string & other) const;
    bool stringEqual(const Utf8String & other) const;
    bool stringEqual(const char * other) const;

    bool stringLess(const std::string & other) const;
    bool stringLess(const Utf8String & other) const;
    bool stringLess(const char * other) const;

    bool stringGreaterEqual(const std::string & other) const;
    bool stringGreaterEqual(const Utf8String & other) const;
    bool stringGreaterEqual(const char * other) const;
    
    bool operator == (const PathElement & other) const;
    bool operator != (const PathElement & other) const;
    bool operator <  (const PathElement & other) const;
    bool operator <= (const PathElement & other) const;

    bool startsWith(const std::string & other) const;
    bool startsWith(const PathElement & other) const;
    bool startsWith(const char * other) const;
    bool startsWith(const Utf8String & other) const;


    Utf8String toUtf8String() const;
    Utf8String toEscapedUtf8String() const;

    std::string stealBytes();
    std::string getBytes() const;
    
    /** Returns if this is an index, that is a non-negative integer
        that can be converted into an array index.
    */
    bool isIndex() const;

    /** Convert to an integer, and return it.  If isIndex() is false,
        then this will return -1.
    */
    ssize_t toIndex() const;

    /** Convert to an integer, and return it.  If isIndex() is false,
        then this will throw an exception.
    */
    size_t requireIndex() const;

    /** If true, we can return a const char * and length that will
        live as long as this CellValue and can be used instead of
        creating a new string when printing.
    */
    bool hasStringView() const;

    /** Return a memory range that is a UTF-8 encoded version of
        this object's string representation.  Should throw if
        hasStringView() is false.
    */
    std::pair<const char *, size_t>
    getStringView() const;

    /// Forwarding function for the hash().  This will, one day soon,
    /// switch to the newHash() function.
    uint64_t hash() const
    {
        return oldHash();
    }

    /// Return the Id-compatible (old) hash.  Slower but compatible with
    /// legacy binaries.
    uint64_t oldHash() const;

    /// Return the non-Id compatible (new) hash.  Faster but not compatible
    /// with legacy hashes.
    uint64_t newHash() const;

    std::array<uint64_t, 2> newHash128() const;
    std::array<uint64_t, 4> newHash256() const;

    inline bool null() const
    {
        return storage_.empty();
    }

    bool empty() const
    {
        return storage_.length() == 1
            && storage_.data()[0] == '\0';
    }
    
    Path operator + (const PathElement & other) const;
    Path operator + (PathElement && other) const;
    Path operator + (const Path & other) const;
    Path operator + (Path && other) const;

    size_t memusage() const;
    
    //private:
    void complexDestroy();
    void complexCopyConstruct(const PathElement & other);
    void complexMoveConstruct(PathElement && other);

    template<typename Str>
    void initString(Str && str);
    template<typename Str>
    void initStringUnchecked(Str && str);

    void initChars(const char * str, size_t len);
    void initChars(const char * str, size_t len, int digits);

    const char * data() const;
    size_t dataLength() const;

    int compareString(const char * str, size_t len) const;
    int compareStringNullTerminated(const char * str) const;

    int compare(const PathElement & other) const;

    static constexpr int EMPTY = 0;
    static constexpr int DIGITS_ONLY = 1;
    static constexpr int NO_DIGITS = 2;
    static constexpr int SOME_DIGITS = 3;

    // A null value is stored with length 0
    // An empty value is stored with length 1 and the first character \0
    // Other values are stored as a normal string
    
    InternedString<27> storage_;
    uint8_t digits_;

} MLDB_PACKED;

std::ostream & operator << (std::ostream & stream, const PathElement & id);

std::istream & operator >> (std::istream & stream, PathElement & id);

inline bool operator == (const std::string & str1, const PathElement & str2)
{
    return str2.stringEqual(str1);
}

inline bool operator != (const std::string & str1, const PathElement & str2)
{
    return !str2.stringEqual(str1);
}

inline bool operator <  (const std::string & str1, const PathElement & str2)
{
    return str2.stringGreaterEqual(str1);
}

inline bool operator == (const Utf8String & str1, const PathElement & str2)
{
    return str2.stringEqual(str1.rawString());
}

inline bool operator != (const Utf8String & str1, const PathElement & str2)
{
    return !str2.stringEqual(str1.rawString());
}

inline bool operator <  (const Utf8String & str1, const PathElement & str2)
{
    return !str2.stringGreaterEqual(str1.rawString());
}

inline bool operator == (const PathElement & str1, const std::string & str2)
{
    return str1.stringEqual(str2);
}

inline bool operator != (const PathElement & str1, const std::string & str2)
{
    return !str1.stringEqual(str2);
}

inline bool operator <  (const PathElement & str1, const std::string & str2)
{
    return str1.stringLess(str2);
}

inline bool operator == (const PathElement & str1, const Utf8String & str2)
{
    return str1.stringEqual(str2.rawString());
}

inline bool operator != (const PathElement & str1, const Utf8String & str2)
{
    return !str1.stringEqual(str2.rawString());
}

inline bool operator <  (const PathElement & str1, const Utf8String & str2)
{
    return str1.stringLess(str2.rawString());
}

inline Utf8String keyToString(const PathElement & key)
{
    return key.toUtf8String();
}

inline PathElement stringToKey(const Utf8String & str, PathElement *)
{
    return PathElement(str);
}

inline PathElement stringToKey(const std::string & str, PathElement *)
{
    return PathElement(str);
}

PREDECLARE_VALUE_DESCRIPTION(PathElement);

struct PathElementNewHasher {
    size_t operator()(const MLDB::PathElement & path) const
    {
        return path.newHash();
    }
};


/*****************************************************************************/
/* PATH BUILDER                                                              */
/*****************************************************************************/

/** This structure is responsible for building paths in an optimal fashion. */

struct PathBuilder {
    PathBuilder();
    PathBuilder & add(PathElement && element);
    PathBuilder & add(const PathElement & element);
    PathBuilder & add(const char * utf8Start, size_t charLength);
    PathBuilder & addRange(const Path & path, size_t first, size_t last);
    Path extract();

private:
    compact_vector<uint32_t, 8> indexes;
    InternedString<244, char> bytes;
    uint32_t digits_;
};


/*****************************************************************************/
/* PATH                                                                      */
/*****************************************************************************/

/** A list of path elements points that gives a full path to an entity.
    Row and column names are paths.
*/

struct Path {
    Path()
        : length_(0), digits_(0), ofsPtr_(nullptr)
    {
    }

    Path(PathElement && path);
    Path(const PathElement & path);

    template<typename T>
    Path(const std::initializer_list<T> & val)
        : Path(val.begin(), val.end())
    {
    }

    Path(const PathElement * start, size_t len);

    template<typename It>
    Path(It first, It last)
        : Path()
    {
        PathBuilder result;
        while (first != last) {
            result.add(std::move(*first++));
        }
        *this = result.extract();
    }

    Path(const Path & other)
        : bytes_(other.bytes_),
          length_(other.length_),
          digits_(other.digits_),
          ofsBits_(other.ofsBits_)
    {
        if (MLDB_UNLIKELY(other.externalOfs())) {
            ofsPtr_ = new uint32_t[length_ + 1];
            ExcAssert(other.ofsPtr_);
            for (size_t i = 0;  i <= length_;  ++i) {
                ofsPtr_[i] = other.ofsPtr_[i];
            }
        }
    }

    Path(Path && other) noexcept
        : Path()
    {
        swap(other);
    }

    void swap(Path & other) noexcept
    {
        using std::swap;
        bytes_.swap(other.bytes_);
        swap(ofsBits_, other.ofsBits_);
        swap(length_, other.length_);
        swap(digits_, other.digits_);
    }

    Path & operator = (Path && other) noexcept
    {
        swap(other);
        return *this;
    }

    Path & operator = (const Path & other)
    {
        Path newMe(other);
        swap(newMe);
        return *this;
    }

    ~Path()
    {
        if (MLDB_UNLIKELY(externalOfs())) {
            delete[] ofsPtr_;
        }
    }

    struct Iterator {
        using iterator_category = std::random_access_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = PathElement;
        using pointer = const PathElement *;
        using reference = const PathElement &;

        const Path * p;
        size_t index;

        Iterator(const Path * p = nullptr, size_t index = 0)
            : p(p), index(index)
        {
        }

        PathElement operator * () const
        {
            ExcAssert(p);
            return p->at(index);
        }

        Iterator& operator ++ ()
        {
            ++index;
            return *this;
        }

        Iterator operator ++ (int)
        {
            Iterator result = *this;
            operator ++ ();
            return result;
        }

        std::ptrdiff_t operator - (const Iterator & other) const
        {
            return index - other.index;
        }

        Iterator operator + (std::ptrdiff_t offset) const
        {
            Iterator result = *this;
            result.index += offset;
            return result;
        }

        Iterator operator - (std::ptrdiff_t offset) const
        {
            Iterator result = *this;
            result.index -= offset;
            return result;
        }

        Iterator & operator += (std::ptrdiff_t offset)
        {
            index += offset;
            return *this;
        }

        bool operator == (const Iterator & other) const
        {
            return p == other.p && index == other.index;
        }

        bool operator != (const Iterator & other) const
        {
            return ! operator == (other);
        }

        bool operator < (const Iterator & other) const
        {
            // Not well defined if base pointers aren't the same
            ExcAssert(p == other.p);
            return index < other.index;
        }
    };

    Iterator begin() const
    {
        return Iterator(this, 0);
    }

    Iterator end() const
    {
        return Iterator(this, length_);
    }

    static Path parse(const Utf8String & str);
    static Path parse(const char * str, size_t len);

    /** Attempt to parse.  Returns the element, plus a boolean flag which indicates
        whether it was successfully parsed or not.
    */
    static std::pair<Path, bool> tryParse(const Utf8String & str);

    /** This function asserts that there is only a single element in
        the scope, and returns it as an Utf8String.  This is used
        for when we want to access the value as a simple, unadulterated
        variable or function name name.

        This will not escape any special characters, and so is not the same
        as toUtf8String(), and nor can the result of this version be
        re-parsed (as embedded dots will cause extra names to be created).
    */
    Utf8String toSimpleName() const;

    Utf8String toUtf8String() const;

    /** Returns if this is an index, that is a non-negative integer
        that can be converted into an array index.
    */
    bool isIndex() const
    {
        return length_ == 1
            && digits(0) == PathElement::DIGITS_ONLY
            && at(0).isIndex();
    }

    /** Convert to an integer, and return it.  If isIndex() is false,
        then this will return -1.
    */
    ssize_t toIndex() const;

    /** Convert to an integer, and return it.  If isIndex() is false,
        then this will throw an exception.
    */
    size_t requireIndex() const;


    Path operator + (const Path & other) const;
    Path operator + (Path && other) const;
    Path operator + (const PathElement & other) const;
    Path operator + (PathElement && other) const;

    operator HashWrapper<1>() const;
    operator HashWrapper<3>() const;

    bool startsWith(const PathElement & prefix) const;
    bool startsWith(const Path & prefix) const;

    bool matchWildcard(const Path & wildcard) const;
    Path replaceWildcard(const Path & wildcard, const Path & with) const;

    Path removePrefix(const PathElement & prefix) const;
    Path removePrefix(const Path & prefix) const;
    Path removePrefix(size_t n = 1) const;

    Path replacePrefix(const PathElement & prefix, const Path & newPrefix) const;
    Path replacePrefix(const Path & prefix, const Path & newPrefix) const;

    /// Forwarding function for the hash().  This will, one day soon,
    /// switch to the newHash() function.
    uint64_t hash() const
    {
        return oldHash();
    }

    /// Return the Id-compatible (old) hash.  Slower but compatible with
    /// legacy binaries.
    uint64_t oldHash() const;

    /// Return the non-Id compatible (new) hash.  Faster but not compatible
    /// with legacy hashes.
    uint64_t newHash() const;

    std::array<uint64_t, 2> newHash128() const;
    std::array<uint64_t, 4> newHash256() const;

    size_t size() const
    {
        return length_;
    }

    bool empty() const
    {
        return length_ == 0;
    }

    PathElement operator [] (size_t el) const
    {
        return at(el);
    }
    
    PathElement at(size_t el) const
    {
        const char * d = data();
        const char * b = d + offset(el);
        const char * e = d + offset(el + 1);
        if (MLDB_LIKELY(el < 16)) {
            return PathElement(b, e - b, digits(el));
        }
        else {
            return PathElement(b, e - b);
        }
    }

    PathElement front() const
    {
        return at(0);
    }

    PathElement back() const
    {
        return at(length_ - 1);
    }

    PathElement head() const
    {
        return front();
    }

    Path tail() const;

    bool operator == (const Path & other) const;
    bool operator != (const Path & other) const;
    bool operator < (const Path & other) const;
    bool operator <= (const Path & other) const;
    bool operator > (const Path & other) const;
    bool operator >= (const Path & other) const;

    int compare(const Path & other) const;

    size_t memusage() const;

    /// Return the range of bytes for the given element
    std::pair<const char *, size_t>
    getStringView(size_t el) const
    {
        size_t o0 = offset(el), o1 = offset(el + 1);
        return { data() + o0, o1 - o0 };
    }

private:
    friend struct PathBuilder;
    bool equalElement(size_t el, const Path & other, size_t otherEl) const;
    bool lessElement(size_t el, const Path & other, size_t otherEl) const;
    int compareElement(size_t el, const Path & other, size_t otherEl) const;

    uint64_t oldHashElement(size_t el) const;
    uint64_t newHashElement(size_t el) const;
    std::array<uint64_t, 2> newHashElement128(size_t el) const;
    std::array<uint64_t, 4> newHashElement256(size_t el) const;

    bool externalOfs() const
    {
        return length_ >= 8 || bytes_.size() >= 256;
    }
    
    const char * data() const
    {
        return bytes_.data();
    }

    /// Return the byte offsets of the begin and end of this element
    MLDB_ALWAYS_INLINE size_t offset(size_t el) const
    {
        //ExcAssertLessEqual(el, length_);
        if (MLDB_LIKELY(!externalOfs())) {
            return ofs_[el];
        }
        return ofsPtr_[el];
    }
    
    /// Return what the composition of the value at the given position is:
    /// does it contain no digits, only digits, or a mixture?  This is
    /// important to know as strings with only digits can be compared more
    /// efficiently (those without a leading zero whose length differs
    /// don't need any comparison at all), and strings without digits can
    /// be compared efficiently with memcmp.  This saves a big amount of
    /// runtime in reducing the expensive natural string ordering comparisons.
    inline int digits(size_t el) const
    {
        return
            (el < 16)
            ? (digits_ >> (2 * el)) & 3  // for the first 16, read directly
            : PathElement::SOME_DIGITS;  // for the rest, assume the worst
    }

    /** Internal parsing method.  Will attempt to parse the given range
        which is assumed to contain valid UTF-8 data, and will return
        the parsed version and true if valid, or if invalid either throw
        an exception (if exceptions is true) or return false in the
        second member.
    */
    static std::pair<Path, bool>
    parseImpl(const char * str, size_t len, bool exceptions);

    /// Encoded version of the string, not including separators
    InternedString<46, char> bytes_;

    /// Number of elements in the path
    uint32_t length_;

    /// Flags about digits per path element.  Contains 16 separate 2 bit
    /// flags, each of which has the following interpretation:
    /// 0 = (not set, invalid)
    /// 1 = only digits
    /// 2 = only non-digits
    /// 3 = digits and non-digits
    uint32_t digits_;
    
    /// Byte index of the first 8 components of the path, or a ptr to
    /// an array with all the rest if length_ > 8
    union {
        uint8_t ofs_[8];
        uint32_t * ofsPtr_;
        uint64_t ofsBits_;
    };
};

std::ostream & operator << (std::ostream & stream, const Path & id);

std::istream & operator >> (std::istream & stream, Path & id);

inline Utf8String keyToString(const Path & key)
{
    return key.toUtf8String();
}

inline Path stringToKey(const Utf8String & str, Path *)
{
    return Path(str);
}

inline Path stringToKey(const std::string & str, Path *)
{
    return Path(str);
}


PREDECLARE_VALUE_DESCRIPTION(Path);

struct PathNewHasher {
    size_t operator()(const MLDB::Path & path) const
    {
        return path.newHash();
    }
};

inline Path restDecode(const Utf8String & val, Path *)
{
    return Path(val);
}

} // namespace MLDB


namespace std {

template<>
struct hash<MLDB::PathElement> {
    size_t operator()(const MLDB::PathElement & path) const
    {
        return path.hash();
    }
};

template<>
struct hash<MLDB::Path> {
    size_t operator()(const MLDB::Path & paths) const
    {
        return paths.hash();
    }
};

} // namespace std
