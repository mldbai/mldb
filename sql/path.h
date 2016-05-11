/** path.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "dataset_fwd.h"
#include "mldb/types/string.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/compact_vector.h"
#include <vector>
#include <cstring>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

#pragma once

namespace Datacratic {
namespace MLDB {

struct Path;

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
    PathElement(const char * str)
        : PathElement(str, std::strlen(str))
    {
    }

    PathElement(const PathElement & other)
        : words{other.words[0], other.words[1], other.words[2] }
    {
        if (other.complex_) {
            complexCopyConstruct(other);
        }
    }

    PathElement(PathElement && other) noexcept
        : words{other.words[0], other.words[1], other.words[2] }
    {
        if (other.complex_) {
            complexMoveConstruct(std::move(other));
        }
        else {
            other.words[0] = 0;
        }
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

    ~PathElement()
    {
        if (complex_)
            complexDestroy();
    }

    PathElement & operator = (PathElement && other) noexcept
    {
        PathElement newMe(std::move(other));
        swap(newMe);
        return *this;
    }

    void swap(PathElement & other) noexcept
    {
        // NOTE: this is only possible because there are no self-referential
        // pointers (ie, this can't point to itself in its contents).
        std::swap(words[0], other.words[0]);
        std::swap(words[1], other.words[1]);
        std::swap(words[2], other.words[2]);
    }

    PathElement & operator = (const PathElement & other) noexcept
    {
        PathElement newMe(other);
        swap(newMe);
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

    bool startsWith(const std::string & other) const;
    bool startsWith(const PathElement & other) const;
    bool startsWith(const char * other) const;
    bool startsWith(const Utf8String & other) const;


    Utf8String toUtf8String() const;

    Utf8String toEscapedUtf8String() const;

    /** Returns if this is an index, that is a non-negative integer
        (possibly with leading zeros) that can be converted into an
        array index.
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

    inline bool empty() const
    {
        return complex_ == 0 && simpleLen_ == 0;
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

    const char * data() const;
    size_t dataLength() const;

    int compareString(const char * str, size_t len) const;
    int compareStringNullTerminated(const char * str) const;

    const Utf8String & getComplex() const;
    Utf8String & getComplex();

    struct Itl;

    struct Str {
        uint64_t md;
        Utf8String str;
        uint64_t savedHash;
    };

    static constexpr size_t INTERNAL_WORDS = 3;
    static constexpr size_t INTERNAL_BYTES = 8 * INTERNAL_WORDS;

    union {
        // The complex_ flag means we can't simply copy the words around;
        // we need to do some more work.
        struct { uint8_t complex_: 1; uint8_t simpleLen_:5; };
        uint8_t bytes[INTERNAL_BYTES];
        uint64_t words[INTERNAL_WORDS];
        Str str;
    };
};

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

struct PathElementNewHasher
    : public std::unary_function<Datacratic::MLDB::PathElement, size_t>
{
    size_t operator()(const Datacratic::MLDB::PathElement & path) const
    {
        return path.newHash();
    }
};


/*****************************************************************************/
/* PATHS                                                                    */
/*****************************************************************************/

/** A list of path elements points that gives a full path to an entity.
    Row and column names are paths.
*/

struct Path: protected ML::compact_vector<PathElement, 2, uint32_t, false> {
    typedef ML::compact_vector<PathElement, 2, uint32_t, false> Base;

    Path();
    Path(PathElement && path);
    Path(const PathElement & path);
    template<typename T>
    Path(const std::initializer_list<T> & val)
        : Path(val.begin(), val.end())
    {
    }

    template<typename It>
    Path(It first, It last)
        : Base(first, last)
    {
    }

    static Path parse(const Utf8String & str);
    static Path parse(const char * str, size_t len);

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

    Path operator + (const Path & other) const;
    Path operator + (Path && other) const;
    Path operator + (const PathElement & other) const;
    Path operator + (PathElement && other) const;

    operator RowHash() const;
    operator ColumnHash() const;

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

    using Base::size;
    using Base::empty;
    using Base::begin;
    using Base::end;
    using Base::at;
    using Base::front;
    using Base::back;
    using Base::operator [];

    PathElement head() const
    {
        return at(0);
    }

    Path tail() const
    {
        ExcAssert(!empty());
        Path result;
        result.insert(result.end(), begin() + 1, end());
        return result;
    }

    bool operator == (const Path & other) const
    {
        return static_cast<const Base &>(*this) == other;
    }

    bool operator != (const Path & other) const
    {
        return ! operator == (other);
    }

    bool operator < (const Path & other) const
    {
        return static_cast<const Base &>(*this) < other;
    }

    bool operator <= (const Path & other) const
    {
        return static_cast<const Base &>(*this) <= other;
    }

    bool operator > (const Path & other) const
    {
        return static_cast<const Base &>(*this) > other;
    }

    bool operator >= (const Path & other) const
    {
        return static_cast<const Base &>(*this) >= other;
    }

    size_t memusage() const;
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

struct PathNewHasher
    : public std::unary_function<Datacratic::MLDB::Path, size_t>
{
    size_t operator()(const Datacratic::MLDB::Path & path) const
    {
        return path.newHash();
    }
};

} // namespace MLDB
} // namespace Datacratic

namespace std {

template<typename T> struct hash;

template<>
struct hash<Datacratic::MLDB::PathElement> : public std::unary_function<Datacratic::MLDB::PathElement, size_t>
{
    size_t operator()(const Datacratic::MLDB::PathElement & path) const
    {
        return path.hash();
    }
};

template<>
struct hash<Datacratic::MLDB::Path> : public std::unary_function<Datacratic::MLDB::Path, size_t>
{
    size_t operator()(const Datacratic::MLDB::Path & paths) const
    {
        return paths.hash();
    }
};

} // namespace std
