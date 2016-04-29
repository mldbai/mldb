/** coord.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "dataset_fwd.h"
#include "mldb/types/string.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/base/exc_assert.h"

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

#pragma once

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* COORD                                                                     */
/*****************************************************************************/

/** This is a coordinate: a list of discrete (integer or string) values
    that are used to index rows, columns, etc within MLDB.

    It can deal with string representations (with dotted values) as well
    as their destructured versions.

    It takes up 32 bytes, and will do its best to inline whatever coordinates
    it is storing.
*/

struct Coord {
    Coord();
    Coord(Utf8String str);
    Coord(std::string str);
    Coord(const char * str, size_t len);

    Coord(const Coord & other)
        : words{other.words[0], other.words[1], other.words[2], other.words[3]}
    {
        if (other.complex_) {
            complexCopyConstruct(other);
        }
    }

    Coord(Coord && other) noexcept
        : words{other.words[0], other.words[1], other.words[2], other.words[3]}
    {
        if (other.complex_) {
            complexMoveConstruct(std::move(other));
        }
        else {
            other.words[0] = 0;
        }
    }

    template<size_t N>
    inline Coord(const char (&str)[N])
        : Coord(str, (N && str[N - 1])?N:N-1)  // remove null char from end
    {
    }

    // Create as an array index
    Coord(uint64_t i);

    ~Coord()
    {
        if (complex_)
            complexDestroy();
    }

    Coord & operator = (Coord && other) noexcept
    {
        Coord newMe(std::move(other));
        swap(newMe);
        return *this;
    }

    void swap(Coord & other) noexcept
    {
        // NOTE: this is only possible because there are no self-referential
        // pointers (ie, this can't point to itself in its contents).
        std::swap(words[0], other.words[0]);
        std::swap(words[1], other.words[1]);
        std::swap(words[2], other.words[2]);
        std::swap(words[3], other.words[3]);
    }

    Coord & operator = (const Coord & other) noexcept
    {
        Coord newMe(other);
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
    
    bool operator == (const Coord & other) const;
    bool operator != (const Coord & other) const;
    bool operator <  (const Coord & other) const;

    Utf8String toUtf8String() const;
    std::string toString() const;  // TODO: will disappear

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

    Coord operator + (const Coord & other) const;
    Coord operator + (Coord && other) const;

    operator RowHash() const;
    operator ColumnHash() const;

    size_t memusage() const;
    
    //private:
    void complexDestroy();
    void complexCopyConstruct(const Coord & other);
    void complexMoveConstruct(Coord && other);
    void initString(Utf8String str);
    void initStringUnchecked(Utf8String str);
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

    union {
        // The complex_ flag means we can't simply copy the words around;
        // we need to do some more work.
        struct { uint8_t complex_: 1; uint8_t simpleLen_:5; };
        uint8_t bytes[32];
        uint64_t words[4];
        Str str;
    };
};

std::ostream & operator << (std::ostream & stream, const Coord & id);

std::istream & operator >> (std::istream & stream, Coord & id);

inline bool operator == (const std::string & str1, const Coord & str2)
{
    return str2.stringEqual(str1);
}

inline bool operator != (const std::string & str1, const Coord & str2)
{
    return !str2.stringEqual(str1);
}

inline bool operator <  (const std::string & str1, const Coord & str2)
{
    return str2.stringGreaterEqual(str1);
}

inline bool operator == (const Utf8String & str1, const Coord & str2)
{
    return str2.stringEqual(str1.rawString());
}

inline bool operator != (const Utf8String & str1, const Coord & str2)
{
    return !str2.stringEqual(str1.rawString());
}

inline bool operator <  (const Utf8String & str1, const Coord & str2)
{
    return !str2.stringGreaterEqual(str1.rawString());
}

inline bool operator == (const Coord & str1, const std::string & str2)
{
    return str1.stringEqual(str2);
}

inline bool operator != (const Coord & str1, const std::string & str2)
{
    return !str1.stringEqual(str2);
}

inline bool operator <  (const Coord & str1, const std::string & str2)
{
    return str1.stringLess(str2);
}

inline bool operator == (const Coord & str1, const Utf8String & str2)
{
    return str1.stringEqual(str2.rawString());
}

inline bool operator != (const Coord & str1, const Utf8String & str2)
{
    return !str1.stringEqual(str2.rawString());
}

inline bool operator <  (const Coord & str1, const Utf8String & str2)
{
    return str1.stringLess(str2.rawString());
}

inline Utf8String keyToString(const Coord & key)
{
    return key.toUtf8String();
}

inline Coord stringToKey(const Utf8String & str, Coord *)
{
    return Coord(str);
}

inline Coord stringToKey(const std::string & str, Coord *)
{
    return Coord(str);
}

PREDECLARE_VALUE_DESCRIPTION(Coord);


struct CoordNewHasher
    : public std::unary_function<Datacratic::MLDB::Coord, size_t>
{
    size_t operator()(const Datacratic::MLDB::Coord & coord) const
    {
        return coord.newHash();
    }
};


} // namespace MLDB

} // namespace Datacratic

namespace std {

template<typename T> struct hash;

template<>
struct hash<Datacratic::MLDB::Coord> : public std::unary_function<Datacratic::MLDB::Coord, size_t>
{
    size_t operator()(const Datacratic::MLDB::Coord & coord) const
    {
        return coord.hash();
    }
};

} // namespace std
