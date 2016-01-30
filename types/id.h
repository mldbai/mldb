/* id.h                                                            -*- C++ -*-
   Jeremy Barnes, 17 February 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Basic ID type for binary IDs of all types.
*/

#pragma once

#include <string>
#include <cstring>
#include "string.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/exception.h"
#include "mldb/types/value_description_fwd.h"
#include <atomic>

namespace Json {
class Value;
} // namespace Json

namespace Datacratic {

/* 
   JTzfCLBhlbWSsdZjcJ4wO4
   
   Google ID: --> CAESEAYra3NIxLT9C8twKrzqaA
   AGID: --> 0828398c-5965-11e0-84c8-0026b937c8e1
   ANID: --> 7394206091425759590

   
   0828398c-5965-11e0-84c8-0026b937c8e1
   32       16   16   16   48
   
   AYra3NIxLT9C8twKrzqaA
   
   21 * 6 = 126 bits so 128 bits
*/

/*****************************************************************************/
/* ID                                                                        */
/*****************************************************************************/

/** Generic class to rapidly deal with IDs such as UUIDs, etc.
 */

struct Id {

    enum Type {
        NONE = 0,
        NULLID = 1,
        UUID = 2,        /// uuid string eg 0828398c-5965-11e0-84c8-0026b937c8e1
        GOOG128 = 3,     /// google CAESEAYra3NIxLT9C8twKrzqaA
        BIGDEC = 4,      /// 7394206091425759590
        BASE64_96 = 5,   /// 16 character base64 string
        HEX128LC = 6,    /// 32 character lowercase hex string
        INT64DEC = 7,    /// obsolete type, do not use
        UUID_CAPS = 8,   /// uuid-ish string eg 0828398C-5965-11E0-84C8-0026B937C8E1

        // other integer-encoded values go here

        SHORTSTR = 191,  /// string with length <= 16
        STR = 192,       /// any string
        //CUSTOM = 6       /// custom type with custom handlers

        COMPOUND2 = 193,  ///< compound of two underlying IDs
        
        // other string-encoded values go here

        UNKNOWN = 255
    };

    Id()
        : type(NONE), val1(0), val2(0)
    {
    }

    ~Id()
    {
        if (type >= STR)
            complexDestroy();
    }

    explicit Id(const std::string & value,
                Type type = UNKNOWN)
        : type(NONE), val1(0), val2(0)
    {
        parse(value, type);
    }
    
    explicit Id(const Utf8String & value,
                Type type = UNKNOWN)
        : type(NONE), val1(0), val2(0)
    {
        parse(value, type);
    }

    explicit Id(const char * value, Type type = UNKNOWN);
    
    explicit Id(const wchar_t * value, Type type = UNKNOWN);

    explicit Id(const char * value, size_t len,
                Type type = UNKNOWN)
        : type(NONE), val1(0), val2(0)
    {
        parse(value, len, type);
    }
    
    explicit Id(uint64_t value):
    		type(BIGDEC),
    		val1(value),val2(0)
    {
    }


    // Construct a compound ID from two others
    Id(const Id & underlying1, const Id & underlying2)
        : type(COMPOUND2),
          cmp1(new Id(underlying1)),
          cmp2(new Id(underlying2))
    {
    }

    Id(Id && other) noexcept
        : type(other.type),
          val1(other.val1), val2(other.val2)
    {
        other.type = NONE;
    }

    Id(const Id & other)
        : type(other.type),
          val1(other.val1), val2(other.val2)
    {
        if (other.type >= STR)
            complexFinishCopy();
    }

    Id & operator = (Id && other) noexcept
    {
        if (type >= STR)
            complexDestroy();
        type = other.type;
        val1 = other.val1;
        val2 = other.val2;
        other.type = NONE;
        return *this;
    }

    Id & operator = (const Id & other)
    {
        if (type >= STR)
            complexDestroy();
        type = other.type;
        val1 = other.val1;
        val2 = other.val2;
        if (other.type >= STR)
            complexFinishCopy();
        return *this;
    }

    void parse(const std::string & value, Type type = UNKNOWN)
    {
        parse(value.c_str(), value.size(), type);
    }

    void parse(const Utf8String & value, Type type = UNKNOWN)
    {
        parse(value.rawData(), value.rawLength(), type);
    }

    void parse(const char * value, size_t len, Type type = UNKNOWN);

    void parse(const char * value, Type type = UNKNOWN)
    {
        parse(value, std::strlen(value), type);
    }
    
    std::string toString() const;

    Utf8String toUtf8String() const;

    /// Optimized comparison of result of toString() with other string
    /// Equivalent to toString() == other
    bool stringEqual(const std::string & other) const;

    /// Optimized comparison of result of toString() with other string
    /// Equivalent to toString() == other
    bool stringEqual(const char * other) const;

    /// Optimized comparison of result of toString() with other string
    /// Equivalent to toString() < other
    bool stringLess(const std::string & other) const;

    /// Optimized comparison of result of toString() with other string
    /// Equivalent to toString() >= other
    bool stringGreaterEqual(const std::string & other) const;

    /** Return the length of the string returned by toString() */
    size_t toStringLength() const;

    /** Return the data for a toString().  Only works for STR and SHORTSTR. */
    const char * stringData() const;

    uint64_t toInt() const
    {
        if (type != BIGDEC)
            throw ML::Exception("can't convert non-BIGDEC to int");
        if (val2) {
            throw ML::Exception("cannot convert 128-bit value to uint64_t");
        }
        return val1;
    }

    //operator std::string () const
    //{
    //    return toString();
    //}

    bool notNull() const
    {
        return type >= NULLID;
    }

    operator bool () const { return notNull(); }

    bool operator == (const Id & other) const
    {
        if (type != other.type) return false;
        if (type == NONE || type == NULLID) return true;
        if (JML_UNLIKELY(type >= STR)) return complexEqual(other);
        return val1 == other.val1 && val2 == other.val2;  // works for SHORTSTR too
    }

    bool operator != (const Id & other) const
    {
        return ! operator == (other);
    }
    
    bool operator < (const Id & other) const
    {
        if (type == SHORTSTR || type == STR)
            return complexLess(other);
        if (type < other.type) return true;
        if (other.type < type) return false;
        if (JML_UNLIKELY(type > STR)) return complexLess(other);
        return (valHigh < other.valHigh
                || (valHigh == other.valHigh && valLow < other.valLow));
    }

    bool operator > (const Id & other) const
    {
        return (*this != other && !(*this < other));
    }

    uint64_t hash() const;

    bool complexEqual(const Id & other) const;
    bool complexLess(const Id & other) const;
    uint64_t complexHash() const;
    void complexDestroy();
    void complexFinishCopy();

    uint8_t type;
    uint8_t unused[3];

    struct StringRep {
        StringRep(int n)
            : ref(n)
        {
        }

        std::atomic<int> ref;
        char data[0];
    };

    union {
        // 128 byte integer
        struct {
            uint64_t val1;  // low order bits
            uint64_t val2;  // high order bits
        };

        struct {
            uint64_t valLow;
            uint64_t valHigh;
        };

        struct {
            uint32_t v1h, v1l;
            uint32_t v2h, v2l;
        };

        // uuid
        struct {
            uint64_t f1:32;
            uint64_t f2:16;
            uint64_t f3:16;
            uint64_t f4:16;
            uint64_t f5:48;
        };

        // string
        struct {
            uint64_t len:56;
            uint64_t ownstr:8;
            //const char * str;
            StringRep * str;
        };

        // short, interned string.  Null padded.  UTF-8 is OK.
        struct {
            char shortStr[16];
        };

        // compound2
        struct {
            Id * cmp1;
            Id * cmp2;
        };
#if 0
        // custom
        struct {
            void * data;
            uint64_t (*controlFn) (int, Id *, Id *);
        };
#endif
    };

    /// Return the first half of a COMPOUND2 Id
    const Id & compoundId1() const;

    /// Return the second half of a COMPUOND2 Id
    const Id & compoundId2() const;
    
    Json::Value toJson() const;
    static Id fromJson(const Json::Value & val);
} JML_PACKED;

std::ostream & operator << (std::ostream & stream, const Id & id);

std::istream & operator >> (std::istream & stream, Id & id);

using std::to_string;

inline std::string to_string(const Id & id)
{
    return id.toString();
}

inline Id stringToKey(const Utf8String & str, Id *)
{
    return Id(str);
}

inline Id stringToKey(const std::string & str, Id *)
{
    return Id(str);
}

PREDECLARE_VALUE_DESCRIPTION(Id);

} // namespace Datacratic

namespace std {

template<typename T> struct hash;

template<>
struct hash<Datacratic::Id> : public std::unary_function<Datacratic::Id, size_t>
{
    size_t operator()(const Datacratic::Id & id) const { return id.hash(); }
};

} // namespace std
