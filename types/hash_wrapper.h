/* hash_wrapper.h                                                  -*- C++ -*-
   Jeremy Barnes, 5 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include <iostream>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/arch/format.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/arch/exception.h"

namespace MLDB {

template<typename Int, int Domain>
struct IntWrapper {
    explicit IntWrapper(Int i = 0)
        : i(i)
    {
    }

    Int index() const { return i; }

    Int i;

    operator Int () const { return i; }
} MLDB_PACKED;

template<typename Int, int D>
std::ostream & operator << (std::ostream & stream, const IntWrapper<Int, D> & h)
{
    return stream << h.index();
}

template<int Domain>
struct HashWrapper : public IntWrapper<uint64_t, Domain> {
    explicit HashWrapper(uint64_t h = 0)
        : IntWrapper<uint64_t, Domain>(h)
    {
    }
    
#if 0
    HashWrapper(const Id & id)
        : IntWrapper<uint64_t, Domain>(id.hash())
    {
    }
#endif

    static HashWrapper max() { return HashWrapper(-1); }

    bool isMax() const { return hash() == -1; }

    uint64_t hash() const { return this->index(); }

    std::string toString() const
    {
        return MLDB::format("%016llx", (unsigned long long)this->index());
    }

    std::string toTaggedString() const
    {
        return MLDB::format("%016llx:%d", (unsigned long long)this->index(), Domain);
    }

    static HashWrapper fromString(const std::string & str)
    {
        unsigned long long val = 0;
        int res = sscanf(str.c_str(), "%llx", &val);
        if (res != 1)
            throw MLDB::Exception("didn't finish parsing hash '%s': returned %d",
                                str.c_str(), res);
        return HashWrapper(val);
    }

    static HashWrapper fromTaggedString(const std::string & str)
    {
        unsigned long long val = 0;
        int dom = 0;
        int res = sscanf(str.c_str(), "%llx:%d", &val, &dom);
        if (res != 2)
            throw MLDB::Exception("didn't finish parsing hash '%s': returned %d",
                                str.c_str(), res);
        if (dom != Domain)
            throw MLDB::Exception("wrong domain for HashWrapper");
        return HashWrapper(val);
    }

    static HashWrapper
    tryFromTaggedString(const std::string & str)
    {
        unsigned long long val = 0;
        int dom = 0;
        int res = sscanf(str.c_str(), "%llx:%d", &val, &dom);
        if (res != 2 || dom != Domain) return HashWrapper();
        return HashWrapper(val);
    }

} MLDB_PACKED;

template<int Domain>
inline
std::ostream & operator << (std::ostream & stream,
                            const HashWrapper<Domain> & h)
{
    return stream << h.toString();
}

} // namespace MLDB

namespace std {
template<int N>
struct hash<MLDB::HashWrapper<N> > {
    uint64_t operator () (const MLDB::HashWrapper<N> & h) const
    {
        return h.hash();
    }
};

} // namespace std

namespace MLDB {

typedef IntWrapper<uint32_t, 0> BI;
typedef HashWrapper<0> BH;
typedef IntWrapper<uint32_t, 1> SI;
typedef HashWrapper<1> SH;
typedef IntWrapper<uint32_t, 2> VI;   ///< Verb Index
typedef HashWrapper<2> VH;            ///< Verb Hash

// Hook them up to the reflection framework

template<int Domain> struct HashWrapperDescription;
template<typename Int, int Domain> struct IntWrapperDescription;

DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(HashWrapper, HashWrapperDescription, int, Domain);
DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(IntWrapper, IntWrapperDescription, typename, Int, int, Domain);


// Allow it to be used as part of a REST interface

template<typename Int, int Domain>
IntWrapper<Int, Domain> restDecode(const std::string & str,
                              IntWrapper<Int, Domain> * = 0)
{
    return IntWrapper<Int, Domain>::fromString(str);
}

template<typename Int, int Domain>
std::string restEncode(IntWrapper<Int, Domain> val)
{
    return val.toString();
}

template<typename Int, int Domain>
std::string keyToString(const IntWrapper<Int, Domain> & val)
{
    return val.toString();
}

template<typename Int, int Domain>
IntWrapper<Int, Domain> stringToKey(const std::string & str,
                                    IntWrapper<Int, Domain> *)
{
    return IntWrapper<Int, Domain>::fromString(str);
}


} // namespace MLDB
