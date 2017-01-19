// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* hash_specializations.h                                          -*- C++ -*-
   Jeremy Barnes, 5 February 2005
   Copyright (c) Jeremy Barnes 2005.  All rights reserved.
   


   ---

   Specializations of standard hash functions.
*/

#ifndef __utils__hash_specializations_h__
#define __utils__hash_specializations_h__


#define _BACKWARD_BACKWARD_WARNING_H 1
#include <ext/hash_map>
#include <string>
#include "mldb/jml/utils/floating_point.h"

#define MLDB_HASH_NS __gnu_cxx

namespace ML {

inline size_t chain_hash(size_t h1, size_t h2 = 0)
{
    return 18446744073709551557ULL * h1 + h2;
}

} // namespace ML


#ifndef __GXX_EXPERIMENTAL_CXX0X__

namespace std {

using MLDB_HASH_NS::hash;

} // namespace std

#endif

namespace __gnu_cxx {

template<>
struct hash<std::string> {
    size_t operator () (const std::string & str) const
    {
        return MLDB_HASH_NS::hash<const char *>()(str.c_str());
    }

};

template<>
struct hash<float> : public ML::float_hasher {
};

template<typename T>
struct hash<T *> {
    size_t operator () (const T * ptr) const
    {
        return ML::chain_hash(reinterpret_cast<size_t>(ptr));
    }
};

template<typename X, typename Y>
struct hash<std::pair<X, Y> > {

    hash<X> hash1;
    hash<Y> hash2;

    size_t operator () (const std::pair<X, Y> & p)
    {
        return ML::chain_hash(hash1(p.first),
                              ML::chain_hash(hash2(p.second)));
    }
};

} // namespace MLDB_HASH_NS


#endif /* __utils__hash_specializations_h__ */
