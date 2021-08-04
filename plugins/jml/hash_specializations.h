/* hash_specializations.h                                          -*- C++ -*-
   Jeremy Barnes, 5 February 2005
   Copyright (c) Jeremy Barnes 2005.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   Specializations of standard hash functions.
*/

#pragma once

#include <string>
#include "mldb/utils/floating_point.h"
#include "mldb/types/string.h"

namespace ML {

inline size_t chain_hash(size_t h1, size_t h2 = 0)
{
    return 18446744073709551557ULL * h1 + h2;
}

} // namespace ML


namespace std {

#if 0
template<>
struct hash<std::string> {
    size_t operator () (const std::string & str) const
    {
        return std::hash<const char *>()(str.c_str());
    }

};
#endif

#if 0
template<>
struct hash<float> : public MLDB::float_hasher {
};
#endif

#if 0
template<typename T>
struct hash<T *> {
    size_t operator () (const T * ptr) const
    {
        return ML::chain_hash(reinterpret_cast<size_t>(ptr));
    }
};
#endif

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

} // namespace std
