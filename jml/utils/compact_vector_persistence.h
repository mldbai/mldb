// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* compact_vector_persistence.h                                    -*- C++ -*-
   Jeremy Barnes, 3 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   Persistence of compact vector.
*/

#pragma once

#include "mldb/utils/compact_vector.h"
#include "mldb/jml/db/persistent_fwd.h"

namespace MLDB {

template<typename D, size_t I, typename Sz, bool Sf, typename P, class A>
inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store,
             const compact_vector<D, I, Sz, Sf, P, A> & v)
{
    ML::DB::serialize_compact_size(store, v.size());
    for (unsigned i = 0;  i < v.size();  ++i)
        store << v[i];
    return store;
}

template<typename D, size_t I, typename Sz, bool Sf, typename P, class A>
inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store,
             compact_vector<D, I, Sz, Sf, P, A> & v)
{
    unsigned long long sz = reconstitute_compact_size(store);
    v.resize(sz);
    for (unsigned i = 0;  i < sz;  ++i)
        store >> v[i];
    return store;
}

} // namespace MLDB

