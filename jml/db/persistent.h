// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* persistent.h                                                    -*- C++ -*-
   Jeremy Barnes, 27 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.



   ---

   Persistence functions.
*/

#ifndef __db__persistent_h__
#define __db__persistent_h__

#include "persistent_fwd.h"
#include "portable_iarchive.h"
#include "portable_oarchive.h"
#include "compact_size_types.h"
#include <fstream>

namespace ML {
namespace DB {

template<class X>
Store_Writer &
operator << (Store_Writer & store, const X & x)
{
    store.save(x);
    return store;
}

template<class X>
Store_Reader &
operator >> (Store_Reader & store, X & x)
{
    store.load(x);
    return store;
}

template<typename T,
         typename X = decltype(((T *)0)->serialize(*(ML::DB::Store_Writer *)0))>
std::string
serializeToString(const T & t, X * = 0)
{
    std::ostringstream stream;
    ML::DB::Store_Writer writer(stream);
    t.serialize(writer);
    return stream.str();
}


template<typename T>
T reconstituteFromString(const std::string & str)
{
    std::istringstream stream(str);
    ML::DB::Store_Reader store(stream);
    T result;
    result.reconstitute(store);
    return result;
}


} // namespace DB
} // namespace ML

#endif /* __db__persistent_h__ */

