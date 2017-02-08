// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* persistent_fwd.h                                                -*- C++ -*-
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.



   ---

   Forward declarations for persistence functions.
*/

#ifndef __db__persistent_fwd_h__
#define __db__persistent_fwd_h__

namespace ML {
namespace DB {


class portable_bin_oarchive;
class portable_bin_iarchive;

typedef portable_bin_oarchive Store_Writer;
typedef portable_bin_iarchive Store_Reader;
typedef portable_bin_oarchive File_Writer;
typedef portable_bin_iarchive File_Reader;

/** Make an enumerated type serializable. */

#define PERSISTENT_ENUM_DECL(type) \
::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val); \
::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val)

#define PERSISTENT_ENUM_IMPL(type) \
::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val) \
{ \
    store << (int)val; \
    return store; \
} \
\
{ \
    int temp; \
    store >> temp; \
    val = (type)temp; \
    return store; \
} \


#define COMPACT_PERSISTENT_ENUM_DECL(type) \
::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val); \
::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val)

#define COMPACT_PERSISTENT_ENUM_IMPL(type) \
::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val) \
{ \
    store << DB::compact_size_t(val); \
    return store; \
} \
\
::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val) \
{ \
    DB::compact_size_t temp(store); \
    val = (type)temp.size_; \
    return store; \
} \

#define BYTE_PERSISTENT_ENUM_DECL(type) \
::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val); \
::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val)

#define BYTE_PERSISTENT_ENUM_IMPL(type) \
::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val) \
{ \
    unsigned char c = (unsigned char)val; \
    store << c; \
    return store; \
} \
\
::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val) \
{ \
    unsigned char c;  store >> c; \
    val = (type)c; \
    return store; \
} \

#define IMPL_SERIALIZE_RECONSTITUTE(type) \
inline ::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val) \
{ \
    val.serialize(store); \
    return store; \
} \
\
inline ::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val) \
{ \
    val.reconstitute(store); \
    return store; \
} \

#define MLDB_IMPL_SERIALIZE_RECONSTITUTE_TEMPLATE(template_params, type)       \
template<template_params> \
inline ::ML::DB::Store_Writer & \
operator << (::ML::DB::Store_Writer & store, const type & val) \
{ \
    val.serialize(store); \
    return store; \
} \
\
template<template_params> \
inline ::ML::DB::Store_Reader & \
operator >> (::ML::DB::Store_Reader & store, type & val) \
{ \
    val.reconstitute(store); \
    return store; \
}

void serialize_compact_size(Store_Writer & store, unsigned long long size);

unsigned long long
reconstitute_compact_size(Store_Reader & store);


} // namespace DB
} // namespace ML


#endif /* __db__persistent_fwd_h__ */


