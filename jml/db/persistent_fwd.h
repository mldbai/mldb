/* persistent_fwd.h                                                -*- C++ -*-
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   ---

   Forward declarations for persistence functions.
*/

#pragma once

namespace MLDB {
namespace DB {


class portable_bin_oarchive;
class portable_bin_iarchive;

typedef portable_bin_oarchive Store_Writer;
typedef portable_bin_iarchive Store_Reader;
typedef portable_bin_oarchive File_Writer;
typedef portable_bin_iarchive File_Reader;

/** Make an enumerated type serializable. */

#define PERSISTENT_ENUM_DECL(type) \
::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val); \
::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val)

#define PERSISTENT_ENUM_IMPL(type) \
::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val) \
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
::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val); \
::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val)

#define COMPACT_PERSISTENT_ENUM_IMPL(type) \
::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val) \
{ \
    store << DB::compact_size_t(val); \
    return store; \
} \
\
::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val) \
{ \
    DB::compact_size_t temp(store); \
    val = (type)temp.size_; \
    return store; \
} \

#define BYTE_PERSISTENT_ENUM_DECL(type) \
::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val); \
::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val)

#define BYTE_PERSISTENT_ENUM_IMPL(type) \
::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val) \
{ \
    unsigned char c = (unsigned char)val; \
    store << c; \
    return store; \
} \
\
::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val) \
{ \
    unsigned char c;  store >> c; \
    val = (type)c; \
    return store; \
} \

#define IMPL_SERIALIZE_RECONSTITUTE(type) \
inline ::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val) \
{ \
    val.serialize(store); \
    return store; \
} \
\
inline ::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val) \
{ \
    val.reconstitute(store); \
    return store; \
} \

#define MLDB_IMPL_SERIALIZE_RECONSTITUTE_TEMPLATE(template_params, type)       \
template<template_params> \
inline ::MLDB::DB::Store_Writer & \
operator << (::MLDB::DB::Store_Writer & store, const type & val) \
{ \
    val.serialize(store); \
    return store; \
} \
\
template<template_params> \
inline ::MLDB::DB::Store_Reader & \
operator >> (::MLDB::DB::Store_Reader & store, type & val) \
{ \
    val.reconstitute(store); \
    return store; \
}

void serialize_compact_size(Store_Writer & store, unsigned long long size);

unsigned long long
reconstitute_compact_size(Store_Reader & store);


} // namespace DB
} // namespace MLDB

