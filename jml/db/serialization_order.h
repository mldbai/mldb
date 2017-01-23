// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* serialization_order.h                                           -*- C++ -*-
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
     


   ---

   Functions to convert between host and serialization order.  We define
   serialization order as the native byte order on x86 machines; this is
   the opposite to the hton and ntoh macros.
*/

#ifndef __db__serialization_order_h__
#define __db__serialization_order_h__

#include <stdint.h>
#include "mldb/compiler/compiler.h"

namespace ML {
namespace DB {

// TODO: on ppc, these need to be byteswapped...

MLDB_ALWAYS_INLINE uint8_t native_order(uint8_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint16_t native_order(uint16_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint32_t native_order(uint32_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint64_t native_order(uint64_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int8_t native_order(int8_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int16_t native_order(int16_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int32_t native_order(int32_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int64_t native_order(int64_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint8_t serialization_order(uint8_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint16_t serialization_order(uint16_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint32_t serialization_order(uint32_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE uint64_t serialization_order(uint64_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int8_t serialization_order(int8_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int16_t serialization_order(int16_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int32_t serialization_order(int32_t val)
{
    return val;
}

MLDB_ALWAYS_INLINE int64_t serialization_order(int64_t val)
{
    return val;
}

} // namespace DB
} // namespace ML

#endif /* __db__serialization_order_h__ */
