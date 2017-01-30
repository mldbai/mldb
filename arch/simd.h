/* simd.h                                                          -*- C++ -*-
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Detection of SIMD (vector) unit and associated functions.
*/

#pragma once

#include <string>
#include "cpuid.h"
#include "mldb/arch/arch.h"

namespace MLDB {

#ifdef MLDB_INTEL_ISA
# ifndef MLDB_USE_SSE1
#   define MLDB_USE_SSE1 1
# endif
# ifndef MLDB_USE_SSE2
#   define MLDB_USE_SSE2 1
# endif
# ifndef MLDB_USE_SSE3
#   define MLDB_USE_SSE3 1
# endif
# ifndef MLDB_USE_SSE3
#   define MLDB_USE_SSE3 1
# endif

MLDB_ALWAYS_INLINE bool has_mmx() { return cpu_info().mmx; }

MLDB_ALWAYS_INLINE bool has_sse1() { return cpu_info().sse; }

MLDB_ALWAYS_INLINE bool has_sse2() { return cpu_info().sse2; }

MLDB_ALWAYS_INLINE bool has_sse3() { return cpu_info().sse3; }

MLDB_ALWAYS_INLINE bool has_sse41() { return cpu_info().sse41; }

MLDB_ALWAYS_INLINE bool has_sse42() { return cpu_info().sse42; }

MLDB_ALWAYS_INLINE bool has_pni() { return cpu_info().pni; }

MLDB_ALWAYS_INLINE bool has_avx()
{
    const CPU_Info & info = cpu_info();
    return info.avx && info.xsave && info.osxsave;
}

MLDB_ALWAYS_INLINE bool has_avx2()
{
    return cpuid(7, 0).ebx & (1 << 5);
}

#endif // __i686__

} // namespace MLDB
