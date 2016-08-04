/* simd.h                                                          -*- C++ -*-
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Detection of SIMD (vector) unit and associated functions.
*/

#pragma once

#include <string>
#include "cpuid.h"
#include "mldb/arch/arch.h"

namespace ML {

#ifdef JML_INTEL_ISA
# ifndef JML_USE_SSE1
#   define JML_USE_SSE1 1
# endif
# ifndef JML_USE_SSE2
#   define JML_USE_SSE2 1
# endif
# ifndef JML_USE_SSE3
#   define JML_USE_SSE3 1
# endif
# ifndef JML_USE_SSE3
#   define JML_USE_SSE3 1
# endif

JML_ALWAYS_INLINE bool has_mmx() { return cpu_info().mmx; }

JML_ALWAYS_INLINE bool has_sse1() { return cpu_info().sse; }

JML_ALWAYS_INLINE bool has_sse2() { return cpu_info().sse2; }

JML_ALWAYS_INLINE bool has_sse3() { return cpu_info().sse3; }

JML_ALWAYS_INLINE bool has_sse41() { return cpu_info().sse41; }

JML_ALWAYS_INLINE bool has_sse42() { return cpu_info().sse42; }

JML_ALWAYS_INLINE bool has_pni() { return cpu_info().pni; }

JML_ALWAYS_INLINE bool has_avx()
{
    const CPU_Info & info = cpu_info();
    return info.avx && info.xsave && info.osxsave;
}

JML_ALWAYS_INLINE bool has_avx2()
{
    return cpuid(7, 0).ebx & (1 << 5);
}

#endif // __i686__

} // namespace ML
