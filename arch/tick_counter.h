/* tick_counter.h                                                  -*- C++ -*-
   Jeremy Barnes, 15 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Code to access the hardware tick counter.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include <stdint.h>
#include "mldb/arch/arch.h"

namespace MLDB {

/** Return the number of CPU clock ticks since some epoch. */
MLDB_ALWAYS_INLINE uint64_t ticks()
{
#if defined(MLDB_INTEL_ISA)
# if (MLDB_BITS == 32)
    uint64_t result;
    asm volatile ("rdtsc\n\t" : "=A" (result));
    return result;
# else
    uint64_t result, procid;
    asm volatile ("rdtsc                  \n\t"
                  "shl     $32, %%rdx     \n\t"
                  "or      %%rdx, %%rax   \n\t"
                  : "=a" (result), "=c" (procid) : : "%rdx" );

    //std::cerr << "procid = " << procid << std::endl;

    return result;
# endif // 32/64 bits
#else // non-intel
    return 0;  
#endif
}

/** The average number of ticks of overhead for the tick counter. */
extern double ticks_overhead;

/** The average number of ticks per second. */
extern double ticks_per_second;

/** Number of seconds per tick */
extern double seconds_per_tick;

double calc_ticks_overhead();
double calc_ticks_per_second(double seconds_to_measure = 0.01);

} // namespace MLDB
