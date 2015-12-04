// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* profile.h                                                       -*- C++ -*-
   Jeremy Barnes, 15 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Profiling code.
*/

#ifndef __utils__profile_h__
#define __utils__profile_h__

#include <boost/timer.hpp>
#include "mldb/arch/timers.h"

namespace ML {

class Function_Profiler {
public:
    boost::timer * t;   
    double & var;
    Function_Profiler(double & var, bool profile)
        : t(0), var(var)
    {
        if (profile) t = new boost::timer();
    }
    ~Function_Profiler()
    {
        if (t) var += t->elapsed();
        delete t;
    }
};

struct StackProfilerSeed {
    std::atomic<double> accum;
    std::atomic<double> wallaccum;
    const char* label;
    StackProfilerSeed(const char * label) : accum(0), wallaccum(0), label(label)
    {
        std::cerr << label << std::endl;
    }
    ~StackProfilerSeed()
    {
        std::cerr << label << " (" << accum << " sec cpu elapsed " << wallaccum << " sec Wall)" << std::endl;
    }
};

struct StackProfiler {
    boost::timer t;
    ML::Timer wall;
    StackProfilerSeed& seed;
    StackProfiler(StackProfilerSeed& seed) : seed(seed)   
    {
        
    }
    ~StackProfiler()
    {
       auto current = seed.accum.load();
       while (!seed.accum.compare_exchange_weak(current, current + t.elapsed()));
       current = seed.wallaccum.load();
       while (!seed.wallaccum.compare_exchange_weak(current, current + wall.elapsed_wall()));
    }
};

#define PROFILE_FUNCTION(var) \
Function_Profiler __profiler(var, profile);

#define STACK_PROFILE(label) \
const char* label##__stack_profile_string = #label; \
ML::StackProfilerSeed label##__stack_profile_seed(label##__stack_profile_string); \
ML::StackProfiler label##__stack_profile(label##__stack_profile_seed);

#define STACK_PROFILE_SEED(label) \
const char* label##__stack_profile_string = #label; \
ML::StackProfilerSeed label##__stack_profile_seed(label##__stack_profile_string); \

#define STACK_PROFILE_ADD(label) \
ML::StackProfiler label##__stack_profile(label##__stack_profile_seed);

} // namespace ML

#endif /* __utils__profile_h__ */
