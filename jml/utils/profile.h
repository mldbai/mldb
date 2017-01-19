/* profile.h                                                       -*- C++ -*-
   Jeremy Barnes, 15 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Profiling code.
*/

#pragma once

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
    MLDB::Timer wall;
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

struct StackProfilerAverage {

    MLDB::Timer wall;
    std::atomic<int>& count;
    std::atomic<double>& sum;
    const char* label;
    StackProfilerAverage(const char * label, std::atomic<int>& count, std::atomic<double>& sum) : count(count), sum(sum), label(label)
    {

    }

    ~StackProfilerAverage()
    {
        auto currentCount = count.load();
        while (!count.compare_exchange_weak(currentCount, currentCount + 1));

        auto current = sum.load();
        while (!sum.compare_exchange_weak(current, current + wall.elapsed_wall()));

        if (currentCount % 1000 == 0)
        {
            std::cerr << label << " (" << (current / currentCount) << " average sec cpu walll " << std::endl;
        }
    }
};


struct ConcurrencyCounter {
    ConcurrencyCounter(const char * label, std::atomic<int>& count, std::atomic<int>& maxcount) : count(count)
    {
        count++;
        int currentMaxCount = maxcount;
        if (count > currentMaxCount)
        {
            while (true)
            {
               if (!std::atomic_compare_exchange_strong(&maxcount, &currentMaxCount, currentMaxCount+1))
               {
                    currentMaxCount = maxcount.load();
                    if (count <= currentMaxCount)
                    {
                        break;
                    }
               }
               else
               {
                  std::cerr << "MAX CONCURRENCY CONNECTION COUNT FOR " << label << ": " << currentMaxCount+1 << std::endl;
               } 
            };
           
        }
    }

    ~ConcurrencyCounter()
    {
        count--;
    }

    std::atomic<int>& count;
};

#define STACK_PROFILE_AVERAGE(label) \
const char* label##__stack_profile_average_string = #label; \
static std::atomic<int> label##__stack_average_count(0); \
static std::atomic<double> label##__stack_average_sum(0); \
ML::StackProfilerAverage label##__average_profile(label##__stack_profile_average_string, label##__stack_average_count, label##__stack_average_sum);

#define STACK_CONCURRENCY_COUNTER(label) \
const char* label##__stack_profile_string = #label; \
static std::atomic<int> label##__stack_concurrency_count(0); \
static std::atomic<int> label##__stack_concurrency_maxcount(0); \
ML::ConcurrencyCounter label##__concurrency_profile(label##__stack_profile_string, label##__stack_concurrency_count, label##__stack_concurrency_maxcount);

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
