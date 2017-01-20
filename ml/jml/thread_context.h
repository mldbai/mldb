/* thread_context.h                                                -*- C++ -*-
   Jeremy Barnes, 26 February 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Context for a thread.  Allows us to keep track of execution resources, etc
   as we go.
*/

#pragma once

//#include "mldb/jml/utils/worker_task.h"
#include <random>
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/arch/exception.h"

namespace ML {

template<class RNG>
struct RNG_Adaptor {

    RNG_Adaptor(RNG & rng)
        : rng(rng)
    {
    }

    RNG & rng;
    
    template<class T>
    T operator () (T val) const
    {
        return rng() % val;
    }
};

class Thread_Context {
public:
    Thread_Context(uint32_t rand_seed = 0,
                   int recursion = 0)
        : recursion_(recursion)
    {
        if (rand_seed != 0)
            rng_.seed(rand_seed);
    }

    void seed(uint32_t value)
    {
        if (value == 0) value = 1;
        rng_.seed(value);
    }

    /** Get a random number in a deterministic way */
    uint32_t random()
    {
        return rng_();
    }

    /** Get a uniform (0, 1) random number in a deterministic way */
    float random01()
    {
        return uniform01_(rng_);
    }

    typedef RNG_Adaptor<std::mt19937> RNG_Type;
    RNG_Type rng() { return RNG_Type(rng_); }

    /** What level are we recursed to? */
    int recursion() const { return recursion_; }

    /** Create a new thread context for another thread, optionally with a
        child identifier. */
    Thread_Context child(bool local_thread_only = false)
    {
        return Thread_Context(random(), recursion_ + 1);
    }

private:
    std::mt19937 rng_;
    std::uniform_real_distribution<> uniform01_;
    int recursion_;
};

} // namespace ML
