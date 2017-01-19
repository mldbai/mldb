// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rng.cc
   Jeremy Barnes, 12 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#include "rng.h"

#include <random>

namespace ML {

/*****************************************************************************/
/* RNG                                                                       */
/*****************************************************************************/

struct RNG::Itl {
    Itl()
    : uniform01_(0, 1)
    {
    }
        
    std::mt19937 rng_;
    std::uniform_real_distribution<> uniform01_;
};

RNG::
RNG()
    : itl(new Itl())
{
    seed(random());
}

RNG::
RNG(uint32_t seedValue)
    : itl(new Itl())
{
    seed(seedValue);
}

RNG::~RNG()
{
}

void
RNG::
seed(uint32_t value)
{
    if (value == 0) value = 1;
    itl->rng_.seed(value);
}

uint32_t
RNG::
random()
{
    return itl->rng_();
}

uint32_t
RNG::
random(uint32_t max)
{
    return itl->rng_() % max;
}
    

float
RNG::
random01()
{
    return itl->uniform01_(itl->rng_);
}

RNG & RNG::defaultRNG()
{
    static thread_local RNG defaultRNGs;
    return defaultRNGs;
}

} // namespace ML
