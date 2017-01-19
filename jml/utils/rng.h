/* rng.h                                                           -*- C++ -*-
   Jeremy Barnes, 12 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   RNG wrapper.
*/

#pragma once

#include <memory>

namespace ML {

/*****************************************************************************/
/* RNG                                                                       */
/*****************************************************************************/

struct RNG {
    
    RNG();

    RNG(uint32_t seedValue);

    ~RNG();

    void seed(uint32_t value);

    /** Get a random number in a deterministic way */
    uint32_t random();

    /** Get a random number between 0 and max-1 */
    uint32_t random(uint32_t max);

    /** Get a uniform (0, 1) random number in a deterministic way */
    float random01();

    /** Used for eg passing to std::random_shuffle. */
    struct StandardRng {

        StandardRng(RNG & rng)
            : rng(rng)
        {
        }
        
        RNG & rng;
        
        template<class T>
        T operator () (T val) const
        {
            return rng.random(val);
        }
    };

    // Return one that can be used in standard algorithms like random_shuffle
    StandardRng standard() { return StandardRng(*this); }

    static RNG & defaultRNG();

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};



} // namespace ML
