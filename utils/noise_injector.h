/* noise_injector.h                                                           -*- C++ -*-
   Francois Maillet, Guy Dumais, 3 May 2017

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Class to generate random noise.
*/

#pragma once
#include <cstdint>
#include <limits>

namespace MLDB {
/*****************************************************************************/
/* NOISE INJECTOR                                                            */
/*****************************************************************************/

struct NoiseInjector {

    NoiseInjector() : mu(0), b(3)
    {
    }

    /** Generate uniform random numbers between [-0.5, 0.5) */
    double rand_uniform() const;

    /** Generate a random number following the Laplace distribution
        with mu and b. By default mu = 0 (centered at the origin) and
        b = 3 (with maximal probability of 1/6).
    */
    double sample_laplace() const;

    /** Add noise to an integer following the Laplace distribution. With the
     default configuration the noise will be centered at 0 and most of the values
     will be in [-6, 6]. */
    std::int64_t add_noise(std::int64_t count,
                      std::int64_t max=std::numeric_limits<std::int64_t>::max()) const;

    // http://stackoverflow.com/a/4609795
    template <typename T> int sgn(T val) const {
        return (T(0) < val) - (val < T(0));
    }

    const double mu;
    const double b;
};

} // namespace MLDB
