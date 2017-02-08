/* distribution_simd.h                                             -*- C++ -*-
   Jeremy Barnes, 12 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   Vectorizes some distribution operations.
*/

#pragma once

#include "distribution.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/compiler/compiler.h"

namespace MLDB {

template<>
MLDB_ALWAYS_INLINE float
    distribution<float, std::vector<float> >::
total() const
{
    return SIMD::vec_sum_dp(&(*this)[0], this->size());
}

template<>
MLDB_ALWAYS_INLINE double
    distribution<double, std::vector<double> >::
total() const
{
    return SIMD::vec_sum(&(*this)[0], this->size());
}

template<>
MLDB_ALWAYS_INLINE double
distribution<float>::
dotprod(const distribution<float, std::vector<float> > & d2) const
{
    if (size() != d2.size())
        wrong_sizes_exception("dotprod", size(), d2.size());
    return SIMD::vec_dotprod_dp(&(*this)[0], &d2[0], size());
}

template<>
MLDB_ALWAYS_INLINE double
distribution<double>::
dotprod(const distribution<double, std::vector<double> > & d2) const
{
    if (size() != d2.size())
        wrong_sizes_exception("dotprod", size(), d2.size());
    return SIMD::vec_dotprod_dp(&(*this)[0], &d2[0], size());
}

template<>
template<>
MLDB_ALWAYS_INLINE double
distribution<double, std::vector<double> >::
dotprod(const distribution<float, std::vector<float> > & d2) const
{
    if (size() != d2.size())
        wrong_sizes_exception("dotprod", size(), d2.size());
    return SIMD::vec_dotprod_dp(&d2[0], &(*this)[0], size());
}

template<>
template<>
MLDB_ALWAYS_INLINE double
distribution<float, std::vector<float> >::
dotprod(const distribution<double, std::vector<double> > & d2) const
{
    if (size() != d2.size())
        wrong_sizes_exception("dotprod", size(), d2.size());
    return SIMD::vec_dotprod_dp(&(*this)[0], &d2[0], size());
}

inline distribution<double>
operator + (const distribution<double> & d1,
            const distribution<double> & d2)
{
    distribution<double> result(d1.size());
    if (d1.size() != d2.size())
        wrong_sizes_exception("+", d1.size(), d2.size());
    SIMD::vec_add(&d1[0], &d2[0], &result[0], d1.size());
    return result;
}

inline distribution<float>
operator + (const distribution<float> & d1,
            const distribution<float> & d2)
{
    distribution<float> result(d1.size());
    if (d1.size() != d2.size())
        wrong_sizes_exception("+", d1.size(), d2.size());
    SIMD::vec_add(&d1[0], &d2[0], &result[0], d1.size());
    return result;
}

inline distribution<double>
operator - (const distribution<double> & d1,
            const distribution<double> & d2)
{
    distribution<double> result(d1.size());
    if (d1.size() != d2.size())
        wrong_sizes_exception("-", d1.size(), d2.size());
    SIMD::vec_minus(&d1[0], &d2[0], &result[0], d1.size());
    return result;
}

inline distribution<float>
operator - (const distribution<float> & d1,
            const distribution<float> & d2)
{
    distribution<float> result(d1.size());
    if (d1.size() != d2.size())
        wrong_sizes_exception("-", d1.size(), d2.size());
    SIMD::vec_minus(&d1[0], &d2[0], &result[0], d1.size());
    return result;
}

inline distribution<double>
operator * (const distribution<double> & d1,
            const distribution<double> & d2)
{
    distribution<double> result(d1.size());
    if (d1.size() != d2.size())
        wrong_sizes_exception("*", d1.size(), d2.size());
    SIMD::vec_prod(&d1[0], &d2[0], &result[0], d1.size());
    return result;
}

inline distribution<float>
operator * (const distribution<float> & d1,
            const distribution<float> & d2)
{
    distribution<float> result(d1.size());
    if (d1.size() != d2.size())
        wrong_sizes_exception("*", d1.size(), d2.size());
    SIMD::vec_prod(&d1[0], &d2[0], &result[0], d1.size());
    return result;
}

inline distribution<float> &
operator *= (distribution<float> & d,
             float factor)
{
    SIMD::vec_scale(&d[0], factor, &d[0], d.size());
    return d;
}

inline distribution<double> &
operator *= (distribution<double> & d,
             double factor)
{
    SIMD::vec_scale(&d[0], factor, &d[0], d.size());
    return d;
}

template<>
template<>
inline distribution<double> &
distribution<double, std::vector<double> >::
operator += (const distribution<float, std::vector<float> > & d)
{
    if (this->size() != d.size())
        wrong_sizes_exception("+= simd", this->size(), size());
    SIMD::vec_add(&(*this)[0], 1.0, &d[0], &(*this)[0], d.size());
    return *this;
}

template<>
template<>
inline void
distribution<float, std::vector<float> >::
min_max(distribution<float, std::vector<float> > & minValues,
        distribution<float, std::vector<float> > & maxValues) const
{
    if (this->size() != minValues.size())
        wrong_sizes_exception("min_max", this->size(), minValues.size());
    if (this->size() != maxValues.size())
        wrong_sizes_exception("max_max", this->size(), maxValues.size());
    SIMD::vec_min_max_el(&(*this)[0], &minValues[0], &maxValues[0],
                         this->size());
}

template<class Underlying>
distribution<float, Underlying>
exp(const distribution<float, Underlying> & dist)
{
    distribution<float, Underlying> result(dist.size());
    SIMD::vec_exp(&dist[0], &result[0], dist.size());
    return result;
}

template<class Underlying>
distribution<double, Underlying>
exp(const distribution<double, Underlying> & dist)
{
    distribution<double, Underlying> result(dist.size());
    SIMD::vec_exp(&dist[0], &result[0], dist.size());
    return result;
}

} // namespace MLDB
