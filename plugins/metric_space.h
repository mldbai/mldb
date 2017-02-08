// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** metric_space.h                                                 -*- C++ -*-
    Jeremy Barnes, 25 April 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Metric space for MLDB.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/jml/stats/distribution.h"


namespace MLDB {

enum MetricSpace {
    METRIC_NONE,          ///< No metric chosen; need to set one
    METRIC_EUCLIDEAN,
    METRIC_COSINE
};

DECLARE_ENUM_DESCRIPTION(MetricSpace);


/*****************************************************************************/
/* DISTANCE METRIC                                                           */
/*****************************************************************************/

struct DistanceMetric {
    virtual ~DistanceMetric()
    {
    }

    /** Add a row, caching information about it. */
    virtual void addRow(int rowNum, const distribution<float> & coords) = 0;

    /** Calculate the distance between two rows.  If either of them have
        a known number, it is passed in rowNum, otherwise that rowNum
        will be -1.
    */
    virtual float dist(int rowNum1, int rowNum2,
                       const distribution<float> & coords1,
                       const distribution<float> & coords2) const = 0;

    /** Factor for distance metric objects. */
    static DistanceMetric * create(MetricSpace space);
};


/*****************************************************************************/
/* EUCLIDEAN DISTANCE METRIC                                                 */
/*****************************************************************************/

struct EuclideanDistanceMetric: public DistanceMetric {

    void addRow(int rowNum, const distribution<float> & coords);

    float dist(int rowNum1, int rowNum2,
               const distribution<float> & coords1,
               const distribution<float> & coords2) const;

    /// Pre cached ||vec||^2 for each row, to allow optimization of the
    /// calculation.
    std::vector<double> sum_dist;

    /// Static method to perform the calculation, with no caching
    static float calc(const distribution<float> & coords1,
                      const distribution<float> & coords2);
};


/*****************************************************************************/
/* COSINE DISTANCE METRIC                                                    */
/*****************************************************************************/

struct CosineDistanceMetric: public DistanceMetric {

    void addRow(int rowNum, const distribution<float> & coords);

    float dist(int rowNum1, int rowNum2,
               const distribution<float> & coords1,
               const distribution<float> & coords2) const;

    /// Pre-cached reciprocal of the two norm of each vector, to allow
    /// optimization of the calculation.
    std::vector<double> two_norm_recip;
    
    /// Static method to perform the calculation, with no caching
    static float calc(const distribution<float> & coords1,
                      const distribution<float> & coords2);
};



} // namespace MLDB


