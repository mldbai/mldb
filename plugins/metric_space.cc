// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** metric_space.cc
    Jeremy Barnes, 25 April 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Create a metric space.
*/

#include "metric_space.h"
#include "mldb/jml/stats/distribution_simd.h"
#include "ml/value_descriptions.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/distribution_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/base/exc_assert.h"
#include "mldb/arch/simd_vector.h"

using namespace std;



namespace MLDB {

DEFINE_ENUM_DESCRIPTION(MetricSpace);

MetricSpaceDescription::
MetricSpaceDescription()
{
    addValue("none", METRIC_NONE, "No metric is chosen.  This will cause an error.");
    addValue("euclidean", METRIC_EUCLIDEAN, "Use Euclidian distance for metric. "
             "This is a good choice for geometric embeddings like the t-SNE "
             "algorithm.");
    addValue("cosine", METRIC_COSINE, "Use cosine distance for metric.  This is "
             "a good choice for normalized and high-dimensional embeddings like the SVD.");
}


/*****************************************************************************/
/* DISTANCE METRIC                                                           */
/*****************************************************************************/

DistanceMetric *
DistanceMetric::
create(MetricSpace space)
{
    switch (space) {
    case METRIC_NONE:
        throw HttpReturnException(400, "No metric space was specified");
    case METRIC_EUCLIDEAN:
        return new EuclideanDistanceMetric();
    case METRIC_COSINE:
        return new CosineDistanceMetric();
    default:
        throw HttpReturnException(400, "Unknown distance metric space "
                                  + to_string(space));
    }
}


/*****************************************************************************/
/* EUCLIDEAN DISTANCE METRIC                                                 */
/*****************************************************************************/

void
EuclideanDistanceMetric::
addRow(int rowNum, const distribution<float> & coords)
{
    ExcAssertEqual(rowNum, sum_dist.size());
    sum_dist.push_back(coords.dotprod(coords));
    ExcAssert(isfinite(sum_dist.back()));
}

float
EuclideanDistanceMetric::
calc(const distribution<float> & coords1,
     const distribution<float> & coords2)
{
    return sqrt(ML::SIMD::vec_euclid(coords1.data(), coords2.data(), coords1.size()));
    return (coords2 - coords1).two_norm();
}

float
EuclideanDistanceMetric::
dist(int rowNum1, int rowNum2,
     const distribution<float> & coords1,
     const distribution<float> & coords2) const
{
    ExcAssertEqual(coords1.size(), coords2.size());

    if (rowNum1 == -1 || rowNum2 == -1) {
        return calc(coords1, coords2);
    }

    // Make sure dist(x,y) == dist(y,x) irrespective of rounding
    if (rowNum2 < rowNum1)
        return dist(rowNum2, rowNum1, coords2, coords1);

    // Make sure dist(x,x) == 0 irrespective of rounding
    if (rowNum1 == rowNum2)
        return 0.0;

    /*  Given two points x and y, whose coordinates are coords[x] and
        coords[y], this will calculate the euclidian distance
        ||x - y|| = sqrt(sum_i (x[i] - y[i])^2)
                  = sqrt(sum_i (x[i]^2 + y[i]^2 - 2 x[i]y[i]) )
                  = sqrt(sum_i x[i]^2 + sum_i y[i]^2 - 2 sum x[i]y[i])
                  = sqrt(||x||^2 + ||y||^2 - 2 x . y)
            
        Must satisfy the triangle inequality, so the sqrt is important.  We
        also take pains to ensure that dist(x,y) === dist(y,x) *exactly*,
        and that dist(x,x) == 0.
    */
        
    // Use the optimized version, since we know the sum
    float dpResult = -2.0 * ML::SIMD::vec_dotprod_dp(&coords1[0],
                                                     &coords2[0],
                                                     coords1.size());
    ExcAssert(isfinite(dpResult));



    float distSquared = dpResult + sum_dist.at(rowNum1) + sum_dist.at(rowNum2);
    ExcAssert(isfinite(distSquared));

    // Deal with rounding errors
    if (distSquared < 0.0)
        distSquared = 0.0;

    return sqrtf(distSquared);
}


/*****************************************************************************/
/* COSINE DISTANCE METRIC                                                    */
/*****************************************************************************/

void
CosineDistanceMetric::
addRow(int rowNum, const distribution<float> & coords)
{
    ExcAssertEqual(rowNum, two_norm_recip.size());
    float twonorm = coords.two_norm();

    // If it's zero, we store the non-finite reciprocal, and the distance
    // it and any vector with a finite reciprocal is 1.

    if (twonorm == 0.0) {
        two_norm_recip.push_back(1.0 / 0.0);
        return;
        throw HttpReturnException(400, "Attempt to add zero magnitude vector to cosine distance",
                                  "coords", coords,
                                  "twoNorm", twonorm);
    }

    if (!isfinite(twonorm))
        throw HttpReturnException(400, "Attempt to add vector with non-finite two norm "
                                  "to cosine distance",
                                  "coords", coords,
                                  "twoNorm", twonorm);
    float recip = 1.0 / twonorm;
    if (!isfinite(recip))
        throw HttpReturnException(400, "Attempt to add vector with non-finite two norm reciprocal "
                                  "to cosine distance",
                                  "coords", coords,
                                  "twoNorm", twonorm,
                                  "recip", recip);
    
    two_norm_recip.push_back(recip);
}

float
CosineDistanceMetric::
calc(const distribution<float> & coords1,
     const distribution<float> & coords2)
{
    if ((coords1 == coords2).all())
        return 0.0;

    double norm1 = coords1.two_norm();
    double norm2 = coords2.two_norm();

    if (norm1 == 0.0 && norm2 == 0.0) {
        return 0.0;
    }
    
    if (norm1 == 0.0 || norm2 == 0.0) {
        return 1.0;

        throw HttpReturnException(400, "Error: can't calculate cosine distance between "
                                  "zero length vectors",
                                  "vec1", coords1,
                                  "vec2", coords2,
                                  "vec1norm", norm1,
                                  "vec2norm", norm2);
    }
    
    return std::max(1 - (coords1.dotprod(coords2) / (norm1 * norm2)), 0.0);
}

float
CosineDistanceMetric::
dist(int rowNum1, int rowNum2,
     const distribution<float> & coords1,
     const distribution<float> & coords2) const
{
    ExcAssertEqual(coords1.size(), coords2.size());

    if (rowNum1 == -1 || rowNum2 == -1) {
        return calc(coords1, coords2);
    }

    // Make sure dist(x,y) == dist(y,x) irrespective of rounding
    if (rowNum2 < rowNum1)
        return dist(rowNum2, rowNum1, coords2, coords1);

    // Make sure dist(x,x) == 0 irrespective of rounding
    if (rowNum1 == rowNum2)
        return 0.0;

    if (!isfinite(two_norm_recip.at(rowNum1))
        && !isfinite(two_norm_recip.at(rowNum2))) {
        return 0.0;
    }
    if (!isfinite(two_norm_recip.at(rowNum1))
        || !isfinite(two_norm_recip.at(rowNum2))) {
        return 1.0;
    }

    float result = 1.0 - coords1.dotprod(coords2) * two_norm_recip.at(rowNum1) * two_norm_recip.at(rowNum2);
    if (result < 0.0) {
        result = 0.0;
    }

    ExcAssert(isfinite(result));
    ExcAssertGreaterEqual(result, 0.0);

    return result;
}



} // namespace MLDB

