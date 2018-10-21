/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include "randomforest_types.h"

namespace MLDB {
namespace RF {





// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether
MLDB_NEVER_INLINE std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */);

// Calculates the score of a split, which is a measure of the
// amount of mutual entropy between the label and the given
// candidate split point.
inline double scoreSplit(const W & wFalse, const W & wTrue)
{
    double score
        = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                   + sqrt(wTrue[0] * wTrue[1]));
    return score;
};

// Chooses which is the best split for a given feature.
MLDB_NEVER_INLINE
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */>
chooseSplitKernel(const W * w /* at least maxBucket + 1 entries */,
                  int maxBucket,
                  bool ordinal,
                  const W & wAll);
    
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */,
           bool /* feature is still active */ >
testFeatureNumber(int featureNum,
                  const std::vector<Feature> & features,
                  Rows::RowIterator rowIterator,
                  size_t numRows,
                  const W & wAll);

/** Main kernel for random forest training.

    Test all features for a split.  Returns the feature number,
    the bucket number and the goodness of the split.

    Outputs
    - Z score of split
    - Feature number
    - Split point (bucket number)
    - W for the left side of the split
    - W from the right side of the split

    Features that are inactive from here on are recorded by mutating
    the active flag in the features argument.
*/

std::tuple<double, int, int, W, W>
testAll(int depth,
        std::vector<Feature> & features,
        const Rows & rows);

} // namespace RF
} // namespace MLDB
