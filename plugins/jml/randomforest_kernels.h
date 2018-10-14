/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "randomforest_types.h"

namespace MLDB {
namespace RF {


// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether
template<typename GetNextRowFn>
static std::pair<bool, int>
testFeatureKernel(GetNextRowFn&& getNextRow,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    // Number of the last bucket we saw.  Enables us to determine if
    // we change buckets at any point.
    int lastBucket = -1;

    // Number of times we've changed bucket numbers.  Since lastBucket
    // starts off at -1, this will be incremented to 0 on the first loop
    // iteration.
    int bucketTransitions = -1;

    // Maximum bucket number we've seen.  Can significantly reduce the
    // work required to search the buckets later on, as those without
    // an example have no possible split point.
    int maxBucket = -1;

    for (size_t j = 0;  j < numRows;  ++j) {
        DecodedRow r = getNextRow();
        int bucket = buckets[r.exampleNum];
        ExcAssertLess(bucket, buckets.numBuckets);
        bucketTransitions += (bucket != lastBucket ? 1 : 0);
        lastBucket = bucket;

        w[bucket][r.label] += r.weight;
        maxBucket = std::max(maxBucket, bucket);
    }

    return { bucketTransitions > 0, maxBucket };
}

// Calculates the score of a split, which is a measure of the
// amount of mutual entropy between the label and the given
// candidate split point.
static double scoreSplit(const W & wFalse, const W & wTrue)
{
    double score
        = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                   + sqrt(wTrue[0] * wTrue[1]));
    return score;
};

// Chooses which is the best split for a given feature.
static std::tuple<double /* bestScore */,
                  int /* bestSplit */,
                  W /* bestLeft */,
                  W /* bestRight */>
chooseSplitKernel(const W * w /* at least maxBucket + 1 entries */,
                  int maxBucket,
                  bool ordinal,
                  const W & wAll)
{
    double bestScore = INFINITY;
    int bestSplit = -1;
        
    W bestLeft;
    W bestRight;

    if (ordinal) {
        // Calculate best split point for ordered values
        W wFalse = wAll, wTrue;

        // Now test split points one by one
        for (unsigned j = 0;  j < maxBucket;  ++j) {
            if (w[j].empty())
                continue;                   

            double s = scoreSplit(wFalse, wTrue);

#if 0                
            if (debug) {
                std::cerr << "  ord split " << j << " "
                          << features.info->bucketDescriptions.getValue(j)
                          << " had score " << s << std::endl;
                std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                std::cerr << "    true:  " << wTrue[0] << " " << wTrue[1] << std::endl;
            }
#endif
                
            if (s < bestScore) {
                bestScore = s;
                bestSplit = j;
                bestRight = wFalse;
                bestLeft = wTrue;
            }
                
            wFalse -= w[j];
            wTrue += w[j];
        }
    }
    else {
        // Calculate best split point for non-ordered values
        // Now test split points one by one

        for (unsigned j = 0;  j <= maxBucket;  ++j) {
                    
            if (w[j].empty())
                continue;

            W wFalse = wAll;
            wFalse -= w[j];                    

            double s = scoreSplit(wFalse, w[j]);

#if 0                    
            if (debug) {
                std::cerr << "  non ord split " << j << " "
                          << features.info->bucketDescriptions.getValue(j)
                          << " had score " << s << std::endl;
                std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                std::cerr << "    true:  " << w[j][0] << " " << w[j][1] << std::endl;
            }
#endif
                    
            if (s < bestScore) {
                bestScore = s;
                bestSplit = j;
                bestRight = wFalse;
                bestLeft = w[j];
            }
        }

    }

    return { bestScore, bestSplit, bestLeft, bestRight };
}
    
} // namespace RF
} // namespace MLDB
