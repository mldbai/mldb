// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* prediction_accumulator.h                                         -*- C++ -*-
   Francois Maillet, May 5, 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include <string>
#include <vector>
#include <map>
#include <unordered_map>

#include "mldb/jml/stats/distribution.h"
#include "mldb/ml/separation_stats.h"

namespace MLDB {

struct ClassStats {
    int count;
    float mean;
    float std;
};

struct PredictionStats {
    ClassStats pos;
    ClassStats neg;
};


/*****************************************************************************/
/* Prediction_Accumulator                                                    */
/*****************************************************************************/

class Prediction_Accumulator {
public:
    // click, pred, uid, groupId
    typedef std::tuple<int, float, Utf8String, std::string> Prediction;

	Prediction_Accumulator();
	Prediction_Accumulator(const std::string& loadFrom);

	void addPrediction(bool click, float prediction, Utf8String uid=Utf8String(),
            const std::string & groupId="");
	std::vector<Prediction> getPredictions();

    PredictionStats getPredictionStats();

	void dumpToFile(const std::string& filename);

    // calibration params explained in getCTRBuckets function header
	void dumpClassHistogram(
            const std::string& outpath,
            const std::vector<float>& bC_vec,
            const std::string & calibrateToGroup="",
            std::pair<int, int> calibrateCountsRef=std::make_pair(0,0));

    ScoredStats getScoredStats(bool weightByClass=true) const;

    std::map<std::string, float> getGroupCalibration() const;

    /**
     * Compute buckets used by probabilizer
     *  calibrateToGroup: if specified, make sure the contributions of all groups
     *      match the apriori probability of that specific group.
     *  calibrateCountsRef: if specified and if the number of occurences of the
     *      specified group specified by calibrateGroup are insifficient to get
     *      a valid estimate, use this ratio as the CxR to calibrate to
     * */
	typedef std::vector<std::pair<double, std::pair<double, double>>> CTR_Buckets;
	CTR_Buckets getCTRBuckets(size_t num_buckets,
            const std::string & calibrateToGroup="",
            std::pair<int, int> calibrateCountsRef=std::make_pair(0,0));

protected:
    bool printedDebugInfo;

    std::vector<Prediction> predictionsAccum;

    std::map<Utf8String, unsigned> uid_counts;

    bool groupsCalibrated;
    std::map<std::string, float> groupCalibration;

    unsigned num_per_class[2];

    struct bucket_config
    {                           // for default [0,1] range
        float initial_bucket;   //   = 0.05;
        float max_bucket;       //   = 1.05;
        float bucket_increment; //   = 0.05;
    };

    struct PredsPerClass {
        // each one has the prediction and the weight
        std::vector<std::pair<float, float>> neg;
        std::vector<std::pair<float, float>> pos;

        static distribution<float> getDistFor(
                const std::vector<std::pair<float, float>> & preds)
        {
            distribution<float> tp(preds.size());
            for(int i=0; i<preds.size(); i++)
                tp[i] = preds[i].first;
            return tp;
        }
	};

    /**
     * if we need to weight for each group. this is required when we're getting
     * data grom multiple groups/sources (ie rtbopt campaigns) that have different
     * a priori probability of the outcome. if we don't weight examples to compensate
     * for higher/lower a priori probs in the different gropu then the one we're going
     * to be using it with (ie current rtbopt campaign) we will end up with probs
     * that are completelly off
     * */
    void computeGroupCalibration(const std::string & calibrateToGroup,
            const std::pair<int, int> & calibrateCountsRef=std::make_pair(0,0));

    float getUidWeight(const Utf8String & uid, const std::string & groupId) const;

    PredsPerClass getSortedPreds(const std::string & calibrateToGroup="",
            const std::pair<int, int> & calibrateCountsRef=std::make_pair(0,0));

    // Compute buckets used for class histogram
    bucket_config assertValidBucketConfig(
            const std::vector<float> & bC,
            const std::vector<std::pair<float, float>> & neg,
            const std::vector<std::pair<float, float>> & pos);
    std::map<std::string, float> getBucketsForClass(
            const std::vector<std::pair<float, float>> & predictions,
            const bucket_config& bC);
};

} // namespace MLDB

