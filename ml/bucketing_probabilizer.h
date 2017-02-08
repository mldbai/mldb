// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* bucketing_probabilizer.h                                          -*- C++ -*-
   Francois Maillet, September 14, 2011
   Copyright (c) 2011 Recoset.  All rights reserved.
*/

#pragma once

#include <string>
#include <vector>

#include "mldb/jml/db/persistent.h"
#include "mldb/ml/prediction_accumulator.h"

namespace MLDB {

/*****************************************************************************/
/* Probabilizer                                                              */
/*****************************************************************************/

class Probabilizer {
public:
    Probabilizer() {}
    virtual float getProb(float score) const = 0;
};


/*****************************************************************************/
/* IdentityProbabilizer                                                      */
/*****************************************************************************/

class IdentityProbabilizer : public Probabilizer {
public:
    float getProb(float score) const
    {
        return score;
    }
};

/*****************************************************************************/
/* Bucketing Probabilizer                                                    */
/*****************************************************************************/

class Bucketing_Probabilizer : public Probabilizer {
public:
    static Prediction_Accumulator::CTR_Buckets
    trainBucketingProbabilizer( 
            Prediction_Accumulator & predAccum, unsigned num_buckets,
            const std::string & calibrateToGroup="",
            std::pair<int, int> calibrateCountsRef=std::make_pair(0,0));
    
    static Prediction_Accumulator::CTR_Buckets
    autoTrainBucketingProbabilizer(Prediction_Accumulator & predAccum,
            int num_buckets = 100, const std::string & calibrateToGroup="",
            std::pair<int, int> calibrateCountsRef=std::make_pair(0,0));
    
    static Bucketing_Probabilizer getBucketingProbabFromExpConfig(
            const std::string & trainedPath,
            const std::string & configPath,
            int probId);

    Bucketing_Probabilizer(const std::string & load_from);
    Bucketing_Probabilizer(const std::string & name, 
            const std::string & prob_type,
            Prediction_Accumulator::CTR_Buckets ctr_buckets);
    
    Bucketing_Probabilizer(Json::Value probConfig, Json::Value buckets);

    static Prediction_Accumulator::CTR_Buckets jsValToCtrBuckets(const Json::Value & jsBuck);

    std::vector<double> getCTRs() const;
    Prediction_Accumulator::CTR_Buckets getCTRBuckets() const;

    float getProb(float score) const;
    size_t getBucket(float score) const;
    float getProbForBucket(size_t bucket) const;

    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);

    void save(const std::string & filename) const;

   
    std::string getType() const;

private:
    std::string name;
    std::string type;
    std::string prob_type;
    std::vector<double> ctrs;
    Prediction_Accumulator::CTR_Buckets ctr_buckets;

    void doInit(const std::string & name, const std::string & prob_type,
            Prediction_Accumulator::CTR_Buckets & ctr_buckets);

    const static double TINY_PROB;
};

} // namespace MLDB

