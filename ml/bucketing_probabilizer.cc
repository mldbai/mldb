// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* bucketing_probabilizer.cc
   Francois Maillet, September 14, 2011
   Copyright (c) 2011 Recoset.  All rights reserved.
*/

#include "bucketing_probabilizer.h"

using namespace std;

#include <iostream>
#include <fstream>
#include "mldb/arch/exception.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/vfs/filter_streams.h"
#include "confidence_intervals.h"
#include <math.h>

namespace MLDB {



/*****************************************************************************/
/*   Bucketing_Probabilizer                                                  */
/*****************************************************************************/

const double Bucketing_Probabilizer::TINY_PROB = 0.0000001;

Prediction_Accumulator::CTR_Buckets Bucketing_Probabilizer::
trainBucketingProbabilizer(
        Prediction_Accumulator & predAccum, unsigned num_buckets,
        const string & calibrateToGroup,
        std::pair<int, int> calibrateCountsRef)
{
    // Compute buckets
    auto ctr_buckets = predAccum.getCTRBuckets(num_buckets, calibrateToGroup,
            calibrateCountsRef);
    if(ctr_buckets.size()==1)
        throw MLDB::Exception(MLDB::format("Problem with CTR buckets. Asked for "
                    "%d buckets; only one back.", num_buckets));
    if(ctr_buckets.size()<num_buckets)
        cerr << MLDB::format("WARNING. Asked for %d buckets, got %d back",
                num_buckets, ctr_buckets.size()) << endl;
    
    return ctr_buckets;
}

Prediction_Accumulator::CTR_Buckets Bucketing_Probabilizer::
autoTrainBucketingProbabilizer(Prediction_Accumulator & predAccum,
        int num_buckets, const string & calibrateToGroup,
        std::pair<int, int> calibrateCountsRef)
{
    map<int, Prediction_Accumulator::CTR_Buckets> bucketsCache;
    while(num_buckets>2) {
        if(num_buckets%5==0)
            cout << "  trying with num_buckets : " << num_buckets << endl;

        // Train prob
        auto ctr_buckets = Bucketing_Probabilizer::
            trainBucketingProbabilizer(predAccum, num_buckets,
                    calibrateToGroup, calibrateCountsRef);

        // Check CTRs
        float lastCTR = -1;
        bool done = true;
        float totalImps = 0;
        for(auto ctr_bucket : ctr_buckets) {
            double clicks = ctr_bucket.second.first;
            double imps = ctr_bucket.second.second;
            totalImps += imps;
            double currCTR = clicks/imps;
            // Tolerate at most 2% lower CTR
            if(currCTR>lastCTR*0.98) {
                lastCTR = currCTR;
                continue;
            } else {
                done = false;
                break;
            }
        }

        // Check for imp proportion
        // we don't want to end up with a bucket having almost no impressions
        if(done) {
            for(auto ctr_bucket : ctr_buckets) {
                double imps = ctr_bucket.second.second;
                double p = imps/totalImps / (1.0/ctr_buckets.size());
                if (p < 0.1) {
                    cout << MLDB::format("   Rejected because a bucket is only %0.3f "
                            "of what it should be. Min is 0.1", p) << endl;
                    done = false;
                    break;
                }
            }
        }

        if(done) {
            cout << "[AutoBucketingProbTrainer] Training successful with " << num_buckets <<
                " buckets." << endl;
            return ctr_buckets;
        } else {
            // save the current bucketing for error printing in case everything fails
            if(num_buckets <= 5) {
                bucketsCache.insert(std::make_pair(num_buckets, ctr_buckets));
            }
            num_buckets = min(num_buckets-1, (int)(ctr_buckets.size() - 1));
            continue;
        }
    }

    cerr << "=== Probabilizer training will fail. Printing last 5 bucketing attemps ===" << endl;
    for(auto it = bucketsCache.begin(); it != bucketsCache.end(); it++) {
        cerr << MLDB::format("  %d buckets:", it->first) << endl;
        for(const std::pair<double, std::pair<double, double>> & b : it->second) {
        cerr << MLDB::format("     thresh: %0.4f     %0.6f/%0.6f = %0.8f", b.first,
                b.second.first, b.second.second, b.second.first/b.second.second) << endl;
        }
    }
    throw MLDB::Exception("Unable to train probabilizer.");
}
Bucketing_Probabilizer::
Bucketing_Probabilizer(const std::string & load_from)
{
    throw MLDB::Exception("deprecated");
    //filter_istream stream(load_from);
    //ML::DB::Store_Reader store(stream);
    //reconstitute(store);
    // doInit();
}

Bucketing_Probabilizer Bucketing_Probabilizer::
getBucketingProbabFromExpConfig(const std::string & trainedPath,
        const std::string & configPath, int probId)
{
    // try to load it as if it was a json. if ever this was saved using the 
    auto loadJson = [](const string & path)
        {
            Json::Value root;
            Json::Reader reader;

            filter_istream stream(path);
            string nextLine;
            string the_json = "";

            try {
                int i=0;
                while(getline(stream, nextLine)) {
                    // since we're generating the config in coffee, the first line
                    // will be: this['blabla']
                    if(i++==0 && nextLine.substr(0, 5)=="this[") continue;
                    the_json += nextLine;
                }
            } catch(std::exception & e) {
                cerr << "Error reading in file: '" << path << 
                    "':\n" << e.what() << endl;
            }

            // remove trailing ; if present
            if(the_json.size()>0 && the_json.substr(the_json.size()-1) == ";")
                the_json = the_json.substr(0, the_json.size()-1);

            bool parsingSuccessful = reader.parse(the_json, root);
            if (!parsingSuccessful)
            {
                std::string errorMsg = "Error parsing JSON in file '" +
                    path + "':\n" +
                    reader.getFormattedErrorMessages();
                throw MLDB::Exception(errorMsg);
            }

            return root;
        };

    auto trainedProb = loadJson(trainedPath);
    auto probSubConfig = loadJson(configPath);

    Json::Value empty;
    return Bucketing_Probabilizer(
            probSubConfig.get("training", empty).get("probabilizers", empty)[probId],
            trainedProb);
}

Bucketing_Probabilizer::
Bucketing_Probabilizer(const string & name,
        const std::string & prob_type,
        Prediction_Accumulator::CTR_Buckets ctr_buckets) : 
    name(name), prob_type(prob_type), ctr_buckets(ctr_buckets)
{
    doInit(name, prob_type, ctr_buckets);
}

Prediction_Accumulator::CTR_Buckets Bucketing_Probabilizer::
jsValToCtrBuckets(const Json::Value & jsBuck)
{
    Prediction_Accumulator::CTR_Buckets buckets;
    for(int i=0; i<jsBuck.size(); i++) {
        auto b = jsBuck[i];
        double imps   = b[1][0].asDouble();
        double clicks = b[1][1].asDouble();
        double thresh = b[0].asDouble();
        buckets.push_back(make_pair(thresh, make_pair(imps, clicks)));
    }
    return buckets;
}
    
Bucketing_Probabilizer::
Bucketing_Probabilizer(Json::Value probConfig, Json::Value buckets)
{
    ctr_buckets = jsValToCtrBuckets(buckets);
    doInit(probConfig["name"].asString(),
            probConfig["prob_type"].asString(), ctr_buckets);
}

void Bucketing_Probabilizer::
doInit(const std::string & name, const std::string & prob_type, 
        Prediction_Accumulator::CTR_Buckets & ctr_buckets)
{
    type = "bucketing";

    typedef std::function<double (double, double)> ProbFunc;
    ProbFunc compute_prob;

    if(prob_type=="raw") {
        compute_prob = [] (double clicks, double imps) { return clicks/imps; };
    }
    else if(prob_type=="lower_bound") {
        float confidence = 0.8;
        ConfidenceIntervals confInterval((1-confidence)/2);

        compute_prob = [=] (double clicks, double imps)
            { 
                return confInterval.binomialLowerBound(imps, clicks);
            };
    }
    else {
        throw MLDB::Exception("Unknown prob type '"+prob_type+"'");
    }

    // Precompute prob for buckets
    for(auto bucket : ctr_buckets) {
        double imps   = bucket.second.second;
        double clicks = bucket.second.first;
        double prob = compute_prob(clicks, imps);
        if (prob<=0) prob = TINY_PROB;
        ctrs.push_back(prob);
    }

}

size_t Bucketing_Probabilizer::
getBucket(float score) const
{
    if(ctr_buckets.size()==0)
        throw MLDB::Exception("ctr buckets is empty!");

    for(unsigned bucket_id=0; bucket_id<ctr_buckets.size(); bucket_id++) {
        if(ctr_buckets[bucket_id].first < score)
            continue;

        return bucket_id;
    }

    return ctr_buckets.size() -1;
}

float Bucketing_Probabilizer::
getProbForBucket(size_t bucket) const
{
    if(ctr_buckets.size()==0)
        throw MLDB::Exception("ctr buckets is empty!");

    if(bucket >= ctr_buckets.size())
        throw MLDB::Exception("bucket id out of bounds");

    return ctrs[bucket];
}

float Bucketing_Probabilizer::
getProb(float score) const
{
    if(ctr_buckets.size()==0)
        throw MLDB::Exception("ctr buckets is empty!");

    for(unsigned bucket_id=0; bucket_id<ctr_buckets.size(); bucket_id++) {
        if(ctr_buckets[bucket_id].first < score)
            continue;

        // if we're at the first or last bucket, return the prob
        if(bucket_id==0 || bucket_id==ctr_buckets.size()-1) {
            return ctrs[bucket_id];
        }

        // smooth the output prob by assuming a linear progression
        // between the current and last bucket
        const auto & bucket = ctr_buckets[bucket_id];
        const auto & curr_ctr = ctrs[bucket_id];
        const auto & prev_bucket = ctr_buckets[bucket_id-1];
        const auto & prev_ctr = ctrs[bucket_id-1];

        float bucket_width = bucket.first - prev_bucket.first;
        float prnct_of_bucket = (bucket.first - score) / bucket_width;
        float bucket_height = curr_ctr - prev_ctr;

        return max(curr_ctr - (prnct_of_bucket * bucket_height), TINY_PROB);
    }

    return max(ctrs.back(), TINY_PROB);
}

Prediction_Accumulator::CTR_Buckets Bucketing_Probabilizer::
getCTRBuckets() const
{
    return ctr_buckets;
}
    
std::vector<double> Bucketing_Probabilizer::
getCTRs() const
{
    return ctrs;
}
    
std::string Bucketing_Probabilizer::
getType() const
{
    return type;
}

void Bucketing_Probabilizer::
serialize(ML::DB::Store_Writer & store) const
{
    int version = 1;
    store << version << ctr_buckets << ctrs << 
        name << type << prob_type;
}

void Bucketing_Probabilizer::
reconstitute(ML::DB::Store_Reader & store)
{
    int version;
    store >> version;
    if (version != 1)
        throw MLDB::Exception("wrong Bucketing_Probabilizer version!");

    store >> ctr_buckets >> ctrs >> 
        name >> type >> prob_type;
}

void Bucketing_Probabilizer::
save(const string & filename) const
{
    filter_ostream stream(filename);
    ML::DB::Store_Writer store(stream);
    serialize(store);
}


} // namespace MLDB
