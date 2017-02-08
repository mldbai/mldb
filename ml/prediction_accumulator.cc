// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* prediction_accumulator.cc
   Francois Maillet, May 5, 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
*/

#include "prediction_accumulator.h"

using namespace std;

#include <iostream>
#include <fstream>
#include "mldb/arch/exception.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/ml/jml/buckets.h"
#include <limits>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include "confidence_intervals.h"

namespace MLDB {

/*****************************************************************************/
/*   PREDICTION_ACCUMULATOR                                                  */
/*****************************************************************************/

Prediction_Accumulator::
Prediction_Accumulator() : printedDebugInfo(false), groupsCalibrated(false)
{
    for(int i=0; i<2; i++)
        num_per_class[i] = 0;
}

Prediction_Accumulator::
Prediction_Accumulator(const string& loadFrom) :
    printedDebugInfo(false), groupsCalibrated(false)
{
    for(int i=0; i<2; i++)
        num_per_class[i] = 0;

    cout << "PredAccum:: Loading from: " << loadFrom << endl;
    filter_istream file(loadFrom);
    string line;
    int i=0;
    while(getline(file, line))
    {
        vector<string> tokens;
        boost::split(tokens, line, boost::is_any_of(" "));

        int c       = boost::lexical_cast<int>(tokens[0]);
        float score = boost::lexical_cast<float>(tokens[1]);
        Utf8String uid = std::move(tokens[2]);
        
        addPrediction(c==1, score, uid);
    }
    cout << "  > Added " << i << " scores" << endl;
}

void Prediction_Accumulator::
addPrediction(bool click, float pred, Utf8String uid, const string & groupId)
{
    // if we previously calibrated groups, the calibration is now
    // invalid
    groupsCalibrated = false;
	predictionsAccum.emplace_back(click, pred, uid, groupId);
    num_per_class[click] += 1;

    auto uid_it = uid_counts.find(uid);
    if(uid_it == uid_counts.end()) {
        uid_counts.insert(make_pair(uid, 1));
    } else {
        uid_it->second += 1;
    }
}

vector<Prediction_Accumulator::Prediction> Prediction_Accumulator::
getPredictions()
{
	return predictionsAccum;
}

void Prediction_Accumulator::
dumpToFile(const string& filename)
{
	filter_ostream f;
	f.open(filename, ios::out);

	// Print header
	f << "this.predictions=[\n";

    // For each prediction
    for(int pred=0; pred<predictionsAccum.size(); pred++) {
        auto & currPred = predictionsAccum[pred];
        f << "["<< std::get<0>(currPred) << "," << std::get<1>(currPred) << "],";
    }
    f << "],\n";
}
    
PredictionStats Prediction_Accumulator::
getPredictionStats()
{
    auto compute = [] (ClassStats & receiver,
            const vector<pair<float, float>> & preds)
        {
            auto tp = PredsPerClass::getDistFor(preds);

            receiver.count = tp.size();
            receiver.mean = tp.mean();
            receiver.std  = tp.std();
        };

    PredictionStats stats;
    PredsPerClass ppc = Prediction_Accumulator::getSortedPreds();
    compute(stats.pos, ppc.pos);
    compute(stats.neg, ppc.neg);
    return stats;
}

Prediction_Accumulator::bucket_config
Prediction_Accumulator::assertValidBucketConfig(
        const vector<float> & bC_vec,
        const vector<pair<float, float>> & neg_vec,
        const vector<pair<float, float>> & pos_vec)
{
    // If we're specifying the bucket config
    if (bC_vec.size() == 3) {
        bucket_config bC    = bucket_config();
        bC.initial_bucket   = bC_vec[0];
        bC.max_bucket       = bC_vec[1];
        bC.bucket_increment = bC_vec[2];
        return bC;
    }

    // If we want adaptive parameters
    if (bC_vec.size() == 1 && bC_vec[0]==-1) {
        auto neg = PredsPerClass::getDistFor(neg_vec);
        auto pos = PredsPerClass::getDistFor(pos_vec);

        double negMean = neg.mean();
        double posMean = pos.mean();
        double negMax = neg.max();
        double posMax = pos.max();
        double negMin = neg.min();
        double posMin = pos.min();
        double negStd = neg.std();
        double posStd = pos.std();

        int sigmas = 3;
        int numBuckets = 20;
        float minVal = max(min(negMin, posMin), min(negMean - sigmas*negStd, posMean - sigmas*posStd));
        float maxVal = min(max(negMax, posMax), max(negMean + sigmas*negStd, posMean + sigmas*posStd));

        bucket_config bC    = bucket_config();
        bC.bucket_increment = (maxVal - minVal) / numBuckets;
        bC.initial_bucket   = minVal + bC.bucket_increment;
        bC.max_bucket       = maxVal + bC.bucket_increment;

        return bC;
    }

    // If we have given invalid options
    throw MLDB::Exception(MLDB::format("Invalid bucket_config size. Should be 3; %d.",
            (int)bC_vec.size()));
}

void Prediction_Accumulator::
computeGroupCalibration(const string & calibrateToGroup,
        const std::pair<int, int> & calibrateCountsRef)
{
    if(calibrateToGroup == "")
        throw MLDB::Exception("Need to provide a group to calibrate to");

    // find uid weighted total for each group
    map<string, pair<unsigned, float>> groupCounts;
    for(const Prediction & currPred : predictionsAccum) {
        float exWeight = std::get<0>(currPred) ? getUidWeight(std::get<2>(currPred), "") : 0;
        auto it = groupCounts.find(std::get<3>(currPred));
        if(it == groupCounts.end()) {
            groupCounts[std::get<3>(currPred)] = make_pair(1, exWeight);
        }
        else {
            it->second.first ++;            // inc imps
            it->second.second += exWeight;  // inc outcomes
        }
    }

    /****** */
    // determine what the the CxR we need to normalise toa
    // if the target group is not there, it's weight is 0
    auto it = groupCounts.find(calibrateToGroup);
    float target_group_weight = 0;
    float target_group_CxR = 0;
    if(it != groupCounts.end()) {
        unsigned num_target_actions = it->second.second;
        target_group_CxR = num_target_actions / float(it->second.first);
        if(num_target_actions >= 100)
            target_group_weight = 100;
        else 
            target_group_weight = num_target_actions;
    }

    float normaliseToCxR = target_group_CxR;
    cerr << MLDB::format(" target group weight:%0.1f  CxR:%0.4f%% (%s)",
            target_group_weight, target_group_CxR*100, calibrateToGroup) << endl;
    if(target_group_weight < 100) {
        // prefer using the calibrateCountsRef CxR over the weighted average
        // of all other groups if possible
        if(calibrateCountsRef.first >= 50000 && calibrateCountsRef.second >= 2) {
            cerr << MLDB::format(" > Using provided CxR to average to. Counts: %d/%d",
                    calibrateCountsRef.second, calibrateCountsRef.first) << endl;
            float refCxR = calibrateCountsRef.second / (float) calibrateCountsRef.first;

            // get the 75% lower bound
            ConfidenceIntervals cI(1-0.75, "clopper_pearson");
            float lower_bound = cI.binomialLowerBound(calibrateCountsRef.first,
                    calibrateCountsRef.second);
            normaliseToCxR = (refCxR + lower_bound) / 2.0;
        }
        else {
            cerr << " > Need to do a weighted average of the targetCxR" << endl;
            cerr << calibrateCountsRef.first << endl;
            cerr << calibrateCountsRef.second << endl;
            // to a weighted average of all the other campaigns and then 
            // merge is with our
            float CxRaccum = 0;
            float impsAccum = 0;
            for(auto itt = groupCounts.begin(); itt != groupCounts.end(); itt++) {
                float cxr = itt->second.second / float(itt->second.first);
                cerr << MLDB::format("   %s: base CxR: %0.4f%%.  imps:%d",
                        itt->first, cxr * 100, itt->second.first) << endl;
                if(itt->first == calibrateToGroup) continue;
                CxRaccum += cxr * itt->second.first;
                impsAccum += itt->second.first;
            }
            float weightedGroupsCxR = CxRaccum / impsAccum;

            normaliseToCxR = ((target_group_CxR * target_group_weight) +
                    (weightedGroupsCxR * (100-target_group_weight))) / 100.0;
        }
    }
    cerr << "           normaliseToCxR: " <<
        MLDB::format("%0.4f%%", normaliseToCxR*100) << endl;

    // calculate the calibration ratio for each group
    cerr << "  -- Calibration for each group -- " << endl;
    for(auto git = groupCounts.begin(); git != groupCounts.end(); git++) {
        if(git->first == calibrateToGroup) {
            groupCalibration[git->first] = 1;
        }
        else {
            float groupCxR = git->second.second / float(git->second.first);
            groupCalibration[git->first] = groupCxR / normaliseToCxR;

            cerr << MLDB::format("%s : GrpCxR:%0.4f%%    NormRatio:%0.4f",
                    git->first, groupCxR*100, groupCxR / normaliseToCxR) << endl;
        }
    }
    cerr << " ----- " << endl << endl;

    groupsCalibrated = true;
}
    
map<string, float> Prediction_Accumulator::
getGroupCalibration() const
{
    return groupCalibration;
}

float Prediction_Accumulator::
getUidWeight(const Utf8String & uid, const string & groupId) const
{
    float groupCalib = 1;
    if(groupsCalibrated && groupId != "") {
        auto it = groupCalibration.find(groupId);
        if(it != groupCalibration.end()) {
            groupCalib = it->second;
        }
    }

    // if we're not using uida
    if(uid_counts.size()<2) return 1 / groupCalib;

    auto uid_it = uid_counts.find(uid);
    if(uid_it == uid_counts.end()) {
        throw MLDB::Exception("unknown uid should not be possible!");
    }

    return 1.0 / uid_it->second / groupCalib;
};

Prediction_Accumulator::PredsPerClass Prediction_Accumulator::
getSortedPreds(const string & calibrateToGroup,
        const std::pair<int, int> & calibrateCountsRef)
{
    vector<pair<float, float>> pos;
    vector<pair<float, float>> neg;
    float num_weighted_pos = 0;

    if(!groupsCalibrated && calibrateToGroup != "")
        computeGroupCalibration(calibrateToGroup, calibrateCountsRef);


    // Split predictions by pos and negative
    for(int pred=0; pred<predictionsAccum.size(); pred++) {
        const Prediction & currPred = predictionsAccum[pred];
        auto & lst = std::get<0>(currPred)==0 ? neg : pos;
        float uidWeight = getUidWeight(std::get<2>(currPred), std::get<3>(currPred));
        lst.push_back(make_pair(std::get<1>(currPred), uidWeight));
        if(std::get<0>(currPred)==1) num_weighted_pos += uidWeight;
    }

    if(!printedDebugInfo) {
        printedDebugInfo = true;
        float weighted_act_rate = num_weighted_pos / (pos.size() + neg.size());
        float unweighted_act_rate = (float)pos.size() / (pos.size() + neg.size());

        cerr << MLDB::format("Weighted action rate:   %0.4f%%",
                weighted_act_rate * 100) << endl;
        cerr << MLDB::format("Unweighted action rate: %0.4f%%",
                unweighted_act_rate * 100) << endl;
        cerr << MLDB::format("Num unweighted pos:     %d", pos.size()) << endl;
        cerr << MLDB::format("Num sum weighted pos:   %0.4f", num_weighted_pos) << endl;
        cerr << MLDB::format("Num elements:           %d", pos.size() + neg.size()) << endl;
        cerr << endl;
    }

    // Sort
    auto sortFunc = [](const pair<float, float> & a,
            const pair<float, float> & b) -> bool
        { 
            return a.first < b.first; 
        };
    sort(pos.begin(), pos.end(), sortFunc);
    sort(neg.begin(), neg.end(), sortFunc);

    PredsPerClass ppc;
    ppc.neg = neg;
    ppc.pos = pos;
    return ppc;
}

void Prediction_Accumulator::
dumpClassHistogram(const string& out_path, 
        const vector<float>& bC_vec,
        const std::string & calibrateToGroup,
        std::pair<int, int> calibrateCountsRef)
{
    PredsPerClass ppc = Prediction_Accumulator::
        getSortedPreds(calibrateToGroup, calibrateCountsRef);

    const bucket_config bC = assertValidBucketConfig(bC_vec, ppc.neg, ppc.pos);

    // Get buckets
    vector<map<string, float>> classBuckets;
    classBuckets.push_back( getBucketsForClass(ppc.neg, bC) );
    classBuckets.push_back( getBucketsForClass(ppc.pos, bC) );

    // Figure out filename of histogram
    //boost::filesystem::path dir(outfolder);
    //string modelfn = MLDB::format("chist-%s.js", modelname.c_str());
    //boost::filesystem::path file(modelfn);
    //boost::filesystem::path full_path = dir / file;
    cout << "  Writing histogram to: " << out_path << endl;

    // Write output to file
    filter_ostream f;
    f.open(out_path, ios::out);

    f << "this.data={";
    for(int classId=0; classId<2; classId++)
    {
        f << "'" << classId << "':{";
        for(auto it = classBuckets[classId].begin();
                it != classBuckets[classId].end(); it++)
        {
            // 'bucket':prnct
            f << "'" << it->first << "':" << it->second << ",";
        }
        f << "},";
    }

    auto tpos = PredsPerClass::getDistFor(ppc.pos);
    auto tneg = PredsPerClass::getDistFor(ppc.neg);
    
    // stats
    f << "'stats':{";
    f << "'mean':[" << tneg.mean() << "," << tpos.mean() << "],";
    f << "'std':["  << tneg.std()  << "," << tpos.std()  << "],";
    f << "'counts':["  << tneg.size()  << "," << tpos.size()  << "],";
    f << "},";

    f << "}";
}

map<string, float> Prediction_Accumulator::
getBucketsForClass(const vector<pair<float, float>> & preds, 
        const bucket_config& bC)
{
    map<string, float> buckets;
    int numPred = preds.size();
    int idx = 0;

    if (bC.initial_bucket == bC.max_bucket)
        throw MLDB::Exception("The first and last buckets are equal.");

    float lowerBound = bC.initial_bucket - bC.bucket_increment;
    if(preds[idx].first < lowerBound) {
        //there are some below, insert a catch-all
        float inBucket = 0;
        while(idx<numPred && preds[idx].first<lowerBound) {
            inBucket++;
            idx++;
        }
        buckets.insert(pair<string, float>("below", inBucket/numPred));
    }

    for(float topOfBucket =  bC.initial_bucket;
              topOfBucket <= bC.max_bucket;
              topOfBucket += bC.bucket_increment)
    {
        float inBucket = 0;
        while(idx<numPred && preds[idx].first<topOfBucket) {
            inBucket++;
            idx++;
        }

        string bucketName = boost::lexical_cast<string>(topOfBucket - bC.bucket_increment);
        buckets.insert(make_pair(bucketName, inBucket/numPred));
    }

    if(idx != numPred) {
        //there are some above, insert a catch-all
        buckets.insert(make_pair("above", (numPred - idx)/numPred));
    }

    return buckets;
}
    
ScoredStats Prediction_Accumulator::
getScoredStats(bool weightByClass) const
{
    if (predictionsAccum.size()==0)
        throw MLDB::Exception("Cannot get ScoredStats with empty PredAccum.");

    double weights[2];
    if(weightByClass) {
        for(int i=0; i<2; i++)
            weights[i] = 1.0 / num_per_class[i];
    } else {
        for(int i=0; i<2; i++)
            weights[i] = 1;
    }

    ScoredStats result;
    for(const Prediction & pred : predictionsAccum) {
        // TODO this is only weighting by class. should we weight by uid also??
        result.update(std::get<0>(pred), std::get<1>(pred), weights[std::get<0>(pred)]);
    }
    result.calculate();
    return result;
}


Prediction_Accumulator::CTR_Buckets
Prediction_Accumulator::getCTRBuckets(size_t num_buckets,
        const string & calibrateToGroup,
        std::pair<int, int> calibrateCountsRef)
{
    if (predictionsAccum.size()==0)
        throw MLDB::Exception("Cannot compute CTR buckets with empty PredAccum.");

    // Get bucket freqs
    vector<float> values(predictionsAccum.size());
    for(int pred=0; pred<predictionsAccum.size(); pred++)
        values[pred] = std::get<1>(predictionsAccum[pred]);

    ML::BucketFreqs bF = ML::BucketFreqs();
    ML::get_freqs(bF, values);

    // Compute bucket boundaries
    vector<float> buckets;
    ML::bucket_dist_reduced(buckets, bF, num_buckets);

    // Compute CTR for buckets
    const PredsPerClass ppc =
        Prediction_Accumulator::getSortedPreds(calibrateToGroup, calibrateCountsRef);
    CTR_Buckets result_buckets;

    unsigned pos_idx = 0;
    unsigned neg_idx = 0;
    float upper = 0;
    for(unsigned bucket_idx=0; bucket_idx<=buckets.size(); bucket_idx++) {
        // Adjust bounds
        if(bucket_idx==buckets.size()) {
            upper = numeric_limits<float>::max();
        }
        else {
            upper = buckets[bucket_idx];
        }

        // Count number of elements in this bucket. Since the lists are sorted,
        // we only have to check against the top bucket
        int nbr_pos = 0;
        float weighted_pos = 0;
        while(pos_idx<ppc.pos.size() && ppc.pos[pos_idx].first<upper) {
            weighted_pos += ppc.pos[pos_idx].second;
            nbr_pos++;
            pos_idx++;
        }

        int nbr_neg = 0;
        while(neg_idx<ppc.neg.size() && ppc.neg[neg_idx].first<upper) {
            nbr_neg++;
            neg_idx++;
        }

        //std::vector<std::pair<float, std::pair<int, int>>>
        result_buckets.push_back( make_pair(upper, make_pair(weighted_pos, nbr_pos+nbr_neg)) );
    }

    // if we're returning only 1 bucket, check if the reason is that there is
    // only a single unique score for all examples
    if(result_buckets.size() == 1) {
        bool unique = true;
        float val = -1;
        for(int i=0; i<predictionsAccum.size(); i++) {
            if(i==0) {
                val = std::get<1>(predictionsAccum[i]);
                continue;
            }

            if(val != std::get<1>(predictionsAccum[i])) {
                unique = false;
                break;
            }
        }

        if(unique) {
            cerr << MLDB::format(" WARNING!!!! When calling getCTRBuckets, "
                    "prediction accumulator contains %d times the unique "
                    "value of %0.6f", predictionsAccum.size(), val) << endl;
        }
    }

    return result_buckets;
}

} // namespace MLDB
