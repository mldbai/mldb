// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* dense_classifier.cc
   Jeremy Barnes, 12 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#include "dense_classifier.h"
#include "mldb/jml/utils/vector_utils.h"


using namespace ML;
using namespace std;


namespace MLDB {



Json::Value
explanationToJson(const ML::Explanation & expl,
                  const ML::Feature_Set & fset,
                  int nFeatures)
{
    Json::Value result;

    auto data = expl.explainRaw(nFeatures, true);

    for (unsigned i = 0;  i < data.size();  ++i) {
        auto feat = data[i].first;
        float score = data[i].second;

        string featName;
        string featValue;
        if (feat == ML::MISSING_FEATURE) {
            featName = "<<BIAS>>";
            featValue = "";
        }
        else {
            featName = expl.fspace->print(feat);
            featValue = expl.fspace->print(feat, (fset)[feat]);
        }

        result[i]["feature"] = featName;
        result[i]["value"]   = featValue;
        result[i]["score"]   = score;
    }

    return result;
}

Json::Value
explanationToJson(const ML::Explanation & expl,
                  int nFeatures)
{
    Json::Value result;

    auto data = expl.explainRaw(nFeatures, true);

    for (unsigned i = 0;  i < data.size();  ++i) {
        auto feat = data[i].first;
        float score = data[i].second;

        string featName;
        if (feat == ML::MISSING_FEATURE) {
            featName = "<<BIAS>>";
        }
        else {
            featName = expl.fspace->print(feat);
        }

        result[i]["feature"] = featName;
        result[i]["score"]   = score;
    }

    return result;
}


/*****************************************************************************/
/* DENSE CLASSIFIER                                                          */
/*****************************************************************************/

void
DenseClassifier::
load(const std::string & filename,
     std::shared_ptr<ML::Dense_Feature_Space> fs)
{
    ML::Classifier classifier;
    classifier.load(filename);
    init(classifier.impl, fs);
}

void
DenseClassifier::
reconstitute(ML::DB::Store_Reader & store,
             std::shared_ptr<ML::Dense_Feature_Space> fs)
{
    ML::Classifier classifier;
    classifier.reconstitute(store);
    init(classifier.impl, fs);
}   

void
dumpFeatureSpace(const ML::Dense_Feature_Space & fs)
{
    vector<Feature> features = fs.dense_features();
    cerr << "Dense_Feature_Space with " << features.size() << " features" << endl;
    for (unsigned i = 0;  i < features.size();  ++i) {
        cerr << format("%3d %-40s %s", i, fs.print(features[i]).c_str(),
                       features[i].print().c_str())
             << endl;
    }
}
                 

void
DenseClassifier::
init(std::shared_ptr<ML::Classifier_Impl> classifier,
     std::shared_ptr<ML::Dense_Feature_Space> fs)
{
    classifier_ = classifier;
    input_fs_ = fs;
    classifier_fs_ = std::const_pointer_cast<ML::Dense_Feature_Space>(classifier_->feature_space<ML::Dense_Feature_Space>());

    //cerr << "input features" << endl;
    //dumpFeatureSpace(*input_fs_);
    //cerr << endl << "classifier features" << endl;
    //dumpFeatureSpace(*classifier_fs_);

    classifier_fs_->create_mapping(*fs, mapping_);
    
    //cerr << "mapping_.vars = " << mapping_.vars << endl;
    //cerr << "mapping_.num_vars_expected = " << mapping_.num_vars_expected_ << endl;

    opt_info_ = classifier_->optimize(classifier_fs_->dense_features());
}

void
DenseClassifier::
save(const std::string & filename) const
{
    ML::Classifier classifier(classifier_);
    classifier.save(filename);
}

void
DenseClassifier::
serialize(DB::Store_Writer & store) const
{
    ML::Classifier classifier(classifier_);
    classifier.serialize(store);
}

float
DenseClassifier::
score(const distribution<float> & features) const
{
    float mapper_output[mapping_.num_vars_expected_];

    classifier_fs_->encode(&features[0],
                           mapper_output,
                           *input_fs_,
                           mapping_);

    return classifier_->predict(1, mapper_output, opt_info_);
}

float
DenseClassifier::
scoreUnbiased(const distribution<float> & features,
              PipelineExecutionContext & context) const
{
    float mapper_output[mapping_.num_vars_expected_];

    classifier_fs_->encode(&features[0],
                           mapper_output,
                           *input_fs_,
                           mapping_);

    return classifier_->predict(1, mapper_output, opt_info_, &context);
}

ML::Label_Dist
DenseClassifier::
labelScores(const distribution<float> & features) const
{
    float mapper_output[mapping_.num_vars_expected_];

    classifier_fs_->encode(&features[0],
                           mapper_output,
                           *input_fs_,
                           mapping_);

    return classifier_->predict(mapper_output, opt_info_);
}

ML::Label_Dist
DenseClassifier::
labelScoresUnbiased(const distribution<float> & features,
                    PipelineExecutionContext & context) const
{
    float mapper_output[mapping_.num_vars_expected_];

    classifier_fs_->encode(&features[0],
                           mapper_output,
                           *input_fs_,
                           mapping_);

    return classifier_->predict(mapper_output, opt_info_, &context);
}

std::pair<ML::Explanation, std::shared_ptr<Mutable_Feature_Set> >
DenseClassifier::
explain(const distribution<float> & features,
        int label) const
{
    std::shared_ptr<Mutable_Feature_Set> fset
        = classifier_fs_->encode(features, *input_fs_, mapping_);

    return make_pair(classifier_->explain(*fset, label), fset);
}

std::pair<ML::Explanation, std::shared_ptr<Mutable_Feature_Set> >
DenseClassifier::
explainUnbiased(const distribution<float> & features,
                int label,
                PipelineExecutionContext & context) const
{
    std::shared_ptr<Mutable_Feature_Set> fset
        = classifier_fs_->encode(features, *input_fs_, mapping_);

    return make_pair(classifier_->explain(*fset, label, 1.0, &context), fset);
}

} // namespace MLDB
