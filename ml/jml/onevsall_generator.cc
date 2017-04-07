// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* onevsall_generator.cc                                          -*- C++ -*-
   Mathieu Marquis Bolduc, 8 March 2017
   Copyright (c) 2017 MLDB.ai  All rights reserved.
   $Source$

   Generator for onevsall classifiers
*/

#include "onevsall_generator.h"
#include "mldb/ml/jml/registry.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/utils/log_fwd.h"
#include "mldb/base/parallel.h"
#include "mldb/ml/jml/info_override_feature_space.h"
#include "mldb/ml/jml/multilabel_training_data.h"

#include "training_index.h"

using namespace std;


namespace ML {

/*****************************************************************************/
/* ONEVSALL_GENERATOR                                                        */
/*****************************************************************************/

OneVsAll_Classifier_Generator::
OneVsAll_Classifier_Generator()
{
    defaults();
}

OneVsAll_Classifier_Generator::
OneVsAll_Classifier_Generator(std::shared_ptr<Classifier_Generator> learner)
{
    defaults();
    weak_learner = learner;
}

OneVsAll_Classifier_Generator::~OneVsAll_Classifier_Generator()
{
}

void
OneVsAll_Classifier_Generator::
configure(const Configuration & config, vector<string> & unparsedKeys)
{
    Classifier_Generator::configure(config, unparsedKeys);

    config.find(verbosity, "verbosity");

    weak_learner = get_trainer("weak_learner", config);
}

void
OneVsAll_Classifier_Generator::
defaults()
{
    Classifier_Generator::defaults();
    weak_learner.reset();
}

Config_Options
OneVsAll_Classifier_Generator::
options() const
{
    Config_Options result = Classifier_Generator::options();
    result       
        .subconfig("weak_leaner", weak_learner,
                   "Binary classifier for each label value");
    
    return result;
}

void
OneVsAll_Classifier_Generator::
init(std::shared_ptr<const Feature_Space> fs, Feature predicted)
{
    Classifier_Generator::init(fs, predicted);
    model = OneVsAllClassifier(fs, predicted);    
}

std::shared_ptr<Classifier_Impl>
OneVsAll_Classifier_Generator::
generate(Thread_Context & context,
         const Training_Data & training_data,
         const distribution<float> & weights,
         const std::vector<Feature> & features,
         int) const
{
    auto logger = MLDB::getMldbLog<OneVsAll_Classifier_Generator>();

    std::shared_ptr<OneVsAllClassifier> current = make_shared<OneVsAllClassifier>(model);

    ML::Mutable_Feature_Info labelInfo = ML::Mutable_Feature_Info(ML::BOOLEAN);

    int labelValue = 0; 

    std::shared_ptr<Info_Override_Feature_Space> fs2 = std::make_shared<Info_Override_Feature_Space>(feature_space);
    fs2->set_info(predicted, labelInfo);

    std::shared_ptr<Multilabel_Training_Data> mutable_trainingData 
        = make_shared<Multilabel_Training_Data>(training_data, predicted, fs2);
    ExcAssert(mutable_trainingData->example_count() == training_data.example_count());

    if (verbosity > 0)
        cerr << numUniqueLabels << " unique label" << endl;

    do {        

        if (verbosity > 0)
            cerr << "label " << labelValue << " out of " << numUniqueLabels << endl;

        //Fix the feature space and training_data for binary classification
        weak_learner->init(fs2, predicted);

        auto getPredictedScore = [&] (int exampleNumber) {
           float currentLabelValue = training_data[exampleNumber][predicted];
           int combinaisonIndex = (int)currentLabelValue;
           ExcAssert(combinaisonIndex < multiLabelList.size());

           const auto& combinaison = multiLabelList[combinaisonIndex];
           return std::count(combinaison.begin(), combinaison.end(), labelValue) > 0 ? 1.0f : 0.0f;
        };

        mutable_trainingData->changePredictedValue(getPredictedScore);

        auto subClassifier = weak_learner->generate(context, *mutable_trainingData, weights, features);

        //Probabilize it
        std::mutex fvsLock;
        std::vector<std::tuple<float, float, float> > fvs;
        fvs.resize(mutable_trainingData->example_count());

        std::function<void (size_t)> doWork = [&] (size_t i) {
            auto fs = mutable_trainingData->get(i);
            float score = subClassifier->predict(1, *fs);
            float weight = weights[i];
            fvs[i] = std::make_tuple(score, (*fs)[predicted], weight);
        };

        parallelMap(0, mutable_trainingData->example_count(), doWork);

        auto probabilizer = make_shared<ProbabilizerModel>(logger);
        probabilizer->train(fvs, LOGIT);
        current->push(subClassifier, probabilizer);

        ++labelValue;
    } while (labelValue < numUniqueLabels);

    return current;
}

/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

Register_Factory<Classifier_Generator, OneVsAll_Classifier_Generator>
    NULL_CLASSIFIER_REGISTER("onevsall");

} // file scope

} // namespace ML
