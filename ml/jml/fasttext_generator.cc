/* fasttext_tree_generator.cc                                       -*- C++ -*-
   Mathieu Marquis Bolduc, 2 March 2017

   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Generator for fasttext classification
*/

#include "fasttext_generator.h"
#include "mldb/ml/jml/registry.h"
#include <boost/timer.hpp>
#include "training_index.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/parallel.h"
#include "mldb/ml/jml/feature_map.h"

#include "mldb/ext/fasttext/src/fasttext.h"
#include "mldb/ext/fasttext/src/args.h"
#include "mldb/ext/fasttext/src/model.h"

#include <random>
#include <mutex>


using namespace std;

namespace ML {

/*****************************************************************************/
/* FASTTEXT_GENERATOR                                                        */
/*****************************************************************************/

FastText_Generator::
FastText_Generator()
{
    defaults();
}

FastText_Generator::~FastText_Generator()
{
}

void
FastText_Generator::
configure(const Configuration & config, vector<string> & unparsedKeys)
{
    Classifier_Generator::configure(config, unparsedKeys);

    config.findAndRemove(epoch, "epoch", unparsedKeys);
    config.findAndRemove(dims, "dims", unparsedKeys);
    config.findAndRemove(verbose, "verbosity", unparsedKeys);
    config.findAndRemove(minCount, "minCount", unparsedKeys);
    config.findAndRemove(maxCount, "maxCount", unparsedKeys);
}

void
FastText_Generator::
defaults()
{
    Classifier_Generator::defaults();
    epoch = 5;
    dims = 100;
    verbose = 0;  
}

Config_Options
FastText_Generator::
options() const
{
    Config_Options result = Classifier_Generator::options();
    result
        .add("epoch", epoch, "1+",
             "Number of iterations over the data")
        .add("dims", dims, "1+",
             "Number of dimensions in the embedding")
        .add("verbosity", verbose, "0+",
             "Level of verbosity in standard output")
        .add("minCount", minCount, "0+",
             "")
        .add("maxCount", maxCount, "0+",
             "Level of verbosity in standard output");
    return result;
}

void
FastText_Generator::
init(std::shared_ptr<const Feature_Space> fs, Feature predicted)
{
    Classifier_Generator::init(fs, predicted);
    model = FastTest_Classifier(fs, predicted);
}

std::shared_ptr<Classifier_Impl>
FastText_Generator::
    generate(Thread_Context & context,
             const Training_Data & training_data,
             const distribution<float> & weights,
             const std::vector<Feature> & infeatures,
             int) const
{
    Feature predicted = model.predicted();
    int numLabels = model.feature_space()->info(predicted).value_count();

    bool regression_problem
        = model.feature_space()->info(predicted).type() == REAL;

    if (regression_problem)
        throw Exception("FastText classifier does not currently support regression mode");

    boost::timer timer;
    std::vector<int64_t> labelCount(numLabels);
    std::vector<int64_t> invalidCount;
    
    std::shared_ptr<fasttext::Args> args_ = make_shared<fasttext::Args>();
    std::shared_ptr<fasttext::Dictionary> dict_ = make_shared<fasttext::Dictionary>(args_);
    
    auto output = std::make_shared<FastTest_Classifier>(std::move(model));
    output->fastText_ = make_shared<fasttext::FastText>();
    fasttext::FastText& fastTextModel = *(output->fastText_);
    fastTextModel.args_ = args_;    

    //Build feature map
    std::set<Feature> allFeatures;
    for (size_t f = 0; f < infeatures.size(); ++f) {
        output->featureMap[infeatures[f]] = f;
        allFeatures.insert(infeatures[f]);
    }

    //////////////////////////////
    //const size_t minCount = 2;
    //const size_t maxNumFeature = 50000;
    const size_t inNumFeatures = infeatures.size();
    std::vector<size_t> featureCount(inNumFeatures);

    for (int i = 0; i < training_data.example_count(); ++i) {
        const Feature_Set & featureSet = training_data[i];

        auto it = featureSet.begin();
        auto itEnd = featureSet.end();

        while (it != itEnd) {
            std::pair<Feature, float> feature = *it;
            if (feature.first != predicted) {
                featureCount[output->featureMap[feature.first]]++;
            }            
            ++it;
        }
    }

    std::vector<std::pair<size_t, size_t>> sortedFeatures(inNumFeatures);
    for (size_t i = 0; i < featureCount.size(); ++i) {
        sortedFeatures[i] = std::make_pair( featureCount[i], i);
    }

    std::sort(sortedFeatures.begin(), sortedFeatures.end());

    /*cerr << "LESS FREQUENT FEATURES " << endl;
    for (int i = 0; i < 20; ++i){
        cerr << model.feature_space()->print(infeatures[sortedFeatures[i].second]) << " count " << sortedFeatures[i].first << endl;       
        
    }

    cerr << "MOST FREQUENT FEATURES " << endl;
    for (int i = 0; i < 20; ++i){
        cerr << model.feature_space()->print(infeatures[sortedFeatures[inNumFeatures-i-1].second]) << " count " << sortedFeatures[inNumFeatures-i-1].first << endl;        
    }*/

    //drop features with not enough count
    ExcAssert(sortedFeatures.size() == inNumFeatures);
    size_t droppedCount = 0;
    for (size_t i = 0; i < sortedFeatures.size(); ++i) {
        if (sortedFeatures[i].first < minCount) {
            allFeatures.erase(infeatures[sortedFeatures[i].second]);
            ++droppedCount;
        }
        else {
            break;
        }
    }

    cerr << inNumFeatures << " total input features" << endl;
    cerr << "Removed " << droppedCount << " features with a low count" << endl;

    //drop features with too much count

    size_t droppedCountLimit = 0;
    for (ssize_t i = 0; i < maxCount; ++i) {

            if (i >= sortedFeatures.size())
                break;

            allFeatures.erase(infeatures[sortedFeatures[inNumFeatures-i-1].second]);
            ++droppedCountLimit;
    }

    cerr << "Removed " << droppedCountLimit << " features with a high count" << endl;

    //rebuild feature map & arrray
    output->featureMap.clear();
    std::vector<Feature> features;
    size_t count = 0;
    for (const Feature& f : allFeatures) {        
        features.push_back(f);
        output->featureMap[f] = count;
        ++count;
    } 

    //////////////////////////////

    output->features = features;

    //0 : Initialize
    int64_t ntokens = 0;
    for (int i = 0; i < training_data.example_count(); ++i) {
        const Feature_Set & featureSet = training_data[i];

        auto it = featureSet.begin();
        auto itEnd = featureSet.end();

        while (it != itEnd) {
            std::pair<Feature, float> feature = *it;
            if (feature.first == predicted) {
                labelCount[feature.second]++;
            }
            else {
                ntokens += (int64_t)feature.second;
            }
            ++it;
        }
    }

    args_->model = fasttext::model_name::sup;
    args_->thread = training_data.example_count() < numCpus() ? 1 : numCpus();
    args_->bucket = 0;
    args_->dim = dims;
    args_->epoch = epoch;
    args_->verbose = verbose;


    std::shared_ptr<fasttext::Matrix> input_ 
        = std::make_shared<fasttext::Matrix>(features.size()+args_->bucket, args_->dim);
    input_->uniform(1.0 / args_->dim);
    std::shared_ptr<fasttext::Matrix> output_ = std::make_shared<fasttext::Matrix>(numLabels, args_->dim);   
    output_->zero();
    fastTextModel.input_ = input_;
    fastTextModel.output_ = output_;

    //Note that we skip Fasttext's dictionary because the strings are already abstracted in our features  

    std::atomic<int64_t> tokenCount(0);
    auto trainThread = [&] (size_t threadId) {

      int32_t start = threadId * training_data.example_count() / args_->thread;

      fasttext::Model model(input_, output_, args_, threadId);
      model.setTargetCounts(labelCount);
    
      int64_t localTokenCount = 0;
      std::vector<int32_t> line, labels;
      while (tokenCount < args_->epoch * ntokens) {

        //if we reach the end we wrap around
        if (start >= training_data.example_count())
            start = 0;

        fasttext::real progress = fasttext::real(tokenCount) / (args_->epoch * ntokens);
        fasttext::real lr = args_->lr * (1.0 - progress);

        const Feature_Set & lineFeatureSet = training_data[start];
        auto it = lineFeatureSet.begin();
        auto itEnd = lineFeatureSet.end();

        line.clear();
        labels.clear();

        while (it != itEnd) {
            std::pair<Feature, float> feature = *it;
            if (feature.first == predicted) {
                labels.push_back(feature.second); //value of the label feature is the label
            }
            else {
                auto featureit = output->featureMap.find(feature.first);
                if (featureit != output->featureMap.end()) {
                    size_t f = (*featureit);
                    for (int i = 0; i < feature.second; ++i)
                        line.push_back(f);        
                }
            }
            localTokenCount++;
            ++it;
        }       

        start++;        
        if (labels.size() != 0 && line.size() > 0) {

            //This for future multicategorical support
            std::uniform_int_distribution<> uniform(0, labels.size() - 1);
            int32_t i = uniform(model.rng); 
            model.update(line, labels[i], lr);
        }
     
        if (localTokenCount > args_->lrUpdateRate) {
            tokenCount += localTokenCount;
            localTokenCount = 0;
            if (threadId == 0 && args_->verbose > 1) {
                fastTextModel.printInfo(progress, model.getLoss());
            }
        }
      }
      if (threadId == 0 && args_->verbose > 0) {
        fastTextModel.printInfo(1.0, model.getLoss());
        std::cout << std::endl;
      }
    };

    parallelMap(0, args_->thread, trainThread);

    //Find the K most important features

    auto rowNormSq = [&] (size_t rowNum) {
        fasttext::Vector v(args_->dim);
        v.zero();
        v.addRow(*fastTextModel.input_, rowNum);
        float sqNorm = fastTextModel.input_->dotRow(v, rowNum);
        return sqNorm;
    };

  /*  std::vector< std::pair< float, size_t > > sortedFeatures;
    const size_t numFeatures = features.size();
    sortedFeatures.resize(numFeatures);
    for (size_t i = 0; i < numFeatures; ++i) {
        sortedFeatures[i] = std::make_pair( rowNormSq(i), i);
    }

    std::sort(sortedFeatures.begin(), sortedFeatures.end());

    cerr << "WORST FEATURES " << endl;
    for (int i = 0; i < 20; ++i){
        cerr << model.feature_space()->print(features[sortedFeatures[i].second]) << " sq norm " << sortedFeatures[i].first << endl;
        fasttext::Vector v(args_->dim);
        v.zero();
        v.addRow(*fastTextModel.input_, sortedFeatures[i].second);
        for (int j = 0; j < args_->dim; ++j) {
            cerr << v[j] << ",";
        }
        cerr << endl;
        
    }

    cerr << "BEST FEATURES " << endl;
    for (int i = 0; i < 20; ++i){
        cerr << model.feature_space()->print(features[sortedFeatures[numFeatures-i-1].second]) << " sq norm " << sortedFeatures[numFeatures-i-1].first << endl;
        fasttext::Vector v(args_->dim);
        v.zero();
        v.addRow(*fastTextModel.input_, sortedFeatures[numFeatures-i-1].second);
        for (int j = 0; j < args_->dim; ++j) {
            cerr << v[j] << ",";
        }
        cerr << endl;
    }*/    

    //for prediction
    fastTextModel.model_ = std::make_shared<fasttext::Model>(fastTextModel.input_, fastTextModel.output_, fastTextModel.args_, 0);
   
    return output;
}

/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

Register_Factory<Classifier_Generator, FastText_Generator>
    FASTTEXT_REGISTER("fasttext");

} // file scope

} // namespace ML
