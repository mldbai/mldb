/** dense_classifier_scorer.cc
    Jeremy Barnes, 13 May 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "dense_classifier_scorer.h"
#include "mldb/types/date.h"
#include "mldb/arch/spinlock.h"
#include "mldb/ml/jml/probabilizer.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/map_reduce.h"
#include <atomic>

using namespace std;
using namespace ML;


namespace MLDB {


/*****************************************************************************/
/* DENSE CLASSIFIER SCORER                                                   */
/*****************************************************************************/

DenseClassifierScorer::
DenseClassifierScorer():
    truePositiveRate(-1.)
{
}

void
DenseClassifierScorer::
init(const DenseClassifier & classifier,
     std::shared_ptr<DenseFeatureGenerator> featureGenerator)
{
    this->classifier = classifier;
    this->featureGenerator = featureGenerator;
}


void
DenseClassifierScorer::
train(const DataPartition & partition,
      std::shared_ptr<ML::Classifier_Generator> trainer,
      std::shared_ptr<DenseFeatureGenerator> featureGenerator,
      int randomSeed,
      float equalizationFactor,
      const boost::regex & excludeFeatures)
{
    std::shared_ptr<ML::Dense_Feature_Space>
        training_fs(new ML::Dense_Feature_Space());
    training_fs->add_feature("LABEL", ML::Feature_Info(BOOLEAN, false /* opt */,
                                                       true /* biased */));
    std::shared_ptr<ML::Dense_Feature_Space> runtime_fs
        = featureGenerator->featureSpace();
    training_fs->add(*runtime_fs);

    size_t nx = partition.exampleCount();

    Training_Data training_data(training_fs);
    distribution<float> labelWeights[2];
    labelWeights[0].resize(nx);
    labelWeights[1].resize(nx);

    std::vector<ML::Feature> allFeatures = training_fs->dense_features();

    std::vector<ML::Feature> trainingFeatures;

    for (unsigned i = 0;  i < allFeatures.size();  ++i) {
        string featureName = training_fs->print(allFeatures[i]);
        //cerr << "featureName = " << featureName << endl;
#if 0
        if (boost::regex_match(featureName, excludeFeatures)
            || featureName == "LABEL") {
            cerr << "excluding feature " << featureName << " from training"
                 << endl;
            continue;
        }
#endif
        trainingFeatures.push_back(allFeatures[i]);
    }

    // Generate training data for the classifier from the partition
    // examples.

    auto onExample = [&] (int exampleNum)
        {
            bool label;
            boost::any user;
            double weight;

            std::tie(label, user, weight) = partition.getExample(exampleNum);

            distribution<float> training_features;
            training_features.push_back(label);
            distribution<float> features
                = featureGenerator->featuresGeneric(user);
            
            training_features.insert(training_features.end(),
                                     features.begin(), features.end());
            
            std::shared_ptr<Mutable_Feature_Set> fset
                = training_fs->encode(training_features);
            fset->locked = true;

            labelWeights[0][exampleNum] = !label;
            labelWeights[1][exampleNum] = label;

            return fset;
        };

    auto reduce = [&] (int i, std::shared_ptr<Mutable_Feature_Set> fset)
    {
        training_data.add_example(fset);
    };

    if (featureGenerator->isCallableFromMultipleThreads())
        parallelMapInOrderReduceChunked(0, nx, onExample, reduce, 16 * 32);
    else
        for (int i=0; i < nx; ++i)
            reduce(i, onExample(i));

    Feature label(0);

    {
        Date before = Date::now();
        training_data.preindex(label, allFeatures);
        Date after = Date::now();

        cerr << "indexed " << nx
             << " feature vectors " << after.secondsSince(before)
             << " at " << nx / after.secondsSince(before)
             << " per second" << endl;
    }


    // Train the classifier
    this->featureGenerator = featureGenerator;

    trainer->init(training_fs, label);

    Thread_Context context;
    context.seed(randomSeed);
    
    double factorTrue  = pow(labelWeights[1].total(), -equalizationFactor);
    double factorFalse = pow(labelWeights[0].total(), -equalizationFactor);

    distribution<float> weights
        = factorTrue  * labelWeights[true]
        + factorFalse * labelWeights[false];
    weights.normalize();

    classifier.init(trainer->generate(context, training_data, weights,
                                      trainingFeatures),
                    runtime_fs);
}

void
DenseClassifierScorer::
dumpFeatureVectorHeader(std::ostream & stream,
                        const std::string & extraFeatures)
{
    cerr << "featureGenerator->featureSpace() = "
         << featureGenerator->featureSpace() << endl;
    
    stream << "LABEL:" << ML::Feature_Info(BOOLEAN, false, true)
           << " WEIGHT:" << ML::Feature_Info(REAL, false, true)
           << " " << extraFeatures
           << featureGenerator->featureSpace()->print()
           << endl;
}

void
DenseClassifierScorer::
dumpFeatureVectors(std::ostream & stream,
                   const DataPartition & partition,
                   GetFeaturesAndComment gfac,
                   bool multiThread)
{
    std::shared_ptr<ML::Dense_Feature_Space> runtime_fs
        = featureGenerator->featureSpace();

    auto getFeatures = [&] (int exampleNum)
        {
            bool label;
            boost::any user;
            double weight;

            std::tie(label, user, weight) = partition.getExample(exampleNum);

            std::string line = MLDB::format("%d %f ", label, weight);
            std::string extraFeatures, comment;
            
            if (gfac)
                std::tie(extraFeatures, comment)
                    = gfac(user);

            if (!extraFeatures.empty())
                line += extraFeatures + " ";

            distribution<float> features
                = featureGenerator->featuresGeneric(user);
            
            std::shared_ptr<Mutable_Feature_Set> fset
                = runtime_fs->encode(features);
            fset->locked = true;

            line += runtime_fs->print(*fset);

            if (comment != "")
                line += " # " + comment;
            line += "\n";

            return line;
        };

    auto writeLine = [&] (int exampleNum, string lineToWrite)
    {
        stream << lineToWrite;
    };
    
    int nbExamples = partition.exampleCount();

    if (multiThread && featureGenerator->isCallableFromMultipleThreads()) {
        int nbCores = 32;
        parallelMapInOrderReduceChunked(0, nbExamples, getFeatures,
                                         writeLine, 16 * nbCores);
    } else {
        for (int i=0; i<nbExamples; ++i)
            writeLine(i, getFeatures(i));
    }
}

ML::Training_Data
DenseClassifierScorer::
generateFeatures(const DataPartition & partition,
                 std::shared_ptr<DenseFeatureGenerator> featureGenerator) const
{
    std::shared_ptr<ML::Dense_Feature_Space>
        training_fs(new ML::Dense_Feature_Space());
    training_fs->add_feature("LABEL", ML::Feature_Info(BOOLEAN, false /* opt */,
                                                       true /* biased */));
    training_fs->add_feature("WEIGHT", ML::Feature_Info(REAL, false /* opt */,
                                                       true /* biased */));
    std::shared_ptr<ML::Dense_Feature_Space> runtime_fs
        = featureGenerator->featureSpace();
    training_fs->add(*runtime_fs);

    Training_Data training_data(training_fs);

    // Generate training data for the classifier from the partition
    // examples.

    std::mutex lock;

    auto onExample = [&] (bool label, const boost::any & user, double weight,
                          int exampleNum)
        {
            distribution<float> training_features;
            training_features.push_back(label);
            training_features.push_back(weight);
            distribution<float> features
                = featureGenerator->featuresGeneric(user);
            
            training_features.insert(training_features.end(),
                                     features.begin(), features.end());
            
            std::shared_ptr<Mutable_Feature_Set> fset
                = training_fs->encode(training_features);
            fset->locked = true;

            std::unique_lock<std::mutex> guard(lock);
            training_data.add_example(fset);
            return true;
        };

    partition.forEachExample(onExample,
                             featureGenerator->isCallableFromMultipleThreads());

    return training_data;
}

void
DenseClassifierScorer::
trainProbabilizer(const DataPartition & partition,
                  double unbiasedPositiveRate)
{
    size_t nd = partition.exampleCount();

    
    /* Convert to the correct data structures. */

    boost::multi_array<double, 2> outputs(boost::extents[2][nd]);  // value, bias
    distribution<double> correct(nd);

    Date before = Date::now();

    std::atomic<size_t> numTrue(0);

    auto onExample = [&] (bool label, const boost::any & user, double weight,
                          size_t exampleNum)
        {
            auto score = this->scoreGeneric(user);
            outputs[0][exampleNum] = score;
            outputs[1][exampleNum] = 1.0;
            correct[exampleNum] = label;
            numTrue += label;
        };

    partition.forEachExample(onExample,
                             featureGenerator->isCallableFromMultipleThreads());

#if 0
    filter_ostream out("prob-in.txt");

    for (unsigned i = 0;  i < nd;  ++i) {
        out << MLDB::format("%.15f %.16f %d\n",
                          outputs[0][i],
                          outputs[1][i],
                          correct[i]);
        
    }
#endif

    Date after = Date::now();

    cerr << "calculated " << partition.exampleCount()
         << " scores in " << after.secondsSince(before)
         << " at " << partition.exampleCount() / after.secondsSince(before)
         << " per second" << endl;


    double trueOneRate = unbiasedPositiveRate;
    double trueZeroRate = 1.0 - trueOneRate;

    double numExamples = nd;
    double sampleOneRate = numTrue / numExamples;
    double sampleZeroRate = 1.0 - sampleOneRate;

    cerr << "trueOneRate = " << trueOneRate
         << " trueZeroRate = " << trueZeroRate
         << " sampleOneRate = " << sampleOneRate
         << " sampleZeroRate = " << sampleZeroRate
         << endl;

#if 0
    double zeroWeight = trueZeroRate / sampleZeroRate;
    double oneWeight  = trueOneRate  / sampleOneRate;

    cerr << "zeroWeight = " << zeroWeight << " oneWeight = "
         << oneWeight << endl;

    // http://gking.harvard.edu/files/0s.pdf, section 4.2
    // Logistic Regression in Rare Events Data (Gary King and Langche Zeng)

    oneWeight = unbiasedPositiveRate / sampleOneRate;
    zeroWeight = (1 - unbiasedPositiveRate) / sampleZeroRate;

    cerr << "zeroWeight = " << zeroWeight << " oneWeight = "
         << oneWeight << endl;

    //zeroWeight = 1.0;
    //oneWeight = 1.0;

    auto weights = correct * oneWeight + (1.0 - correct) * zeroWeight;

    cerr << "total weight on one = " << (correct * oneWeight).total()
         << endl;
    cerr << "total weigth on zero = " << ((1.0 - correct) * zeroWeight).total()
         << endl;

    cerr << "total weight is " << weights.total() << " mean "
         << weights.mean() << endl;
#else
    distribution<double> weights(nd, 1.0);
#endif

    Ridge_Regressor regressor;
    distribution<double> probParams
        = ML::run_irls(correct, outputs, weights, LOGIT, regressor);
    
    cerr << "probParams = " << probParams << endl;

#if 1
    // http://gking.harvard.edu/files/0s.pdf, section 4.2
    // Logistic Regression in Rare Events Data (Gary King and Langche Zeng)

    double correction = -log((1 - trueOneRate) / trueOneRate
                             * sampleOneRate / (1 - sampleOneRate));

    cerr << "paramBefore = " << probParams[1] << endl;
    cerr << "correction = " << correction
         << endl;

    probParams[1] += correction;

    cerr << "paramAfter = " << probParams[1] << endl;
#endif


    probabilizer.link = LOGIT;
    probabilizer.params.resize(1);
    probabilizer.params[0] = probParams;
}

ML::Explanation
DenseClassifierScorer::
explain(const DataPartition & partition) const
{
    // 1.  Run the model on everything
    Explanation result(classifier.classifier_fs(), 1.0);

    auto onExample = [&] (size_t exampleNum)
        {
            bool label;
            boost::any user;
            double weight;

            std::tie(label, user, weight) = partition.getExample(exampleNum);

            auto explanation = explainGeneric(user, label).first;
            return make_pair(move(explanation), weight);
        };

    auto reduce = [&] (size_t exampleNumber, pair<Explanation, double> x)
    {
        result.add(x.first, x.second);
    };
    
    int exampleCount = partition.exampleCount();
    if (featureGenerator->isCallableFromMultipleThreads())
        parallelMapInOrderReduceChunked(0, exampleCount, onExample, reduce,
                                        16 * 32);
    else
        for (int i=0; i<exampleCount; ++i)
            reduce(i, onExample(i));

    return result;
}

float
DenseClassifierScorer::
scoreGeneric(const boost::any & args) const
{
    PipelineExecutionContext context;
    context.seed = args;
    return classifier.scoreUnbiased(featureGenerator->featuresGeneric(args),
                                    context);
}

float
DenseClassifierScorer::
probabilityGeneric(const boost::any & args) const
{
    return probabilizer.apply(ML::Label_Dist(1, scoreGeneric(args)))[0];
}

ML::Label_Dist
DenseClassifierScorer::
labelScoresGeneric(const boost::any & args) const
{
    return classifier.labelScores(featureGenerator->featuresGeneric(args));
}

std::pair<ML::Explanation, std::shared_ptr<ML::Feature_Set> >
DenseClassifierScorer::
explainGeneric(const boost::any & args, int label) const
{
    PipelineExecutionContext context;
    context.seed = args;
    return classifier.explainUnbiased(featureGenerator->featuresGeneric(args), label,
                                      context);
}

FeatureExplanation
DenseClassifierScorer::
explainFeaturesGeneric(const ML::Explanation & explanation,
                       const boost::any & args) const
{
    int arity = featureGenerator->featureSpace()->variable_count();

    // Map back onto the feature space for the feature generator
    const ML::Dense_Feature_Space::Mapping & mapping
        = classifier.mapping();

    // Get the feature weights
    distribution<float> weightsIn(arity);
    
    // And now break it down...
    for (auto & fw: explanation.feature_weights) {
        const Feature & f = fw.first;

        // Map the feature
        int mappedFeature = mapping.vars.at(f.type());

        //cerr << "f " << f << " "
        //     << explanation.fspace->print(f)
        //     << " " << mappedFeature << " "
        //     << featureGenerator->featureSpace()->print(Feature(mappedFeature))
        //     << endl;

        double weight = fw.second;

        if (mappedFeature < 0 || mappedFeature >= arity) {

            cerr << "f = " << f << " printed = " << featureGenerator->featureSpace()->print(f) << " arity = " << arity << endl;

            auto fs = featureGenerator->featureSpace();
            
            auto feats = fs->features();

            for (unsigned i = 0;  i < feats.size();  ++i) {
                cerr << i << " " << feats[i] << " " << fs->print(feats[i]) << endl;
            }

            abort();

            continue;
        }
        
        weightsIn.at(mappedFeature) = weight;
    }

    return featureGenerator->explainGeneric(weightsIn, args);
}

void
DenseClassifierScorer::
serialize(ML::DB::Store_Writer & store) const
{
    unsigned char version = 2;
    store << version;
    store << string("DenseClassifierScorer");
    store << truePositiveRate;
    DenseFeatureGenerator::polySerialize(*featureGenerator, store);
    classifier.serialize(store);
    probabilizer.serialize(store);
}

void
DenseClassifierScorer::
reconstitute(ML::DB::Store_Reader & store)
{
    unsigned char version;
    store >> version;
    if (version > 2)
        throw MLDB::Exception("unknown DenseClassifierScorer version "
                            "to reconstitute");
    string tag;
    store >> tag;
    if (tag != "DenseClassifierScorer")
        throw MLDB::Exception("unknown DenseClassifierScorer tag [" + tag + "]");
    if (version >= 2 )
        store >> truePositiveRate;
    else
        truePositiveRate = -1.;
    featureGenerator = DenseFeatureGenerator::polyReconstitute(store);
    std::shared_ptr<ML::Dense_Feature_Space> fs
        = featureGenerator->featureSpace();
    classifier.reconstitute(store, fs);

    if (version > 0)
        probabilizer.reconstitute(store);
}

void
DenseClassifierScorer::
save(const std::string & filename) const
{
    filter_ostream stream(filename);
    ML::DB::Store_Writer store(stream);
    serialize(store);
}

void
DenseClassifierScorer::
load(const std::string & filename)
{
    filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store);
}

} // namespace MLDB
