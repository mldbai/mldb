// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* dense_classifier_scorer.h                                       -*- C++ -*-
   Jeremy Barnes, 13 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   A scorer is a classifier + a feature generator.
*/

#pragma once

#include "scorer.h"
#include "dense_classifier.h"
#include "dense_feature_generator.h"
#include "separation_stats.h"
#include "data_partition.h"
#include "mldb/ml/jml/classifier_generator.h"
#include "mldb/ml/jml/probabilizer.h"
#include "mldb/jml/db/persistent_fwd.h"
#include <boost/regex.hpp>
#include <tuple>


namespace MLDB {


/*****************************************************************************/
/* DENSE CLASSIFIER SCORER                                                   */
/*****************************************************************************/

/** Scorer that runs a dense classifier under the hood. */

struct DenseClassifierScorer : virtual public Scorer {

    DenseClassifierScorer();

    virtual ~DenseClassifierScorer()
    {
    }

    /** Initialize from an already trained classifier. */
    void init(const DenseClassifier & classifier,
              std::shared_ptr<DenseFeatureGenerator> featureGenerator);

    /** Train the given classifier over the selected partition. */
    void train(const DataPartition & partition,
               std::shared_ptr<ML::Classifier_Generator> trainer,
               std::shared_ptr<DenseFeatureGenerator> featureGenerator,
               int randomSeed = 1,
               float equalizationFactor = 0.0,
               const boost::regex & excludeFeatures = boost::regex());

    /** Generate a dataset over the selected partition. */
    ML::Training_Data
    generateFeatures(const DataPartition & partition,
                     std::shared_ptr<DenseFeatureGenerator> featureGenerator) const;
    
    /** Callback used when dumping feature vectors to return the extra
        features and label.
    */
    typedef std::function<std::pair<std::string, std::string>
                          (const boost::any &)>
    GetFeaturesAndComment;

    /** Dump a feature vector header. */
    void dumpFeatureVectorHeader(std::ostream & stream,
                                 const std::string & extraFeatures = "");

    /** Dump a feature vector file over the selected partition. */
    void dumpFeatureVectors(std::ostream & stream,
                            const DataPartition & partition,
                            GetFeaturesAndComment gfac = nullptr,
                            bool multiThread = true);

    /** Explain the output over the selected partition. */
    ML::Explanation explain(const DataPartition & partition) const;

    /** Train a probabilizer over the selected partition (which should NOT
        be the training partition).

        The unbiasedPositiveRate is the true positive rate in the unsampled
        population.
    */
    void trainProbabilizer(const DataPartition & partition,
                           double unbiasedPositiveRate);

    virtual float scoreGeneric(const boost::any & args) const;

    virtual ML::Label_Dist labelScoresGeneric(const boost::any & args) const;

    virtual float probabilityGeneric(const boost::any & args) const;

    virtual bool isCallableFromMultipleThreads() const
    {
        return featureGenerator->isCallableFromMultipleThreads();
    }

    /** Explain the output of the given example. */
    virtual std::pair<ML::Explanation, std::shared_ptr<ML::Feature_Set> >
    explainGeneric(const boost::any & args, int label) const;

    /** Explain the output of the given example. */
    virtual FeatureExplanation
    explainFeaturesGeneric(const ML::Explanation & weights,
                           const boost::any & args) const;

    /** Serialize to disk. */
    virtual void serialize(ML::DB::Store_Writer & store) const;

    /** Reconstitute from disk. */
    virtual void reconstitute(ML::DB::Store_Reader & store);

    /** Save to disk. */
    void save(const std::string & filename) const;

    /** Load from disk. */
    void load(const std::string & filename);
    
    DenseClassifier classifier;
    ML::GLZ_Probabilizer probabilizer;
    std::shared_ptr<DenseFeatureGenerator> featureGenerator;

    // a priori conversion probability
    double truePositiveRate;
};


/*****************************************************************************/
/* DENSE CLASSIFIER SCORER TEMPLATE                                          */
/*****************************************************************************/

/** Scorer that runs a dense classifier with types nicely dressed up. */

template<typename... Args>
struct DenseClassifierScorerT
    : public DenseClassifierScorer,
      virtual public ScorerT<Args...> {

    DenseClassifierScorerT():
        DenseClassifierScorer()
    {
    }

    virtual ~DenseClassifierScorerT()
    {
    }

    virtual float scoreGeneric(const boost::any & args) const
    {
        return DenseClassifierScorer::scoreGeneric(args);
    }

    virtual float probabilityGeneric(const boost::any & args) const
    {
        return DenseClassifierScorer::probabilityGeneric(args);
    }

    using DenseClassifierScorer::probabilityGeneric;


    /** Initialize from an already trained classifier. */
    void init(const DenseClassifier & classifier,
              std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGenerator)
    {
        DenseClassifierScorer::init(classifier, featureGenerator);
        featureGeneratorCast = featureGenerator;
    }

    /** Train the given classifier over the selected partition. */
    void train(const DataPartition & partition,
               std::shared_ptr<ML::Classifier_Generator> trainer,
               std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGenerator,
               int randomSeed = 1,
               float equalizationFactor = 0.0,
               const boost::regex & excludeFeatures = boost::regex())
    {
        this->featureGenerator = this->featureGeneratorCast = featureGenerator;
        DenseClassifierScorer::train(partition, trainer, featureGenerator,
                                     randomSeed, equalizationFactor,
                                     excludeFeatures);
    }

    /** Generate feature vectors over the given partition. */
    ML::Training_Data
    generateFeatures(const DataPartition & partition,
                     std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGenerator) const
    {
        return DenseClassifierScorer::generateFeatures(partition, featureGenerator);
    }

    void loadClassifier(const std::string & filename,
                        std::shared_ptr<ML::Dense_Feature_Space> fs)
    {
        classifier.load(filename, fs);
        initFeatureGeneratorCast();
    }

    void initFeatureGeneratorCast()
    {
        featureGeneratorCast
            = std::dynamic_pointer_cast<DenseFeatureGeneratorT<Args...> >
            (featureGenerator);
        if (!featureGeneratorCast)
            throw MLDB::Exception("couldn't cast dense feature generator");
    }

    /** Reconstitute from disk. */
    virtual void reconstitute(ML::DB::Store_Reader & store)
    {
        DenseClassifierScorer::reconstitute(store);
        initFeatureGeneratorCast();
    }

    virtual float score(Args... args) const
    {
        auto seed = this->encodeStatic(std::forward<Args>(args)...);
        return scoreGeneric(seed);
    }

    virtual float probability(Args... args) const
    {
        auto seed = this->encodeStatic(std::forward<Args>(args)...);
        return probabilityGeneric(seed);
    }

    virtual std::pair<ML::Explanation, std::shared_ptr<ML::Feature_Set> >
    explainExample(Args... args, int label) const
    {
        auto seed = this->encodeStatic(std::forward<Args>(args)...);
        return explainGeneric(seed, label);
    }
    
    /** Explain how the given features were constructed. */
    FeatureExplanation explainFeatures(const ML::Explanation & explanation,
                                       Args... args) const
    {
        auto seed = this->encodeStatic(std::forward<Args>(args)...);
        return explainFeaturesGeneric(explanation, seed);
    }


    std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGeneratorCast;
};


} // namespace MLDB
