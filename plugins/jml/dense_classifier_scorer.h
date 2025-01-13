/* dense_classifier_scorer.h                                       -*- C++ -*-
   Jeremy Barnes, 13 May 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   A scorer is a classifier + a feature generator.
*/

#pragma once

#include "scorer.h"
#include "dense_classifier.h"
#include "dense_feature_generator.h"
#include "separation_stats.h"
#include "data_partition.h"
#include "mldb/plugins/jml/jml/classifier_generator.h"
#include "mldb/plugins/jml/jml/probabilizer.h"
#include "mldb/types/db/persistent_fwd.h"
#include <regex>
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
               std::shared_ptr<MLDB::Classifier_Generator> trainer,
               std::shared_ptr<DenseFeatureGenerator> featureGenerator,
               int randomSeed = 1,
               float equalizationFactor = 0.0,
               const std::regex & excludeFeatures = std::regex());

    /** Generate a dataset over the selected partition. */
    MLDB::Training_Data
    generateFeatures(const DataPartition & partition,
                     std::shared_ptr<DenseFeatureGenerator> featureGenerator) const;
    
    /** Callback used when dumping feature vectors to return the extra
        features and label.
    */
    typedef std::function<std::pair<std::string, std::string>
                          (const std::any &)>
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
    MLDB::Explanation explain(const DataPartition & partition) const;

    /** Train a probabilizer over the selected partition (which should NOT
        be the training partition).

        The unbiasedPositiveRate is the true positive rate in the unsampled
        population.
    */
    void trainProbabilizer(const DataPartition & partition,
                           double unbiasedPositiveRate);

    virtual float scoreGeneric(const std::any & args) const;

    virtual MLDB::Label_Dist labelScoresGeneric(const std::any & args) const;

    virtual float probabilityGeneric(const std::any & args) const;

    virtual bool isCallableFromMultipleThreads() const
    {
        return featureGenerator->isCallableFromMultipleThreads();
    }

    /** Explain the output of the given example. */
    virtual std::pair<MLDB::Explanation, std::shared_ptr<MLDB::Feature_Set> >
    explainGeneric(const std::any & args, int label) const;

    /** Explain the output of the given example. */
    virtual FeatureExplanation
    explainFeaturesGeneric(const MLDB::Explanation & weights,
                           const std::any & args) const;

    /** Serialize to disk. */
    virtual void serialize(MLDB::DB::Store_Writer & store) const;

    /** Reconstitute from disk. */
    virtual void reconstitute(MLDB::DB::Store_Reader & store);

    /** Save to disk. */
    void save(const std::string & filename) const;

    /** Load from disk. */
    void load(const std::string & filename);
    
    DenseClassifier classifier;
    MLDB::GLZ_Probabilizer probabilizer;
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

    virtual float scoreGeneric(const std::any & args) const
    {
        return DenseClassifierScorer::scoreGeneric(args);
    }

    virtual float probabilityGeneric(const std::any & args) const
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
               std::shared_ptr<MLDB::Classifier_Generator> trainer,
               std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGenerator,
               int randomSeed = 1,
               float equalizationFactor = 0.0,
               const std::regex & excludeFeatures = std::regex())
    {
        this->featureGenerator = this->featureGeneratorCast = featureGenerator;
        DenseClassifierScorer::train(partition, trainer, featureGenerator,
                                     randomSeed, equalizationFactor,
                                     excludeFeatures);
    }

    /** Generate feature vectors over the given partition. */
    MLDB::Training_Data
    generateFeatures(const DataPartition & partition,
                     std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGenerator) const
    {
        return DenseClassifierScorer::generateFeatures(partition, featureGenerator);
    }

    void loadClassifier(const std::string & filename,
                        std::shared_ptr<MLDB::Dense_Feature_Space> fs)
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
    virtual void reconstitute(MLDB::DB::Store_Reader & store)
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

    virtual std::pair<MLDB::Explanation, std::shared_ptr<MLDB::Feature_Set> >
    explainExample(Args... args, int label) const
    {
        auto seed = this->encodeStatic(std::forward<Args>(args)...);
        return explainGeneric(seed, label);
    }
    
    /** Explain how the given features were constructed. */
    FeatureExplanation explainFeatures(const MLDB::Explanation & explanation,
                                       Args... args) const
    {
        auto seed = this->encodeStatic(std::forward<Args>(args)...);
        return explainFeaturesGeneric(explanation, seed);
    }


    std::shared_ptr<DenseFeatureGeneratorT<Args...> > featureGeneratorCast;
};


} // namespace MLDB
