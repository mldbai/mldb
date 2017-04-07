// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* onevsall.h                                          -*- C++ -*-
   Mathieu Marquis Bolduc, 8 March 2017
   Copyright (c) 2017 MLDB.ai  All rights reserved.
   $Source$

   onevsall classifier
*/

#pragma once


#include "mldb/ml/jml/classifier.h"
#include "feature_set.h"
#include "mldb/ml/jml/feature_map.h"
#include "mldb/ml/jml/probabilizer.h"


namespace ML {


/*****************************************************************************/
/* ONEVSALL_CLASSIFIER                                                       */
/*****************************************************************************/

/** Classifier that trains a binary classifier for each possible label */

class OneVsAllClassifier : public Classifier_Impl {
public:
    OneVsAllClassifier();
    OneVsAllClassifier(const std::shared_ptr<const Feature_Space> & fs,
                    const Feature & predicted);
    OneVsAllClassifier(const std::shared_ptr<const Feature_Space> & fs,
                    const Feature & predicted, size_t label_count);

    virtual ~OneVsAllClassifier();

    using Classifier_Impl::predict;

    /** Predict the score for all classes. */
    virtual distribution<float>
    predict(const Feature_Set & features,
            PredictionContext * context = 0) const;

    virtual std::string print() const;

    virtual std::vector<Feature> all_features() const;

    virtual Output_Encoding output_encoding() const;

    virtual std::string class_id() const;

    /** Serialization and reconstitution. */
    virtual void serialize(DB::Store_Writer & store) const;

    virtual void reconstitute(DB::Store_Reader & store,
                              const std::shared_ptr<const Feature_Space>
                                  & feature_space);

    /** Allow polymorphic copying. */
    virtual OneVsAllClassifier * make_copy() const;


    void push(std::shared_ptr<Classifier_Impl> subClassifier,std::shared_ptr<ProbabilizerModel> probabilizer) {
        subClassifiers.push_back(subClassifier);
        probabilizers.push_back(probabilizer);
    }

    std::vector<std::shared_ptr<Classifier_Impl>> subClassifiers;
    std::vector<std::shared_ptr<ProbabilizerModel>> probabilizers;
};

} // namespace ML

