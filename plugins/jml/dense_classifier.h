/* dense_classifier.h                                              -*- C++ -*-
   Jeremy Barnes, 12 May 2012

   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Convenience dense classifier class.
*/

#pragma once

#include "mldb/plugins/jml/jml/classifier.h"
#include "mldb/plugins/jml/jml/dense_features.h"
#include "mldb/types/db/persistent_fwd.h"
#include "mldb/ext/jsoncpp/json.h"
#include "pipeline_execution_context.h"

namespace MLDB {

/** Return a JSON rendering of an explanation of a given feature set. */
Json::Value
explanationToJson(const MLDB::Explanation & expl,
                  const MLDB::Feature_Set & fset,
                  int nFeatures = -1);

/** Return a JSON rendering of an explanation. */
Json::Value
explanationToJson(const MLDB::Explanation & expl,
                  int nFeatures = -1);

/*****************************************************************************/
/* DENSE CLASSIFIER                                                          */
/*****************************************************************************/

/** A wrapped JML classifier that uses dense features. */

struct DenseClassifier {

    /** Load the classifier from the file to run on the given feature
        space. 
    */
    void load(const std::string & filename,
              std::shared_ptr<MLDB::Dense_Feature_Space> fs);

    /** Reconstitute from the given store to run on the given feature
        space. */
    void reconstitute(MLDB::DB::Store_Reader & store,
                      std::shared_ptr<MLDB::Dense_Feature_Space> fs);
    
    /** Initialize from the given classifier with the given input
        feature space. */
    void init(std::shared_ptr<MLDB::Classifier_Impl> classifier,
              std::shared_ptr<MLDB::Dense_Feature_Space> fs);
    
    /** Save the classifier to the given file. */
    void save(const std::string & filename) const;

    /** Serialize the classifier to the given file. */
    void serialize(MLDB::DB::Store_Writer & store) const;

    /** Calculate the score for a given feature set. */
    float score(const distribution<float> & features) const;

    /** Calculate the score for a given feature set. */
    float scoreUnbiased(const distribution<float> & features,
                        PipelineExecutionContext & context) const;

    /** Calculate the label scores for a given feature set. */
    MLDB::Label_Dist
    labelScores(const distribution<float> & features) const;

    /** Calculate the label scores for a given feature set. */
    MLDB::Label_Dist
    labelScoresUnbiased(const distribution<float> & features,
                        PipelineExecutionContext & context) const;

    std::shared_ptr<MLDB::Classifier_Impl> classifier() const
    {
        return classifier_;
    }

    std::shared_ptr<MLDB::Dense_Feature_Space> input_fs() const
    {
        return input_fs_;
    }

    std::shared_ptr<MLDB::Dense_Feature_Space> classifier_fs() const
    {
        return classifier_fs_;
    }

    const MLDB::Dense_Feature_Space::Mapping & mapping() const
    {
        return mapping_;
    }

    /** Explain which features contributed to what extent to the
        calculation of the final score.

        Returns the explanation object as well as the feature set used
        to make the explanation.
    */
    std::pair<MLDB::Explanation, std::shared_ptr<MLDB::Mutable_Feature_Set> >
    explain(const distribution<float> & features,
            int label) const;

    /** Explain which features contributed to what extent to the
        calculation of the final score.

        Returns the explanation object as well as the feature set used
        to make the explanation.
    */
    std::pair<MLDB::Explanation, std::shared_ptr<MLDB::Mutable_Feature_Set> >
    explainUnbiased(const distribution<float> & features,
                    int label,
                    PipelineExecutionContext & context) const;

    size_t variableCount() const
    {
        if (!input_fs_)
            return 0;
        return input_fs_->variable_count();
    }
    
private:
    std::shared_ptr<MLDB::Classifier_Impl> classifier_;
    std::shared_ptr<MLDB::Dense_Feature_Space> classifier_fs_;
    std::shared_ptr<MLDB::Dense_Feature_Space> input_fs_;
    MLDB::Dense_Feature_Space::Mapping mapping_;
    MLDB::Optimization_Info opt_info_;
};

} // namespace MLDB
