/* fasttext_classifier.h                                                -*- C++ -*-
   Mathieu Marquis Bolduc, 27 fevrier 2017
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/ml/jml/classifier.h"
#include "feature_set.h"
#include "mldb/ml/jml/feature_map.h"

namespace fasttext {
    class FastText;
}

namespace ML {


class Training_Data;


/*****************************************************************************/
/* FastTest_Classifier                                                       */
/*****************************************************************************/

class FastTest_Classifier : public Classifier_Impl {
public:
    /** Default construct.  Must be initialised before use. */
    FastTest_Classifier();

    /** Construct it by reconstituting it from a store. */
    FastTest_Classifier(DB::Store_Reader & store,
                  const std::shared_ptr<const Feature_Space> & fs);
    
    /** Construct not filled in yet. */
    FastTest_Classifier(std::shared_ptr<const Feature_Space> feature_space,
                  const Feature & predicted);
    
    virtual ~FastTest_Classifier();
    
    void swap(FastTest_Classifier & other);

    using Classifier_Impl::predict;

    virtual float predict(int label, const Feature_Set & features,
                          PredictionContext * context = 0) const;

    virtual distribution<float>
    predict(const Feature_Set & features,
            PredictionContext * context = 0) const;

    virtual Explanation explain(const Feature_Set & feature_set,
                                const ML::Label & label,
                                double weight = 1.0,
                                PredictionContext * context = 0) const;

    /** Is optimization supported by the classifier? */
    virtual bool optimization_supported() const;

    /** Is predict optimized?  Default returns false; those classifiers which
        a) support optimized predict and b) have had optimize_predict() called
        will override to return true in this case.
    */
    virtual bool predict_is_optimized() const;
    /** Function to override to perform the optimization.  Default will
        simply modify the optimization info to indicate that optimization
        had failed.
    */
    virtual bool
    optimize_impl(Optimization_Info & info);
    

    virtual std::string print() const;

    virtual std::string summary() const;

    virtual std::vector<Feature> all_features() const;

    virtual Output_Encoding output_encoding() const;

    virtual void serialize(DB::Store_Writer & store) const;
    virtual void reconstitute(DB::Store_Reader & store,
                              const std::shared_ptr<const Feature_Space>
                                  & feature_space);
    virtual void reconstitute(DB::Store_Reader & store);

    virtual void serialize(DB::Store_Writer & store,
                           const Feature & feature) const;
    virtual void reconstitute(DB::Store_Reader & store,
                              Feature & feature) const;
    
    virtual std::string class_id() const;

    virtual FastTest_Classifier * make_copy() const;

    Output_Encoding encoding;  ///< How the outputs are represented
    std::shared_ptr<fasttext::FastText> fastText_;
    std::vector<Feature> features;

    //The feature map needs to be mutable else the map doesnt work in predict
    //I suspect either some compiler const optimization 
    //*or* a bug in the judy array const overload but I haven't found the root cause yet.
    mutable Feature_Map<size_t> featureMap;
};


} // namespace ML

