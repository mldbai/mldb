/* fasttext_classifier.cc                                                -*- C++ -*-
   Mathieu Marquis Bolduc, 27 fevrier 2017
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "fasttext_classifier.h"
#include "classifier_persist_impl.h"
#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <functional>
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/ml/jml/registry.h"

#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/smart_ptr_utils.h"

#include "mldb/ext/fasttext/src/fasttext.h"
#include "mldb/ext/fasttext/src/args.h"
#include "mldb/ext/fasttext/src/model.h"


using namespace std;
using namespace ML::DB;

namespace ML {


/*****************************************************************************/
/* DECISION_TREE                                                             */
/*****************************************************************************/

FastTest_Classifier::FastTest_Classifier()
{

}

FastTest_Classifier::
FastTest_Classifier(DB::Store_Reader & store,
              const std::shared_ptr<const Feature_Space> & fs)    
{
    throw Exception("FastTest_Classifier constructor(reconst): not implemented");
}
    
FastTest_Classifier::
FastTest_Classifier(std::shared_ptr<const Feature_Space> feature_space,
              const Feature & predicted)   
 : Classifier_Impl(feature_space, predicted)
{
    ExcAssert(Classifier_Impl::feature_space());
}
    
FastTest_Classifier::
~FastTest_Classifier()
{
}
    
void
FastTest_Classifier::
swap(FastTest_Classifier & other)
{
    Classifier_Impl::swap(other);

    std::swap(encoding, other.encoding);  ///< How the outputs are represented
    std::swap(fastText_, other.fastText_);
    std::swap(features, other.features);
    std::swap(featureMap, other.featureMap);    
}

float
FastTest_Classifier::
predict(int label, const Feature_Set & infeatures,
        PredictionContext * context) const
{
    Label_Dist results = predict(infeatures, context);
    return results[label];
}

Label_Dist
FastTest_Classifier::
predict(const Feature_Set & infeatures,
        PredictionContext * context) const
{
    ExcAssert(fastText_);
    ExcAssert(fastText_->model_);
    Label_Dist results;
    results.resize(label_count());   

    std::vector<int32_t> words;
  
    for (const auto& feature : infeatures) {
        auto it = featureMap.find(feature.first);
        if (it != featureMap.end()) {
            size_t f = (*it);
            for (int i = 0; i < feature.second; ++i)
                words.push_back(f);
        }
    }

    if (!words.empty()) {
        fasttext::Vector hidden(fastText_->args_->dim);
        fasttext::Vector output(label_count());
        std::vector<std::pair<fasttext::real,int32_t>> modelPredictions;
        fastText_->model_->predict(words, label_count(), modelPredictions, hidden, output);
        for (auto it = modelPredictions.cbegin(); it != modelPredictions.cend(); it++) {
            results[it->second] = it->first;
        }
    }

    return results;
}

Explanation 
FastTest_Classifier::
explain(const Feature_Set & feature_set,
        const ML::Label & label,
        double weight,
        PredictionContext * context) const
{
    if (label < 0 || label > fastText_->output_->n_)
        throw Exception("FastTest_Classifier explain : label not in model");

    Explanation result;
    auto dims = fastText_->args_->dim;

    auto getEl = [] (const fasttext::Matrix& mat, size_t i, size_t j) -> float {
        return mat.data_[i * mat.n_ + j];
    };

    fasttext::Vector outputVector(dims);
    outputVector.zero();
    outputVector.addRow(*(fastText_->output_), label);

    for (const auto&f : feature_set) {
        auto feature = f.first;
        auto it = featureMap.find(feature);
        if (it != featureMap.end()) {
            size_t f = (*it);
            float sum = 0.0f;
            for (int dim = 0; dim < fastText_->args_->dim; ++dim) {
                sum += getEl(*fastText_->input_, f, dim)*outputVector[dim];
        }

        result.feature_weights[feature] = sum;
        }
    }

    result.bias = 0.f;

    return result;
}

bool
FastTest_Classifier::
optimization_supported() const
{
    return false;
}

bool
FastTest_Classifier::
predict_is_optimized() const
{
    return false;
}

bool
FastTest_Classifier::
optimize_impl(Optimization_Info & info)
{
    return false;
}

std::string
FastTest_Classifier::
print() const
{
    string result = "Fast Text Summary: ";
    result += std::to_string(fastText_->args_->dim) + " dims ";
    return result;
}

std::string
FastTest_Classifier::
summary() const
{
    return print();
}


std::vector<ML::Feature>
FastTest_Classifier::
all_features() const
{
   return features;
}

Output_Encoding
FastTest_Classifier::
output_encoding() const
{
    throw Exception("FastTest_Classifier output_encoding: not implemented");
    return encoding;
}

void
FastTest_Classifier::
serialize(DB::Store_Writer & store) const
{
    auto serializeFasttextMatrix = [&] (std::shared_ptr<fasttext::Matrix> matrix) {
        ExcAssert(matrix);
        store << matrix->m_;
        store << matrix->n_;
        fasttext::real* p = matrix->data_;
        for (int i = 0; i < matrix->m_*matrix->n_; ++i, p++) {
            store << *p;
            ExcAssert(!std::isnan(*p));
        }
    };

    store << class_id();
    store << compact_size_t(0);  // version
    store << compact_size_t(label_count());
    feature_space_->serialize(store, predicted_);

    size_t numFeatures = features.size();
    store << numFeatures;
    for (auto& f : features)
        serialize(store, f);

    //serialize input and output matrices
    ExcAssert(fastText_);
    serializeFasttextMatrix(fastText_->input_);
    serializeFasttextMatrix(fastText_->output_);

    size_t dims = fastText_->args_->dim;
    store << dims;

    store << encoding;
    store << compact_size_t(12345);  // end marker*/
}

void
FastTest_Classifier::
serialize(DB::Store_Writer & store, const Feature & feature) const
{
    store << DB::compact_size_t(feature.type())
          << DB::compact_size_t(feature.arg1())
          << DB::compact_size_t(feature.arg2());
}

void
FastTest_Classifier::
reconstitute(DB::Store_Reader & store, Feature & feature) const
{
    DB::compact_size_t type, arg1, arg2;
    store >> type >> arg1 >> arg2;
    feature = Feature(type, arg1, arg2);
}

void
FastTest_Classifier::
reconstitute(DB::Store_Reader & store,
             const std::shared_ptr<const Feature_Space> & feature_space)
{
    auto reconstituteFasttextMatrix = [&] () {
        int64_t m,n;

        store >> m;
        store >> n;
        auto matrix = std::make_shared<fasttext::Matrix>(m,n);
        fasttext::real* p = matrix->data_;
        for (int i = 0; i < m*n; ++i, p++) {
            store >> *p;
            ExcAssert(!isnan(*p));
        }

        return matrix;
    };

    string id;
    store >> id;

    if (id != class_id())
        throw Exception("FastTest_Classifier::reconstitute: read bad ID '"
                        + id + "'");

    compact_size_t version(store);
    
    switch (version) {
    case 0: {    
        compact_size_t label_count(store);
        feature_space->reconstitute(store, predicted_);
        Classifier_Impl::init(feature_space, predicted_);

        size_t numFeatures;
        store >> numFeatures;
        features.resize(numFeatures);
        for (auto& f : features)
            reconstitute(store, f);

        fastText_ = make_shared<fasttext::FastText>();

        fastText_->input_ = reconstituteFasttextMatrix();
        fastText_->output_ = reconstituteFasttextMatrix();
        fastText_->args_ = make_shared<fasttext::Args>();

        fastText_->model_ = std::make_shared<fasttext::Model>(fastText_->input_, fastText_->output_, fastText_->args_, 0);

        size_t dims = 0;
        store >> dims;
        fastText_->args_->dim = dims;

        store >> encoding;
        break;
    }
    default:
        throw Exception("FastTest_Classifier: Attempt to reconstitute model of "
                        "unknown version " + ostream_format(version.size_));
    }

    compact_size_t marker(store);
    if (marker != 12345)
        throw Exception("FastTest_Classifier::reconstitute: read bad marker at end");

    for (size_t f = 0; f < features.size(); ++f) {
        featureMap[features[f]] = f;
    }
}

void
FastTest_Classifier::
reconstitute(DB::Store_Reader & store)
{
    reconstitute(store, nullptr);
}
    
std::string
FastTest_Classifier::
class_id() const
{
    return "FASTTEXT";
}

FastTest_Classifier *
FastTest_Classifier::
make_copy() const
{
    //as far as I could tell make_copy is only used for bagging
    throw Exception("FastTest_Classifier: bagging is not yet supported");
    return new FastTest_Classifier(*this);
}

/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

Register_Factory<Classifier_Impl, FastTest_Classifier> REGISTER("FASTTEXT");

} // file scope

} // namespace ML

