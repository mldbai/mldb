// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* onevsall.h                                          -*- C++ -*-
   Mathieu Marquis Bolduc, 8 March 2017
   Copyright (c) 2017 MLDB.ai  All rights reserved.
   $Source$

   onevsall classifier

   This classifier handles classification problems where each example has a set of labels 
   instead of a single one (for example, tagging content) by training an ensemble of binary sub-classifiers,
   one for each possible unique label.

   The binary sub-classifiers are probabilized because their score need to be always comparable.
*/


#include "onevsall.h"
#include "mldb/plugins/jml/jml/registry.h"
#include "mldb/types/db/persistent.h"
#include "classifier_persist_impl.h"
#include "mldb/ext/jsoncpp/json.h"


using namespace std;
using namespace MLDB::DB;


namespace MLDB {


/*****************************************************************************/
/* ONEVSALL_CLASSIFIER                                                       */
/*****************************************************************************/

OneVsAllClassifier::OneVsAllClassifier()
{
}

OneVsAllClassifier::
OneVsAllClassifier(const std::shared_ptr<const Feature_Space> & fs,
                const Feature & predicted)
    : Classifier_Impl(fs, predicted)
{
}

OneVsAllClassifier::
OneVsAllClassifier(const std::shared_ptr<const Feature_Space> & fs,
                const Feature & predicted,
                size_t label_count)
    : Classifier_Impl(fs, predicted, label_count)
{
}

OneVsAllClassifier::~OneVsAllClassifier()
{
}

distribution<float>
OneVsAllClassifier::
predict(const Feature_Set & features,
                         PredictionContext * context) const
{
    ExcAssert(label_count() == subClassifiers.size());
    distribution<float> result(label_count(), 0.0);

    for (unsigned i = 0;  i < subClassifiers.size();  ++i) {

        Label_Dist sub_result = subClassifiers[i]->predict(features, context);
        result[i] = probabilizers[i]->glz.apply(MLDB::Label_Dist(1,sub_result[1]))[0];
    }

    return result;
}

Explanation 
OneVsAllClassifier::
explain(const Feature_Set & feature_set,
        const MLDB::Label & label,
        double weight,
        PredictionContext * context) const
{
    if (label < 0 || label >= subClassifiers.size())
        throw Exception("OneVsAllClassifier explain : label not in model");

    return subClassifiers[(size_t)label]->explain(feature_set, MLDB::Label(1), weight, context);    
}

std::string OneVsAllClassifier::print() const
{
    return "one vs all classifier";
}

std::vector<MLDB::Feature> OneVsAllClassifier::all_features() const
{
    std::vector<MLDB::Feature> result;
    return result;
}

Output_Encoding
OneVsAllClassifier::
output_encoding() const
{
    return OE_PROB;
}

std::string OneVsAllClassifier::class_id() const
{
    return "ONEVSALL_CLASSIFIER";
}

namespace {

static const std::string ONEVSALL_CLASSIFIER_MAGIC = "ONEVSALL_CLASSIFIER";
static const compact_size_t ONEVSALL_CLASSIFIER_VERSION = 0;

} // file scope

void OneVsAllClassifier::
serialize(DB::Store_Writer & store) const
{
    store << ONEVSALL_CLASSIFIER_MAGIC << ONEVSALL_CLASSIFIER_VERSION
          << compact_size_t(label_count());
    store << compact_size_t(subClassifiers.size());
    for (auto& c : subClassifiers) {
        c->poly_serialize(store, false /* write_fs */);
    }
    for (auto& p : probabilizers) {
        p->serialize(store);
    }

    feature_space()->serialize(store, predicted());
}

void OneVsAllClassifier::
reconstitute(DB::Store_Reader & store,
             const std::shared_ptr<const Feature_Space> & feature_space)
{
    string magic;
    compact_size_t version;
    store >> magic >> version;
    if (magic != ONEVSALL_CLASSIFIER_MAGIC)
        throw Exception("Attempt to reconstitute \"" + magic
                        + "\" with OneVsAllClassifier reconstitutor");
    if (version > ONEVSALL_CLASSIFIER_VERSION)
        throw Exception(format("Attemp to reconstitute OneVsAllClassifier "
                               "version %zd, only <= %zd supported",
                               version.size_,
                               ONEVSALL_CLASSIFIER_VERSION.size_));
    
    compact_size_t label_count_(store);
    compact_size_t numSub = 0;
    store >> numSub;

    for (size_t i = 0; i < numSub; ++i)
        subClassifiers.push_back(Classifier_Impl::poly_reconstitute(store, feature_space));
    for (size_t i = 0; i < numSub; ++i) {
        probabilizers.push_back(std::make_shared<ProbabilizerModel>());
        probabilizers.back()->reconstitute(store);
    }

    predicted_ = Feature(0, 0, 0);
    feature_space->reconstitute(store, predicted_);
    
    Classifier_Impl::init(feature_space, predicted_);

}

OneVsAllClassifier * OneVsAllClassifier::make_copy() const
{
    return new OneVsAllClassifier(*this);
}

/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

namespace {

Register_Factory<Classifier_Impl, OneVsAllClassifier>
NULL_REGISTER("ONEVSALL_CLASSIFIER");

} // file scope

} // namespace MLDB