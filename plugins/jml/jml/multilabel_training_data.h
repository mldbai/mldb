/* multilabel_training_data.h                                     -*- C++ -*-
   Mathieu Marquis Bolduc, 23 mars 2017

   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
   Training set, to hold a list of features from a sentence/paragraph.
*/

#pragma once

#include "mldb/plugins/jml/jml/training_data.h"
#include <functional>

namespace MLDB {

/*****************************************************************************/
/* MULTILABEL_TRAINING_DATA                                                   */
/*****************************************************************************/

//training data that overrides the values of the features

class Multilabel_Training_Data : public Training_Data
{
private: 
    class Multilabel_Feature_Set : public Feature_Set {
    public:

        Multilabel_Feature_Set(const std::shared_ptr<MLDB::Feature_Set> inner_, Feature predicted) : inner(inner_) {
            initialize(predicted);
        }

        virtual size_t size() const { return newScores.size(); }

        virtual std::tuple<const Feature *, const float *, int, int, size_t>
        get_data(bool need_sorted) const {
            ExcAssert(need_sorted);
            auto tuple = inner->get_data(true);
            ExcAssert(std::get<4>(tuple) == newScores.size());
            std::get<1>(tuple) = newScores.data();
            std::get<3>(tuple) = sizeof(float);

            return tuple;
        }

        void overridePredicted(float newScore) {
            newScores[predictedIndex] = newScore;
        }

        void initialize(Feature predicted);
        virtual void sort();
        virtual Feature_Set * make_copy() const;

    private:
        const std::shared_ptr<MLDB::Feature_Set> inner;
        std::vector<float> newScores;
        int predictedIndex;
    };
public:
    Multilabel_Training_Data(const Training_Data & training_data, 
                             MLDB::Feature overrideFeature, std::shared_ptr<const Feature_Space> fs);

    void changePredictedValue(std::function<float(int)> getValue);

private:

    MLDB::Feature overrideFeature;
    const Training_Data & inner;
};

} // ML
