// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* multilabel_training_data.cc
   Mathieu Marquis Bolduc, 23 mars 2017

*/


#include "multilabel_training_data.h"

namespace ML {

void
Multilabel_Training_Data::Multilabel_Feature_Set::
sort() 
{
    auto predictedPair = (*inner)[predictedIndex];
    inner->sort();
    initialize(predictedPair.first);
}

void
Multilabel_Training_Data::Multilabel_Feature_Set::
initialize(Feature predicted) 
{   
    auto tuple = inner->get_data(true);
    newScores.resize(std::get<4>(tuple));
    float* pnewScores = newScores.data();
    char* oldScores = (char*)std::get<1>(tuple);
    const int step = std::get<3>(tuple);
    const char* pFeature = (char*)std::get<0>(tuple);
    const int featureStep = std::get<2>(tuple);
    for (size_t i = 0; i < newScores.size(); ++i, ++pnewScores, oldScores+=step, pFeature+=featureStep) {
        *pnewScores = *((float*)oldScores);
        if (*((Feature*)pFeature) == predicted) {
            predictedIndex = i;
        }
    }
}

Feature_Set * 
Multilabel_Training_Data::Multilabel_Feature_Set::
make_copy() const 
{
    return new Multilabel_Training_Data::Multilabel_Feature_Set(*this);
}

void 
Multilabel_Training_Data::
changePredictedValue(std::function<float(int)> getValue) {

    int i = 0;
    for (auto& fs : data_) {
        float score = getValue(i);
        ExcAssert(std::dynamic_pointer_cast<Multilabel_Training_Data::Multilabel_Feature_Set>(fs));
        std::static_pointer_cast<Multilabel_Training_Data::Multilabel_Feature_Set>(fs)->overridePredicted(score);
        ++i;
    }
    dirty_ = true;
}

}