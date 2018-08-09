/* stump_predict.h                                                 -*- C++ -*-
   Jeremy Barnes, 20 February 2004
   Copyright (c) 2004 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Inline prediction methods for the stump.
*/

#pragma once

#include "stump.h"


namespace ML {

template<class FeatureExPtrIter>
void Stump::predict(Label_Dist & result,
                    FeatureExPtrIter first, FeatureExPtrIter last) const
{
    Split::Weights weights;
    split.apply(first, last, weights);
    action.apply(result, weights);
}

template<class FeatureExPtrIter>
float Stump::
predict(int label, FeatureExPtrIter first, FeatureExPtrIter last) const
{
    Split::Weights weights;
    split.apply(first, last, weights);
    return action.apply(label, weights);
}

} // namespace ML
