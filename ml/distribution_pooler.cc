// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* distribution_pooler.cc
   Francois Maillet, Oct 23, 2012
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
*/

#include "distribution_pooler.h"

using namespace std;
using namespace ML;

#include <math.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <limits>
#include <set>



namespace MLDB {

void DistributionPooler::add(std::shared_ptr<distribution<float>> d)
{
    if(!init) {
        for(int i=0; i<d->size(); i++)
            feature_vectors.push_back(distribution<float>());
        init = true;
    }
    for(int i=0; i<d->size(); i++)
        feature_vectors[i].push_back((*d)[i]);
}

distribution<float> DistributionPooler::pool()
{
    distribution<float> d;
    d.push_back(feature_vectors[0].size());
    for(int i=0; i<feature_vectors.size(); i++) {
        d.push_back(feature_vectors[i].min());
        d.push_back(feature_vectors[i].max());
        d.push_back(feature_vectors[i].mean());
        d.push_back(feature_vectors[i].std());
    }
    return d;
}


}
