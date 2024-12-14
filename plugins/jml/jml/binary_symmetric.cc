// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* binary_symmetric.cc
   Jeremy Barnes, 17 March 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.
   $Source$

   Functions to deal with binary symmetric data.
*/

#include "binary_symmetric.h"
#include "training_data.h"
#include "training_index.h"
#include "mldb/plugins/jml/multi_array_utils.h"


using namespace std;


namespace MLDB {

bool
convert_bin_sym(MLDB::Matrix<float, 2> & weights, const Training_Data & data,
                const Feature & predicted, const vector<Feature> & features)
{
    /* If we have a binary symmetric problem, then we reduce the weights down
       to just one column, since the second column would be the same as the
       first one.
       
       The bin_sym flag is true when all of the label weights will have the
       same value.  This is only generally true in the binary classification
       case.  It will be false when:

       1.  We learn a stump which updates the weights where

    */
    int nl = data.label_count(predicted);

    bool bin_sym = false;

    int nx = weights.dim(0);

    //cerr << "convert_bin_sym" << endl;
    //cerr << "nl = " << nl << " nx = " << nx << endl;
    //cerr << "bin_sym: input: weights.dim(1) = "
    //     << weights.dim(1) << endl;

    if (nl == 2) {

        bin_sym = true;
        
        /* Look at all of these features.  If any don't have exactly_one true,
           then we are not binary symmetric. */
        for (unsigned i = 0;  i < features.size();  ++i) {
            if (!data.index().only_one(features[i])) {
                //cerr << "false due to feature "
                //     << data.feature_space()->print(features[i])
                //     << endl;
                bin_sym = false;
                break;
            }
        }
        
        if (!bin_sym && weights.dim(1) == 1) {
            /* Not bin sym for these features... expand them. */
            //cerr << "expanding" << endl;
            MLDB::Matrix<float, 2> new_weights(MLDB::extents[nx][2]);
            for (unsigned x = 0;  x < nx;  ++x)
                new_weights[x][0] = new_weights[x][1] = weights[x][0];
            std::swap(weights, new_weights);
        }

        else if (bin_sym && weights.dim(1) == 2) {
            for (unsigned x = 0;  x < nx;  ++x) {
                bin_sym = true;
                if (weights[x][0] != weights[x][1]) {
                    bin_sym = false;
                    //cerr << "false due to unequal weights " << endl;
                    break;
                }
            }
            
            /* If we are binary symmetric, then we can reduce our weights
               array. */
            if (bin_sym) {
                MLDB::Matrix<float, 2> new_weights(MLDB::extents[nx][1]);
                for (unsigned x = 0;  x < nx;  ++x)
                    new_weights[x][0] = weights[x][0];
                std::swap(weights, new_weights);
            }
        }
    }
    
    //cerr << "bin_sym: returned " << bin_sym << " weights.dim(1) = "
    //     << weights.dim(1) << endl;

    return bin_sym;
}

bool
is_bin_sym(const MLDB::Matrix<float, 2> & weights, const Training_Data & data,
           const Feature & predicted,
           const vector<Feature> & features)
{
    /* If we have a binary symmetric problem, then we reduce the weights down
       to just one column, since the second column would be the same as the
       first one. */
    int nl = data.label_count(predicted);

    bool bin_sym = false;
    
    int nx = weights.dim(0);

    //cerr << "is_bin_sym" << endl;
    //cerr << "nl = " << nl << " nx = " << nx << endl;
    
    if (nl == 2) {
        
        if (weights.dim(1) == 1) bin_sym = true;
        
        /* Look at all of these features.  If any don't have exactly_one true,
           then we are not binary symmetric. */
        for (unsigned i = 0;  i < features.size();  ++i) {
            if (!data.index().only_one(features[i])) {
                bin_sym = false;
                break;
            }
        }

        if (bin_sym && weights.dim(1) == 2) {
            for (unsigned x = 0;  x < nx;  ++x) {
                bin_sym = true;
                if (weights[x][0] != weights[x][1]) {
                    bin_sym = false;
                    break;
                }
            }
        }
    }

    //cerr << "is_bin_sym: returning " << bin_sym << endl;
    
    return bin_sym;
}

} // namespace MLDB
