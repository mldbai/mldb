/* boosted_stumps_core.h                                           -*- C++ -*-
   Jeremy Barnes, 23 February 2004
   Copyright (c) 2004 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Core of the boosted stumps routines.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include "training_data.h"
#include "training_index.h"
#include "stump.h"
#include "stump_predict.h"

namespace MLDB {

/** The boosting loss function.  It is exponential in the margin. */
struct Boosting_Loss {
    MLDB_ALWAYS_INLINE
    float operator () (int label, int corr, float pred, float current) const
    {
        int correct = (corr == label);
        //cerr << "label = " << label << " corr = " << corr << " pred = "
        //     << pred << " correct = " << correct;
        *((int *)&pred) ^= (correct << 31);  // flip sign bit if correct
        //cerr << " flipped = " << pred << " current = " << current
        //     << " result = " << current * exp(pred) << endl;
        return current * exp(pred);
    }
};

/** The logistic boost loss function.  Requires the z function from the boosting
    loss to work; fortunately this is provided by the stumps. */
struct Logistic_Loss {
    Logistic_Loss(double z) : z(z) {}
    
    MLDB_ALWAYS_INLINE
    float operator () (int label, int corr, float pred, float current) const
    {
        int correct = (corr == label);
        *((int *)&pred) ^= (correct << 31);  // flip sign bit if correct
        float qz = exp(pred);
        return 1.0 / ((z/qz * ((1.0 / current) - 1.0)) - 1.0);
    }
    
    double z;
};

/* A "loss" function used to update a set of prediction weights for some
   training data. */
struct Boosting_Predict {
    MLDB_ALWAYS_INLINE
    float operator () (int label, int corr, float pred, float current) const
    {
        return current + pred;
    }
};

template<class Fn>
struct Binsym_Update {
    Binsym_Update(const Fn & fn = Fn()) : fn(fn) {}
    Fn fn;

    template<class FeatureIt, class WeightIt>
    float operator () (const Stump & stump, float cl_weight, int corr,
                       FeatureIt ex_start, FeatureIt ex_range,
                       WeightIt weight_begin, int advance) const
    {
        float pred = stump.predict(0, ex_start, ex_range) * cl_weight;

        //cerr << "    pred = { " << pred << " " << -pred << " }" << endl;

        *weight_begin = fn(0, corr, pred, *weight_begin);
        return *weight_begin * 2.0;
    }

    template<class PredIt, class WeightIt>
    float operator () (PredIt pred_it, PredIt pred_end, int corr,
                       WeightIt weight_begin, int advance) const
    {
        *weight_begin = fn(0, corr, *pred_it, *weight_begin);
        return *weight_begin * 2.0;
    }
}; 

template<class Fn>
struct Normal_Update {
    Normal_Update(const Fn & fn = Fn()) : fn(fn) {}
    Fn fn;
    mutable distribution<float> pred;

    /* Make a prediction and return an update. */
    template<class FeatureIt, class WeightIt>
    float operator () (const Stump & stump, float cl_weight, int corr,
                       FeatureIt ex_start, FeatureIt ex_range,
                       WeightIt weight_begin, int advance) const
    {
        size_t nl = stump.label_count();
        if (pred.size() != nl) pred = distribution<float>(nl);

        stump.predict(pred, ex_start, ex_range);
        if (cl_weight != 1.0) pred *= cl_weight;

        //cerr << "    pred = " << pred << endl;
        
        return operator () (pred.begin(), pred.end(), corr, weight_begin,
                            advance);
    }
    
    /* Perform an update once the predictions are already known. */
    template<class PredIt, class WeightIt>
    float operator () (PredIt pred_it, PredIt pred_end, int corr,
                       WeightIt weight_begin, int advance) const
    {
        size_t nl = pred_end - pred_it;
        //cerr << "nl = " << nl << " advance = " << advance << endl;
        //cerr << __PRETTY_FUNCTION__ << endl;
        float total = 0.0;
        
        if (advance) {
            for (unsigned l = 0;  l < nl;  ++l) {
                *weight_begin = fn(l, corr, pred_it[l], *weight_begin);
                total += *weight_begin;
                weight_begin += advance;
            }
        }
        else
            total = Binsym_Update<Fn>(fn)(pred_it, pred_end, corr,
                                          weight_begin, 0);
        
        return total;
    }
};

} // namespace MLDB
