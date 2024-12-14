// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* binary_symmetric.h                                              -*- C++ -*-
   Jeremy Barnes, 17 March 2006
   Copyright (C) 2006 Jeremy Barnes.  All rights reserved.
   $Source$

   Code to deal with binary symmetric classifiers.
*/

#pragma once

#include "mldb/plugins/jml/algebra/matrix.h"
#include <vector>

namespace MLDB {


class Training_Data;
class Feature;


/** Check to see if the given dataset and weights are binary symmetric
    for the given set of features.  Will convert the weights array
    back and forth from a 1-dimensional to a two-dimensional depending
    upon the response.
*/
bool
convert_bin_sym(MLDB::Matrix<float, 2> & weights,
                const Training_Data & data,
                const Feature & predicted,
                const std::vector<Feature> & features);

/** Same as above, but doesn't convert it. */
bool
is_bin_sym(const MLDB::Matrix<float, 2> & weights,
           const Training_Data & data,
           const Feature & predicted,
           const std::vector<Feature> & features);

} // namespace MLDB
