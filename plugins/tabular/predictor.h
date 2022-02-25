/* predictor.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <array>
#include <numeric>
#include <limits>
#include "mldb/types/value_description_fwd.h"


namespace MLDB {

struct MappingContext;

struct Predictor {
    static constexpr uint32_t BASIS_DIMS = 2;
    std::array<float, BASIS_DIMS> params = {0};
    int64_t offset = 0;

    float & intercept() { return params[0]; }
    float intercept() const { return params[0]; }

    float & slope() { return params[1]; }
    float slope() const { return params[1]; }

    static constexpr std::array<float, BASIS_DIMS> basis(uint32_t x)
    {
        float fx = x;
        //float fx2 = fx * fx;
        return { 1, fx /*, fx2, fx * fx2*/ };
    }

    int64_t predict(uint32_t x) const;
};

DECLARE_STRUCTURE_DESCRIPTION(Predictor);

} // namespace MLDB
