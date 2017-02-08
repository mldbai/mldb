/** feature_generators.h                                           -*- C++ -*-
    Francois Maillet, 27 juillet 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/value_function.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/ml/value_descriptions.h"



namespace MLDB {


/*****************************************************************************/
/* FeatureHasher FUNCTION                                                    */
/*****************************************************************************/

enum HashingMode {
    COLUMNS,
    COLUMNS_AND_VALUES
};

struct HashedColumnFeatureGeneratorConfig {
    HashedColumnFeatureGeneratorConfig(int numBits = 8)
        : numBits(numBits), mode(COLUMNS)
    {
    }

    int numBits;
    HashingMode mode;
};

DECLARE_STRUCTURE_DESCRIPTION(HashedColumnFeatureGeneratorConfig);

struct FeatureGeneratorInput {
    ExpressionValue columns;  // row 
};

DECLARE_STRUCTURE_DESCRIPTION(FeatureGeneratorInput);

struct FeatureGeneratorOutput {
    ExpressionValue hash; // column
};

DECLARE_STRUCTURE_DESCRIPTION(FeatureGeneratorOutput);

struct HashedColumnFeatureGenerator: public ValueFunctionT<FeatureGeneratorInput, FeatureGeneratorOutput>  {
    HashedColumnFeatureGenerator(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);
    
    ~HashedColumnFeatureGenerator();

    virtual FeatureGeneratorOutput call(FeatureGeneratorInput input) const override;

    std::vector<KnownColumn> outputColumns;

    HashedColumnFeatureGeneratorConfig functionConfig;

    int numBuckets() const
    {
        return 1 << functionConfig.numBits;
    }

};


} // namespace MLDB

