// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** feature_generators.h                                                   -*- C++ -*-
    Francois Maillet, 27 juillet 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/server/algorithm.h"
#include "mldb/server/function.h"
#include "mldb/types/value_description.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/ml/value_descriptions.h"


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* FeatureHasher FUNCTION                                                    */
/*****************************************************************************/

struct HashedColumnFeatureGeneratorConfig {
    HashedColumnFeatureGeneratorConfig(int numBits = 8)
        : numBits(numBits)
    {
    }

    int numBits;
};

DECLARE_STRUCTURE_DESCRIPTION(HashedColumnFeatureGeneratorConfig);


struct HashedColumnFeatureGenerator: public Function {
    HashedColumnFeatureGenerator(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);
    
    ~HashedColumnFeatureGenerator();

    virtual Any getStatus() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    std::vector<KnownColumn> outputColumns;

    HashedColumnFeatureGeneratorConfig functionConfig;

    int numBuckets() const
    {
        return 1 << functionConfig.numBits;
    }

};


} // namespace MLDB
} // namespace Datacratic
