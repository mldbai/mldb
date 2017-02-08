/**
 * bucketize_procedure.h
 * Mich, 2015-10-27
 * Copyright (c) 2015 mldb.ai inc. All rights reserved.

 * This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

struct BucketizeProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "bucketize";

    BucketizeProcedureConfig();

    InputQuery inputData;
    PolyConfigT<Dataset> outputDataset;
    std::map<std::string, std::pair<float, float>> percentileBuckets;
};

DECLARE_STRUCTURE_DESCRIPTION(BucketizeProcedureConfig);

struct BucketizeProcedure: public Procedure {

    BucketizeProcedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    BucketizeProcedureConfig procedureConfig;
};

} // namespace MLDB

