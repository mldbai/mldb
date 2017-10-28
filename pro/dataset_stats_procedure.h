/**
 * dataset_stats_procedure.h
 * Mich, 2016-05-18
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"


namespace MLDB {

struct DatasetStatsProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "dataset.stats";

    DatasetStatsProcedureConfig();

    InputQuery inputData;
    PolyConfigT<Dataset> outputDataset;
};

DECLARE_STRUCTURE_DESCRIPTION(DatasetStatsProcedureConfig);

struct DatasetStatsProcedure: public Procedure {

    DatasetStatsProcedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    DatasetStatsProcedureConfig procedureConfig;
};

} // namespace MLDB

