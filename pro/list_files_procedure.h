/**
 * list_files_procedure.h
 * Mich, 2016-05-10
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"


namespace MLDB {

struct ListFilesProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "list.files";

    ListFilesProcedureConfig();

    Url path;
    Date modifiedSince;
    PolyConfigT<Dataset> outputDataset;
    int maxDepth;
};

DECLARE_STRUCTURE_DESCRIPTION(ListFilesProcedureConfig);

struct ListFilesProcedure: public Procedure {

    ListFilesProcedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ListFilesProcedureConfig procedureConfig;
};

} // namespace MLDB

