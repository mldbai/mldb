/**
 * summary_statistics_proc.h
 * Mich, 2016-06-30
 * Copyright (c) 2016 mldb.ai inc. All rights reserved.
 *
 * Generates column statistics based on an input query. The statistics are
 * computed from a sequence of mldb sql queries.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

struct SummaryStatisticsProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "summary.statistics";

    SummaryStatisticsProcedureConfig();

    InputQuery inputData;
    PolyConfigT<Dataset> outputDataset;
    std::map<std::string, std::pair<float, float>> percentileBuckets;
    bool gotWildcard;
};

DECLARE_STRUCTURE_DESCRIPTION(SummaryStatisticsProcedureConfig);

struct SummaryStatisticsProcedure: public Procedure {

    SummaryStatisticsProcedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    SummaryStatisticsProcedureConfig procedureConfig;
};

} // namespace MLDB

