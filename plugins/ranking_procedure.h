/**
 * ranking_procedure.h
 * Mich, 2016-01-11
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

enum RankingType {
    PERCENTILE,
    INDEX
};
DECLARE_ENUM_DESCRIPTION(RankingType);

struct RankingProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "ranking";

    RankingProcedureConfig();

    InputQuery inputData;
    PolyConfigT<Dataset> outputDataset;
    RankingType rankingType;
    std::string rankingColumnName;

};
DECLARE_STRUCTURE_DESCRIPTION(RankingProcedureConfig);

struct RankingProcedure: public Procedure {

    RankingProcedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    RankingProcedureConfig procedureConfig;
};

} // namespace MLDB

