/**                                                                 -*- C++ -*-
 * mock_procedure.h
 * Mich, 2016-10-18
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

struct MockProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "mock";

    MockProcedureConfig();

    int durationMs;
    int refreshRateMs;
};

DECLARE_STRUCTURE_DESCRIPTION(MockProcedureConfig);

struct MockProcedure: public Procedure {

    MockProcedure(
        MldbServer * owner,
        PolyConfig polyConfig,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    MockProcedureConfig config;
};

} // namespace MLDB
