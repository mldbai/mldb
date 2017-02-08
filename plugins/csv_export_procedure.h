/**
 * csv_export_procedure.h
 * Mich, 2015-11-11
 * This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
 **/

#pragma once
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

struct CsvExportProcedureConfig : ProcedureConfig {
    CsvExportProcedureConfig()
        : headers(true), skipDuplicateCells(false),
          delimiter(","), quoteChar("\"")
    {
    }

    static constexpr const char * name = "export.csv";

    InputQuery exportData;
    Url dataFileUrl;
    bool headers;
    bool skipDuplicateCells;
    std::string delimiter;
    std::string quoteChar;
};

DECLARE_STRUCTURE_DESCRIPTION(CsvExportProcedureConfig);


struct CsvExportProcedure: public Procedure {

    CsvExportProcedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    CsvExportProcedureConfig procedureConfig;
};

} // namespace MLDB

