// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/**
 * csv_export_procedure.cc
 * Mich, 2015-11-11
 * Copyright (c) 2015 Datacratic Inc. All rights reserved.
 **/

#include "csv_export_procedure.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/parallel.h"
#include "mldb/server/function_contexts.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/types/date.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/soa/utils/csv_writer.h"
#include "mldb/plugins/sql_config_validator.h"
#include <memory>

using namespace std;


namespace Datacratic {
namespace MLDB {

CsvExportProcedureConfig::
CsvExportProcedureConfig()
    : headers(true), delimiter(","), quoteChar("\"")
{
}

DEFINE_STRUCTURE_DESCRIPTION(CsvExportProcedureConfig);

CsvExportProcedureConfigDescription::
CsvExportProcedureConfigDescription()
{
    addField("exportData", &CsvExportProcedureConfig::exportData,
             "An SQL query to select the data to be exported.  This could "
             "be any query on an existing dataset.");
    addField("dataFileUrl", &CsvExportProcedureConfig::dataFileUrl,
             "URL where the csv file should be written to. If a file already "
             "exists, it will be overwritten.");
    addField("headers", &CsvExportProcedureConfig::headers,
             "Whether to print headers", true);
    addField("delimiter", &CsvExportProcedureConfig::delimiter,
             "The delimiter to place between each value", string("\""));
    addField("quoteChar", &CsvExportProcedureConfig::quoteChar,
             "The character to enclose the values within when they contain "
             "either a delimiter or a quoteChar", string(","));
    addParent<ProcedureConfig>();

    onPostValidate = [&] (CsvExportProcedureConfig * cfg,
                          JsonParsingContext & context)
    {
        if (cfg->delimiter.size() != 1) {
            throw ML::Exception("delimiter must be 1 char long.");
        }
        if (cfg->quoteChar.size() != 1) {
            throw ML::Exception("Quotechar must be 1 char long.");
        }
        MustContainFrom<InputQuery>()(cfg->exportData, "export.csv");
    };
}

CsvExportProcedure::
CsvExportProcedure(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<CsvExportProcedureConfig>();
}

RunOutput
CsvExportProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);
            
    SqlExpressionMldbContext context(server);
    filter_ostream out(runProcConf.dataFileUrl.toString());
    CsvWriter csv(out, runProcConf.delimiter.at(0),
                  runProcConf.quoteChar.at(0));

    auto boundDataset = runProcConf.exportData.stm->from->bind(context);

    vector<shared_ptr<SqlExpression> > calc;
    BoundSelectQuery bsq(runProcConf.exportData.stm->select,
                         *boundDataset.dataset,
                         boundDataset.asName,
                         runProcConf.exportData.stm->when,
                         *runProcConf.exportData.stm->where,
                         runProcConf.exportData.stm->orderBy,
                         calc);

    const auto columnNames = bsq.getSelectOutputInfo()->allColumnNames();
    vector<string> lineBuffer; // keeps the data that cannot be outputed
                               // yet to the csv due to the ordering difference
                               // between columnNames and the order in which
                               // columns are in the bound select query execute
    const auto lineSize = columnNames.size();
    lineBuffer.resize(columnNames.size());
    const auto columnNamesEnd = columnNames.end();
    const auto columnNamesBegin = columnNames.begin();

    std::mutex lineMutex;
    auto outputCsvLine = [&] (NamedRowValue & row_,
                              const vector<ExpressionValue> & calc)
    {
        std::unique_lock<std::mutex> guard(lineMutex);
        MatrixNamedRow row = row_.flattenDestructive();
        ExcAssert(lineBuffer.size() == columnNames.size());
        size_t lineBufferIndex = 0; // position of the buffered value ready to
                                    // be outputed

        auto outputLineBuffer = [&] () {
            // inline function to make sure the index is set to "" after each
            // use
            csv << lineBuffer[lineBufferIndex];
            lineBuffer[lineBufferIndex] = "";
        };

        for (const auto & col: row.columns) {
            const auto seekColumn = std::get<0>(col); // the column to seek in
                                                      // the csv ordering
            auto columnNamesIt = columnNames.begin() + lineBufferIndex;
            size_t columnIndex;

            auto updatePointers = [&] () {
                // Linear performance will hurt if there are many columns
                for (; *columnNamesIt != seekColumn; ++ columnNamesIt) {
                    // column must always be found, otherwise me should be in a
                    // context where cells have multiple values.
                    if (columnNamesIt == columnNamesEnd) {
                        throw ML::Exception("CSV export does not work over "
                                            "cells having multiple values");
                    }
                }
                columnIndex = columnNamesIt - columnNamesBegin;
            };
            updatePointers();

            if (columnIndex == lineBufferIndex) {
                // immediate output
                csv << std::get<1>(col).toUtf8String().rawString();
                ++ lineBufferIndex;

                // check if the buffer is filled on the next position and
                // output it as long as it is
                for (; lineBufferIndex < lineSize
                       && lineBuffer[lineBufferIndex] != "";
                     ++ lineBufferIndex)
                {
                    outputLineBuffer();
                }
            }
            else {
                // store for later

                if (lineBuffer[columnIndex] != "") {
                    // collision - Happens when a column is found both in an
                    // explicit statement and a star clause. Since they don't
                    // mingle, having a collision means we can output until the
                    // current columnIndex
                    for (; lineBufferIndex <= columnIndex
                        && lineBuffer[lineBufferIndex] != "";
                        ++ lineBufferIndex)
                    {
                        outputLineBuffer();
                    }
                    // find the next index where to store, collision are not
                    // possible
                    ++ columnNamesIt;
                    updatePointers();
                }
                ExcAssert(lineBuffer[columnIndex] == "");
                lineBuffer[columnIndex] =
                    std::get<1>(col).toUtf8String().rawString();
            }
        }

        // output until the end of the buffer
        for (; lineBufferIndex < lineSize; ++ lineBufferIndex) {
            outputLineBuffer();
        }
        csv.endl();
        return true;
    };

    if (runProcConf.headers) {
        for (const auto & name: bsq.getSelectOutputInfo()->allColumnNames()) {
            csv << name.toUtf8String();
        }
        csv.endl();
    }
    bsq.execute(outputCsvLine, 
                runProcConf.exportData.stm->offset, 
                runProcConf.exportData.stm->limit,
                onProgress);
    RunOutput output;
    return output;
}

Any
CsvExportProcedure::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<CsvExportProcedure, CsvExportProcedureConfig>
regCsvExportProcedure(
    builtinPackage(),
    "export.csv",
    "Exports a dataset to a target location as a CSV",
    "procedures/CsvExportProcedure.md.html");

} // namespace MLDB
} // namespace Datacratic
