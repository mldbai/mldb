/**
 * list_files_procedure.cc
 * Mich, 2016-15-11
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/

#include "list_files_procedure.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/parallel.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/types/date.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/plugins/sql_config_validator.h"
#include <memory>

using namespace std;


namespace Datacratic {
namespace MLDB {

ListFilesProcedureConfig::
ListFilesProcedureConfig()
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(ListFilesProcedureConfig);

ListFilesProcedureConfigDescription::
ListFilesProcedureConfigDescription()
{
    addField("path", &ListFilesProcedureConfig::path,
             "The path to start listing files from.");
    addField("modifiedSince", &ListFilesProcedureConfig::modifiedSince,
             "Filter that will keep the files modified since the "
             "specified date.", Date::negativeInfinity());
    addField("outputDataset", &ListFilesProcedureConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("tabular"));
    addParent<ProcedureConfig>();
}

ListFilesProcedure::
ListFilesProcedure(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<ListFilesProcedureConfig>();
}

RunOutput
ListFilesProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);

    SqlExpressionMldbScope context(server);

    auto output = createDataset(server, runProcConf.outputDataset,
                                nullptr, true /*overwrite*/);
    typedef tuple<ColumnName, CellValue, Date> Cell;

    Date now = Date::now();

    vector<std::pair<RowName, vector<Cell>>> rows;
    const int ROWS_SIZE(1024);
    rows.reserve(ROWS_SIZE);

    auto onFoundObject = [&](const std::string &uri,
                             const FsObjectInfo &info,
                             const OpenUriObject & open,
                             int depth) {
        if (info.lastModified < runProcConf.modifiedSince) {
            return true;
        }

        //RowName rowName(idx++);
        //Cell c(ColumnName("path"), uri, now);
        vector<Cell> cells;
        cells.emplace_back(ColumnName("size"), info.size, now);
        cells.emplace_back(ColumnName("last_modified"), info.lastModified, now);
        cells.emplace_back(ColumnName("etag"), info.etag, now);
        cells.emplace_back(ColumnName("storage_class"), info.storageClass, now);
        cells.emplace_back(ColumnName("owner_id"), info.ownerId, now);
        cells.emplace_back(ColumnName("owner_name"), info.ownerName, now);
        rows.push_back(make_pair(RowName(uri), cells));
        if (rows.size() == ROWS_SIZE) {
            output->recordRows(rows);
            rows.clear();
        }
        return true;
    };

    auto onSubDir = [&] (const std::string & dirName, int depth) {
        return true;
    };

    string startScanAt = "";
    forEachUriObject(runProcConf.path, onFoundObject, onSubDir, "/",
                     startScanAt);

    if (rows.size()) {
        output->recordRows(rows);
        rows.clear();
    }
    output->commit();
    return output->getStatus();
}

Any
ListFilesProcedure::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<ListFilesProcedure, ListFilesProcedureConfig>
regListFilesProcedure(
    builtinPackage(),
    "Lists all files found at a given path.",
    "procedures/ListFilesProcedure.md.html",
    nullptr /* static route */,
    { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB
} // namespace Datacratic

