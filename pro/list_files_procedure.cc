/**
 * list_files_procedure.cc
 * Mich, 2016-15-11
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#include "list_files_procedure.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/bound_queries.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/date.h"
#include "mldb/pro/pro_plugin.h"
#include <memory>

using namespace std;



namespace MLDB {

ListFilesProcedureConfig::
ListFilesProcedureConfig() : maxDepth(-1)
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(ListFilesProcedureConfig);

ListFilesProcedureConfigDescription::
ListFilesProcedureConfigDescription()
{
    addField("path", &ListFilesProcedureConfig::path,
             "The path to start listing files from. ");
    addField("modifiedSince", &ListFilesProcedureConfig::modifiedSince,
             "Filter that will keep the files modified since the "
             "specified date.", Date::negativeInfinity());
    addField("outputDataset", &ListFilesProcedureConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("maxDepth", &ListFilesProcedureConfig::maxDepth,
             "Maximum depth of directories to go in. -1 means unlimited.", -1);
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
    typedef tuple<ColumnPath, CellValue, Date> Cell;

    Date now = Date::now();

    vector<std::pair<RowPath, vector<Cell>>> rows;
    const int ROWS_SIZE(1024);
    rows.reserve(ROWS_SIZE);

    auto onFoundObject = [&](const std::string &uri,
                             const FsObjectInfo &info,
                             const OpenUriObject & open,
                             int depth) {
        if (info.lastModified < runProcConf.modifiedSince) {
            return true;
        }

        vector<Cell> cells;
        cells.emplace_back(ColumnPath("size"), info.size, now);
        cells.emplace_back(ColumnPath("last_modified"), info.lastModified, now);
        cells.emplace_back(ColumnPath("etag"), info.etag, now);
        cells.emplace_back(ColumnPath("storage_class"), info.storageClass, now);
        cells.emplace_back(ColumnPath("owner_id"), info.ownerId, now);
        cells.emplace_back(ColumnPath("owner_name"), info.ownerName, now);
        cells.emplace_back(ColumnPath("uri"), uri, now);
        rows.push_back(make_pair(RowPath(uri), std::move(cells)));
        if (rows.size() == ROWS_SIZE) {
            output->recordRows(rows);
            rows.clear();
        }
        return true;
    };

    auto onSubDir = [&] (const std::string & dirName, int depth)
    {
        return runProcConf.maxDepth == -1 || depth <= runProcConf.maxDepth;
    };
    forEachUriObject(runProcConf.path.toString(), onFoundObject, onSubDir);

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
    proPackage(),
    "Lists all files found at a given path.",
    "ListFilesProcedure.md.html",
    nullptr /* static route */,
    { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB


