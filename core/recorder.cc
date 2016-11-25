/** recorder.cc
    Jeremy Barnes, 26 March 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.
*/

#include "recorder.h"
#include "mldb/rest/poly_collection.h"
#include "mldb/rest/poly_collection_impl.h"


namespace MLDB {


/*****************************************************************************/
/* RECORDER                                                                  */
/*****************************************************************************/

Recorder::
Recorder(MldbServer * server)
{
}

Any
Recorder::
getStatus() const
{
    return Any();
}
    
void
Recorder::
recordRowExprDestructive(RowPath rowName,
                         ExpressionValue expr)
{
    recordRowExpr(rowName, expr);
}

void
Recorder::
recordRowDestructive(RowPath rowName,
                     std::vector<std::tuple<ColumnPath, CellValue, Date> > vals)
{
    recordRow(rowName, vals);
}

void
Recorder::
recordRowsDestructive(std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows)
{
    recordRows(rows);
}

void
Recorder::
recordRowsExprDestructive(std::vector<std::pair<RowPath, ExpressionValue > > rows)
{
    recordRowsExpr(rows);
}

void
Recorder::
finishedChunk()
{
}

std::function<void (RowPath rowName, Date timestamp,
                    CellValue * vals, size_t numVals,
                    std::vector<std::pair<ColumnPath, CellValue> > extra)>
Recorder::
specializeRecordTabular(const std::vector<ColumnPath> & columnNames)
{
    return [=] (RowPath rowName, Date timestamp,
                CellValue * vals, size_t numVals,
                std::vector<std::pair<ColumnPath, CellValue> > extra)
        {
            recordTabularImpl(std::move(rowName), timestamp,
                              vals, numVals, std::move(extra),
                              columnNames);
        };
}

void
Recorder::
recordTabularImpl(RowPath rowName,
                  Date timestamp,
                  CellValue * vals,
                  size_t numVals,
                  std::vector<std::pair<ColumnPath, CellValue> > extra,
                  const std::vector<ColumnPath> & columnNames)
{
    ExcAssertEqual(columnNames.size(), numVals);
    std::vector<std::tuple<ColumnPath, CellValue, Date> > result;
    result.reserve(numVals + extra.size());

    for (unsigned i = 0;  i < columnNames.size();  ++i) {
        result.emplace_back(columnNames[i], std::move(vals[i]), timestamp);
    }

    for (auto & e: extra) {
        result.emplace_back(std::move(e.first), std::move(e.second), timestamp);
    }

    recordRowDestructive(std::move(rowName), std::move(result));
}


/*****************************************************************************/
/* REGISTRATION FUNCTIONS                                                    */
/*****************************************************************************/

std::shared_ptr<Recorder>
createRecorder(MldbServer * server,
               const PolyConfig & config,
               const std::function<bool (const Json::Value & progress)> & onProgress)
{
    return PolyCollection<Recorder>::doConstruct(MldbEntity::getPeer(server),
                                                 config, onProgress);
}

DEFINE_STRUCTURE_DESCRIPTION_NAMED(RecorderPolyConfigDescription,
                                   PolyConfigT<Recorder>);

RecorderPolyConfigDescription::
RecorderPolyConfigDescription()
{
    addParent<PolyConfig>();
    setTypeName("Recorder");
    documentationUri = "/doc/builtin/recorders/RecorderConfig.md";
}

std::shared_ptr<RecorderType>
registerRecorderType(const Package & package,
                    const Utf8String & name,
                    const Utf8String & description,
                    std::function<Recorder * (RestDirectory *,
                                             PolyConfig,
                                             const std::function<bool (const Json::Value)> &)>
                        createEntity,
                    TypeCustomRouteHandler docRoute,
                    TypeCustomRouteHandler customRoute,
                    std::shared_ptr<const ValueDescription> config,
                    std::set<std::string> registryFlags)
{
    return PolyCollection<Recorder>
        ::registerType(package, name, description, createEntity,
                       docRoute, customRoute,
                       config, registryFlags);
}

} // namespace MLDB

