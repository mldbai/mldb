/**
 * mongo_dataset.cc
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/

#include "mongo_package.h"
#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/any_impl.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

struct MongoDatasetConfig {
    MongoDatasetConfig() {}
    int FIXME;
};
DECLARE_STRUCTURE_DESCRIPTION(MongoDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoDatasetConfig);

MongoDatasetConfigDescription::
MongoDatasetConfigDescription()
{
    addField("FIXME", &MongoDatasetConfig::FIXME, "FIXME");
}

struct MongoDataset: Dataset {

    MongoDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Dataset(owner)
    {
    }
    
    Any getStatus() const override
    {
        return std::string("ok");
    }

    void recordRowItl(
        const RowName & rowName,
        const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) override
    {
    }
    
    void recordRows(
        const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date>>>> & rows) override
    {
    }

    /** Commit changes to the database.  Default is a no-op. */
    void commit() override
    {
    }

    std::pair<Date, Date> getTimestampRange() const override
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    Date quantizeTimestamp(Date timestamp) const override
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    std::shared_ptr<MatrixView> getMatrixView() const override
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    std::shared_ptr<ColumnIndex> getColumnIndex() const override
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    std::shared_ptr<RowStream> getRowStream() const override
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const override
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }
};
static RegisterDatasetType<MongoDataset, MongoDatasetConfig>
regMongodbDataset(mongodbPackage(),
                 "mongodb.record",
                 "Dataset type that forwards records to a mongodb database",
                 "MongoRecord.md.html");

struct Fmlh {
    Fmlh() {
        cerr << "INTO FMLH!!!!!!!" << endl;
    }
} fmlh;

} // namespace MLDB
} // namespace Datacratic
