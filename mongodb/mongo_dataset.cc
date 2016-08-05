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

#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/builder/stream/array.hpp"

#include "mongocxx/client.hpp"

using namespace std;

namespace Datacratic {
namespace MLDB {

struct MongoDatasetConfig {
    static constexpr const char * name = "mongodb.record";
    MongoDatasetConfig() {}
    string connectionScheme;
    string collection;
};
DECLARE_STRUCTURE_DESCRIPTION(MongoDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoDatasetConfig);

MongoDatasetConfigDescription::
MongoDatasetConfigDescription()
{
    addField("connectionScheme", &MongoDatasetConfig::connectionScheme,
             mongoScheme);
    addField("collection", &MongoDatasetConfig::collection,
             "The collection to import");
}

typedef tuple<ColumnName, CellValue, Date> Cell;

struct MongoDataset: Dataset {

    mongocxx::client conn;
    mongocxx::database db;
    string collection;

    MongoDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Dataset(owner)
    {
        auto dsConfig = config.params.convert<MongoDatasetConfig>();
        mongocxx::uri mongoUri(dsConfig.connectionScheme);
        conn = mongocxx::client(mongoUri);
        db = conn[mongoUri.database()];
        collection = dsConfig.collection;
    }
    
    Any getStatus() const override
    {
        return std::string("ok");
    }

    void recordRowItl(const Path & rowName,
                      const std::vector<Cell> & row) override
    {
        using bsoncxx::builder::stream::document;
        using bsoncxx::builder::stream::open_document;
        using bsoncxx::builder::stream::close_document;
        using bsoncxx::builder::stream::open_array;
        using bsoncxx::builder::stream::close_array;
        using bsoncxx::builder::stream::finalize;

        Dataset::validateNames(rowName, row); // TODO ouin?
        document topDoc;
        bsoncxx::builder::stream::array topArray;
        topDoc << "rowName" << rowName.toUtf8String().rawString();
        for (const Cell & col: row) {
            auto & colName = std::get<0>(col);
            if (colName.toUtf8String().rawString().find(".") != string::npos) {
                throw HttpReturnException(
                    400, "Cannot push dotted keys into MongoDB.");
            }
            topArray
                << open_document
                << "columnName" << colName.toUtf8String().rawString()
                << "ts" << std::get<2>(col).secondsSinceEpoch()
                << "data" << jsonEncodeUtf8(std::get<1>(col)).rawString()
                << close_document;
        }
        topDoc << "columns" << bsoncxx::types::b_array{topArray.view()};
        db[collection].insert_one(topDoc.extract());
    }
    
    void recordRows(
        const std::vector<std::pair<Path, std::vector<Cell>>> & rows) override
    {
        for (const auto row: rows) {
            recordRowItl(row.first, row.second);
        }
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

} // namespace MLDB
} // namespace Datacratic
