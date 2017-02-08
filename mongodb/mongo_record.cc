/**                                                                 -*- C++ -*-
 * mongo_record.cc
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
 **/
#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/builder/stream/array.hpp"
#include "mongocxx/client.hpp"

#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/any_impl.h"
#include "mldb/ext/spdlog/include/spdlog/spdlog.h"
#include "mldb/utils/log.h"
#include "mldb/utils/log_fwd.h"

#include "mongo_common.h"

using namespace std;


namespace MLDB {
namespace Mongo {

struct MongoRecordConfig {
    static constexpr const char * name = "mongodb.record";
    MongoRecordConfig() {}
    string uriConnectionScheme;
    string collection;
};
DECLARE_STRUCTURE_DESCRIPTION(MongoRecordConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoRecordConfig);

MongoRecordConfigDescription::
MongoRecordConfigDescription()
{
    addField("uriConnectionScheme", &MongoRecordConfig::uriConnectionScheme,
             mongoConnSchemeAndDesc);
    addField("collection", &MongoRecordConfig::collection,
             "The collection to record to.");

    onPostValidate = [] (MongoRecordConfig * config,
                         JsonParsingContext & context)
    {
        validateConnectionScheme(config->uriConnectionScheme);
        validateCollection(config->collection);
    };
}

typedef tuple<ColumnPath, CellValue, Date> Cell;

struct MongoRecord: Dataset {

    mongocxx::client conn;
    mongocxx::database db;
    string collection;

    MongoRecord(MldbServer * owner,
                 PolyConfig config,
                 const ProgressFunc & onProgress)
        : Dataset(owner)
    {
        auto dsConfig = config.params.convert<MongoRecordConfig>();
        mongocxx::uri mongoUri(dsConfig.uriConnectionScheme);
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

        Dataset::validateNames(rowName, row);
        document topDoc;
        topDoc << "_id" << rowName.toUtf8String().rawString();
        for (const Cell & col: row) {
            auto colName = [&] () {
                // Weird lambda to prevent a freeze within mongo
                Utf8String str = std::get<0>(col).toUtf8String();
                if (str.rawString()[0] == '$') {
                    throw HttpReturnException(
                        400,
                        "Keys starting with a dollar sign cannot be recorded "
                        "to MongoDB. Error on key: " + str.rawString());
                }
                if (str.find('.') != str.end()) {
                    throw HttpReturnException(
                        400,
                        "Dotted keys cannot be recorded "
                        "to MongoDB. Error on key: " + str.rawString());
                }
                return str.rawString();
            };
            const auto cellValue = std::get<1>(col);
            if (cellValue.empty() || cellValue.isString()) {
                topDoc << colName() << cellValue.toString();
            }
            else if (cellValue.isInteger()) {
                int64_t val = cellValue.toInt();
                if (val >= std::numeric_limits<int>::min() && val <= std::numeric_limits<int>::max()) {
                    topDoc << colName() << static_cast<int>(val);
                }
                else {
                    topDoc << colName() << val;
                }
            }
            else if (cellValue.isDouble()) {
                topDoc << colName() << cellValue.toDouble();
            }
            else if (cellValue.isTimestamp()) {
                using bsoncxx::types::b_date;
                topDoc << colName()
                       << b_date(cellValue.toTimestamp().secondsSinceEpoch() * 1000);
            }
            else {
                // TODO MLDB-1918 - should be warning level
                logger->error()
                    << "Uncovered CellValue type conversion from "
                    << cellValue.cellType();
                topDoc << colName()
                    << jsonEncodeUtf8(cellValue).rawString();
            }
        }
        db[collection].insert_one(topDoc.extract());
    }

    void recordRows(
        const std::vector<std::pair<Path, std::vector<Cell>>> & rows) override
    {
        for (const auto & row: rows) {
            recordRowItl(row.first, row.second);
        }
    }

    std::shared_ptr<MatrixView> getMatrixView() const override
    {
        throw HttpReturnException(400,
                                  "mongodb.record dataset is record-only");
    }

    std::shared_ptr<ColumnIndex> getColumnIndex() const override
    {
        throw HttpReturnException(400,
                                  "mongodb.record dataset is record-only");
    }
};
static RegisterDatasetType<MongoRecord, MongoRecordConfig>
regMongodbDataset(mongodbPackage(),
                 "mongodb.record",
                 "Dataset type that forwards records to a MongoDB database",
                 "MongoRecord.md.html");

} // namespace Mongo
} // namespace MLDB

