/**                                                                 -*- C++ -*-
 * mongo_dataset.cc
 * Mich, 2016-08-05
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/
#include <memory>

#include "bsoncxx/oid.hpp"
#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/builder/stream/array.hpp"
#include "mongocxx/client.hpp"

#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/any_impl.h"

#include "mongo_common.h"

using namespace std;

namespace Datacratic {
namespace MLDB {
namespace Mongo {

typedef tuple<Path, CellValue, Date> Cell;

struct MongoMatrixView : MatrixView {

    vector<Path> getRowNames(ssize_t start = 0,
                             ssize_t limit = -1) const override
    {
        return vector<Path>{};
    }

    vector<RowHash> getRowHashes(ssize_t start = 0,
                                 ssize_t limit = -1) const override
    {
        return vector<RowHash>{};
    }

    size_t getRowCount() const override
    {
        return -1;
    }

    bool knownRow(const Path & rowName) const override
    {
        return false;
    }
    
    MatrixNamedRow getRow(const Path & rowName) const override
    {
        throw HttpReturnException(500, "Unimplemented getRow");
    }

    Path getRowName(const RowHash & row) const override
    {
        throw HttpReturnException(500, "Unimplemented getRowName");
    }

    bool knownColumn(const Path & columnName) const override
    {
        return false;
    }
    
    Path getColumnName(ColumnHash column) const override
    {
        throw HttpReturnException(500, "Unimplemented getColumnName");
    }

    vector<Path> getColumnNames() const override
    {
        return vector<Path>{};
    }

    size_t getColumnCount() const override
    {
        return -1;
    }

    uint64_t getRowColumnCount(const Path & row) const override
    {
        return 0;
    }
};

struct MongoColumnIndex : ColumnIndex {

    typedef std::function<bool (const Path & columnName,
                                const ColumnStats & stats)> OnColumnStats;

    MatrixColumn getColumn(const Path & column) const override
    {
        throw HttpReturnException(500, "Unimplemented getColumn");
    }

    bool knownColumn(const Path & column) const override
    {
        return false;
    }

    vector<Path> getColumnNames() const override
    {
        return vector<Path>{};
    }

    vector<Path> getRowNames(ssize_t start = 0,
                             ssize_t limit = -1) const override
    {
        return vector<Path>{};
    }
};

struct MongoDatasetConfig {
    static constexpr const char * name = "mongodb.dataset";
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

    shared_ptr<MatrixView> getMatrixView() const override
    {
        return make_shared<MongoMatrixView>();
    }

    shared_ptr<ColumnIndex> getColumnIndex() const override
    {
        return make_shared<MongoColumnIndex>();
    }

    GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & scope,
                      const Utf8String& alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const override
    {
        //bool useWhere = where != SqlExpression::TRUE;
        SqlBindingScope scope2(scope);
        const auto whereBound = where.bind(scope2);

        using mongocxx::cursor;
        shared_ptr<cursor> res(new cursor(db[collection].find({})));
        shared_ptr<cursor::iterator> el(new cursor::iterator(res->begin()));

        return {[=] (ssize_t numToGenerate, Any token,
                     const BoundParameters & params)
            {
                std::vector<Path> rowsToKeep;
                for (;*el != res->end(), numToGenerate > 0; --numToGenerate, ++*el) {
                    rowsToKeep.emplace_back((**el)["_id"].get_oid().value.to_string());
                    // TODO apply where
                }
                return make_pair(rowsToKeep, Any{});
            },
            "MongoDbDataset row generation"};

    }

    ExpressionValue getRowExpr(const Path & rowName) const override
    {
        using bsoncxx::builder::stream::document;

        document queryDoc;
        queryDoc << "_id" << bsoncxx::oid(rowName.toUtf8String().rawString());
        {
            auto cursor = db[collection].find(queryDoc.view());
            for (auto&& doc : cursor) {
                auto oid = doc["_id"].get_oid();
                Path rowName(oid.value.to_string());
                auto ts = Date::fromSecondsSinceEpoch(oid.value.get_time_t());
                return ExpressionValue(extract(ts, doc));
            }
        }

        StructValue sv;
        return ExpressionValue(sv);
    }
};
static RegisterDatasetType<MongoDataset, MongoDatasetConfig>
regMongodbDataset(mongodbPackage(),
                 "mongodb.dataset",
                 "Dataset type that forwards records to a mongodb database",
                 "MongoRecord.md.html");

} // namespace Mongo
} // namespace MLDB
} // namespace Datacratic
