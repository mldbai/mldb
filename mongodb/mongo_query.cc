/**                                                                 -*- C++ -*-
 * mongo_query.cc
 * Mich, 2016-08-05
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/

#include <string>

#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/json.hpp"
#include "mongocxx/client.hpp"

#include "mldb/core/function.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"

#include "mongo_common.h"


using namespace std;

namespace Datacratic {
namespace MLDB {
namespace Mongo {


struct MongoQueryConfig {
    string connectionScheme;
    string collection;
    string query;
};

DECLARE_STRUCTURE_DESCRIPTION(MongoQueryConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoQueryConfig);

MongoQueryConfigDescription::
MongoQueryConfigDescription()
{
    addField("connectionScheme", &MongoQueryConfig::connectionScheme,
             mongoScheme);
    addField("collection", &MongoQueryConfig::collection,
             "The collection to query");
}

struct MongoQueryFunction: Function {
    MongoQueryFunction(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress)
            : Function(owner)
    {
        queryConfig = config.params.convert<MongoQueryConfig>();
        mongoUri = mongocxx::uri(queryConfig.connectionScheme);
    }

    Any
    getStatus() const override
    {
        return Any();
    }

    ExpressionValue apply(const FunctionApplier & applier,
                          const ExpressionValue & context) const override
    {
        using bsoncxx::builder::stream::document;

        const auto & query = context.getColumn(PathElement("query"));
        const auto queryStr = query.toUtf8String().rawString();

        auto queryDoc = bsoncxx::from_json(queryStr);

        auto conn = mongocxx::client(mongoUri);
        auto db = conn[mongoUri.database()];

        {
            auto cursor = db[queryConfig.collection].find(queryDoc.view());
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

    FunctionInfo
    getFunctionInfo() const override
    {
        FunctionInfo result;

        std::vector<KnownColumn> inputColumns, outputColumns;
        inputColumns.emplace_back(
            PathElement("query"), std::make_shared<AtomValueInfo>(),
            COLUMN_IS_DENSE, 0);

        result.input.reset(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
        result.output.reset(new RowValueInfo(outputColumns, SCHEMA_OPEN));

        return result;
    }

    MongoQueryConfig queryConfig;
    mongocxx::uri mongoUri;
};

static RegisterFunctionType<MongoQueryFunction, MongoQueryConfig>
regMongodbDataset(mongodbPackage(),
                 "mongodb.query",
                 "Takes a mongodb query, forwards it to mongo, parses the "
                 "result and returns it as an MLDB result.",
                 "MongoQueryFunction.md.html");


} // namespace Mongo
} // namespace MLDB
} // namespace Datacratic
