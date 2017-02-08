/**                                                                 -*- C++ -*-
 * mongo_query.cc
 * Mich, 2016-08-05
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#include <string>

#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/builder/stream/array.hpp"
#include "bsoncxx/json.hpp"
#include "mongocxx/client.hpp"
#include "mongocxx/stdx.hpp"

#include "mldb/core/function.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/plugins/sql_functions.h"

#include "mongo_common.h"


using namespace std;


namespace MLDB {
namespace Mongo {


struct MongoQueryConfig {
    string uriConnectionScheme;
    string collection;
    string query;
    SqlQueryOutput outputType;

    MongoQueryConfig() : outputType(FIRST_ROW) {}
};

DECLARE_STRUCTURE_DESCRIPTION(MongoQueryConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoQueryConfig);

MongoQueryConfigDescription::
MongoQueryConfigDescription()
{
    addField("uriConnectionScheme", &MongoQueryConfig::uriConnectionScheme,
             mongoConnSchemeAndDesc);
    addField("collection", &MongoQueryConfig::collection,
             "The collection to query.");

    // Desc copied from sql_functions.cc.
    addField("output", &MongoQueryConfig::outputType,
             "Controls how the query output is converted into a row. "
             "`FIRST_ROW` (the default) will return only the first row produced "
             "by the query.  `NAMED_COLUMNS` will construct a row from the "
             "whole returned table, which must have a 'value' column "
             "containing the value.  If there is a 'column' column, it will "
             "be used as a column name, otherwise the row name will be used.",
             FIRST_ROW);

    onPostValidate = [] (MongoQueryConfig * config,
                         JsonParsingContext & context)
    {
        validateConnectionScheme(config->uriConnectionScheme);
        validateCollection(config->collection);
    };
}

struct MongoQueryFunction: Function {
    MongoQueryFunction(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress)
            : Function(owner, config)
    {
        queryConfig = config.params.convert<MongoQueryConfig>();
        mongoUri = mongocxx::uri(queryConfig.uriConnectionScheme);
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
        using bsoncxx::builder::stream::array;
        using bsoncxx::builder::stream::open_document;
        using bsoncxx::builder::stream::close_document;

        const auto & query = context.getColumn(PathElement("query"));
        if (query.empty()) {
            throw HttpReturnException(
                400, "You must define the parameter \"query\"");
        }
        const auto queryStr = query.toUtf8String().rawString();
        bsoncxx::v_noabi::document::value queryDoc = bsoncxx::from_json(queryStr);
        Json::Value jsonVal = Json::parse(queryStr);
        document doc;

        if (jsonVal.isMember("_id")) {
            array idOpts;
            string _id = jsonVal.removeMember("_id").asString();
            if (_id.size() == 12 || _id.size() == 24) {
                idOpts << open_document
                       << "_id" << bsoncxx::oid(_id)
                       << close_document;
            }
            else {
                idOpts << open_document
                       << "_id" << _id
                       << close_document;
            }
            doc << "$or" << bsoncxx::types::b_array{idOpts.view()};
        }

        auto conn = mongocxx::client(mongoUri);
        auto db = conn[mongoUri.database()];

        {
            mongocxx::stdx::optional<mongocxx::v_noabi::cursor> cursor;
            if (queryConfig.outputType == FIRST_ROW) {
                mongocxx::options::find opts;
                opts.limit(2); // Limit 1 yields error unset document::element
                if (doc.view().empty()) {
                    cursor = db[queryConfig.collection].find(queryDoc.view(), opts);
                }
                else {
                    cursor = db[queryConfig.collection].find(doc.view(), opts);
                }
                for (auto&& it : *cursor) {
                    Date ts = Date::negativeInfinity();
                    if ((it)["_id"].type() == bsoncxx::type::k_oid) {
                        auto oid = (it)["_id"].get_oid();
                        ts = Date::fromSecondsSinceEpoch(oid.value.get_time_t());
                    }
                    return ExpressionValue(extract(ts, it));
                }
            }
            else if (queryConfig.outputType == NAMED_COLUMNS) {
                if (doc.view().empty()) {
                    cursor = db[queryConfig.collection].find(queryDoc.view());
                }
                else {
                    cursor = db[queryConfig.collection].find(doc.view());
                }
                StructValue row;
                for (auto && el: *cursor) {
                    if ((el)["_id"].type() != bsoncxx::type::k_oid) {
                        throw HttpReturnException(
                            500,
                            "monbodb.query: unimplemented support for "
                            "MongoDB records with key \"_id\" that are not "
                            "objectIDs.");
                    }
                    auto oid = el["_id"].get_oid();
                    PathElement rowName(oid.value.to_string());
                    auto ts = Date::fromSecondsSinceEpoch(oid.value.get_time_t());
                    ExpressionValue expr(extract(ts, el));
                    row.emplace_back(std::move(rowName), std::move(expr));
                }
                return std::move(row);
            }
            else {
                throw MLDB::Exception("Unknown outputType");
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

        result.input.emplace_back(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
        result.output.reset(new RowValueInfo(outputColumns, SCHEMA_OPEN));

        return result;
    }

    MongoQueryConfig queryConfig;
    mongocxx::uri mongoUri;
};

static RegisterFunctionType<MongoQueryFunction, MongoQueryConfig>
regMongodbDataset(mongodbPackage(),
                 "mongodb.query",
                 "Takes a MongoDB query, forwards it to MongDB, parses the "
                 "result and returns it as an MLDB result.",
                 "MongoQueryFunction.md.html");


} // namespace Mongo
} // namespace MLDB

