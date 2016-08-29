/**                                                                 -*- C++ -*-
 * mongo_import.cc
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/
#include "bsoncxx/builder/stream/document.hpp"
#include "mongocxx/client.hpp"
#include "mongocxx/uri.hpp"

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/any_impl.h"
#include "mldb/utils/log.h"

#include "mongo_common.h"


using namespace std;

namespace Datacratic {
namespace MLDB {
namespace Mongo {

typedef tuple<ColumnName, CellValue, Date> Cell;

struct MongoImportConfig: ProcedureConfig {
    static constexpr const char * name = "mongodb.import";
    string connectionScheme;
    string collection;
    PolyConfigT<Dataset> outputDataset;

    int64_t limit;
    int64_t offset;
    bool ignoreParsingErrors;
    SelectExpression select;
    std::shared_ptr<SqlExpression> where;
    std::shared_ptr<SqlExpression> named;

    MongoImportConfig() :
        connectionScheme(""),
        limit(-1),
        offset(0),
        ignoreParsingErrors(false),
        select(SelectExpression::STAR),
        where(SqlExpression::TRUE),
        named(SqlExpression::TRUE)
    {
        outputDataset.withType("sparse.mutable");
    }
};

DECLARE_STRUCTURE_DESCRIPTION(MongoImportConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoImportConfig);

MongoImportConfigDescription::
MongoImportConfigDescription()
{
    addParent<ProcedureConfig>();
    addField("connectionScheme", &MongoImportConfig::connectionScheme,
             mongoScheme);
    addField("collection", &MongoImportConfig::collection,
             "The collection to import");
    addField("outputDataset", &MongoImportConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("limit", &MongoImportConfig::limit,
             "Maximum number of lines to process");
    addField("offset", &MongoImportConfig::offset,
             "Skip the first n lines.", int64_t(0));
    addField("ignoreParsingErrors", &MongoImportConfig::ignoreParsingErrors,
             "If true, any record causing an error will be skipped. Any "
             "record with BSON regex or BSON internal data type will cause an "
             "error.", false);
    addField("select", &MongoImportConfig::select,
             "Which columns to use.",
             SelectExpression::STAR);
    addField("where", &MongoImportConfig::where,
             "Which lines to use to create rows.",
             SqlExpression::TRUE);
    addField("named", &MongoImportConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name and that names cannot be objects. The "
             "default value, `oid()`, refers to the MongoDB ObjectID.",
             SqlExpression::parse("oid()"));

    onPostValidate = [] (MongoImportConfig * config,
                         JsonParsingContext & context)
    {
        validateConnectionScheme(config->connectionScheme);
        validateCollection(config->collection);
    };
}

struct MongoImportProcedure: public Procedure {
    MongoImportProcedure(MldbServer * server,
                         PolyConfig config_,
                         std::function<bool (Json::Value)> onProgress)
        : Procedure(server)
    {
        config = config_.params.convert<MongoImportConfig>();
    }

    Any getStatus() const override
    {
        Json::Value result;
        result["ok"] = true;
        return result;
    }
    MongoImportConfig config;

    RunOutput run(const ProcedureRunConfig & run,
                  const std::function<bool (const Json::Value &)> & onProgress) const override
    {
        const auto runConfig = applyRunConfOverProcConf(config, run);

        mongocxx::uri mongoUri(runConfig.connectionScheme);
        mongocxx::client conn(mongoUri);
        auto db = conn[mongoUri.database()];

        auto logger = MLDB::getMldbLog("MongoDbPluginLogger");
        logger->set_level(spdlog::level::debug);

        logger->debug() << "\n"
            << "Db name:    " << mongoUri.database() << "\n"
            << "Collection: " << runConfig.collection;

        auto output = createDataset(server, runConfig.outputDataset,
                                    nullptr, true /*overwrite*/);

        MongoScope mongoScope(server);
        ExpressionValue storage;
        const auto whereBound  = runConfig.where->bind(mongoScope);
        const auto selectBound = runConfig.select.bind(mongoScope);
        const auto namedBound  = runConfig.named->bind(mongoScope);

        bool useSelect = config.select != SelectExpression::STAR;
        bool useWhere = !config.where->isConstantTrue();

        // using incorrect default value to ease check
        bool useNamed = config.named != SqlExpression::TRUE;

        auto processor = [&](Dataset & output,
                             const bsoncxx::document::view & doc)
        {
            auto oid = doc["_id"].get_oid();
            Path rowName(oid.value.to_string());
            auto ts = Date::fromSecondsSinceEpoch(oid.value.get_time_t());
            ExpressionValue expr(extract(ts, doc));

            if (useWhere || useSelect || useNamed) {
                MongoRowScope row(expr, oid.value.to_string());
                if (useWhere && !whereBound(row, storage, GET_ALL).isTrue()) {
                    return;
                }

                if (useNamed) {
                    rowName = RowName(
                        namedBound(row, storage, GET_ALL).toUtf8String());
                }

                if (useSelect) {
                    expr = selectBound(row, storage, GET_ALL);
                    storage = expr;
                }
            }

            output.recordRowExpr(rowName, expr);
        };

        auto offset = runConfig.offset;
        auto limit = runConfig.limit;
        int errors = 0;
        {
            auto cursor = db[runConfig.collection].find({});
            int i = 0;
            for (auto&& doc : cursor) {
                if (offset > 0) {
                    --offset;
                    continue;
                }
                if (limit == 0) {
                    break;
                }
                else if (limit > 0) {
                    --limit;
                }
                if (++i % 1000 == 0) {
                    logger->debug() << "Processing " << i << "th document";
                }
                if (runConfig.ignoreParsingErrors) {
                    try {
                        processor(*output.get(), doc);
                    }
                    catch (const ML::Exception & exc) {
                        ++ errors;
                        logger->error() << exc.what();
                    }
                }
                else {
                    processor(*output.get(), doc);
                }
            }
            logger->debug() << "Fetched " << i << " documents";
        }
        output->commit();
        Json::Value res = jsonEncode(output->getStatus());
        res["numParsingErrors"] = errors;
        return RunOutput(res);
    }
};

static RegisterProcedureType<MongoImportProcedure, MongoImportConfig>
regMongodbImport(mongodbPackage(),
                 "Import a dataset from mongodb",
                 "MongoImport.md.html");

} // namespace Mongo
} // namespace MLDB
} // namespace Datacratic
