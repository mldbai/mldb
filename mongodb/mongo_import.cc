/**
 * mongo_import.cc
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/
#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/any_impl.h"
#include "mldb/utils/log.h"
#include "mldb/server/dataset_context.h"
#include "mongo_package.h"

#include "bsoncxx/builder/stream/document.hpp"
#include "bsoncxx/json.hpp"

#include "mongocxx/client.hpp"
#include "mongocxx/options/find.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/logger.hpp"
#include "mongocxx/uri.hpp"

using namespace std;

using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::open_document;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::finalize;

namespace Datacratic {
namespace MLDB {

typedef tuple<ColumnName, CellValue, Date> Cell;

struct MongoRowScope : SqlRowScope {
    MongoRowScope(const ExpressionValue & expr, const string & oid)
        : expr(expr), oid(oid) {}
    const ExpressionValue & expr;
    const string oid;
};

struct MongoScope : SqlExpressionMldbScope {

    MongoScope(MldbServer * server) : SqlExpressionMldbScope(server){}

    ColumnGetter doGetColumn(const Utf8String & tableName,
                             const ColumnName & columnName) override
    {
        return {[=] (const SqlRowScope & scope, ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
            {
                const auto & row = scope.as<MongoRowScope>();
                const ExpressionValue * res =
                    row.expr.tryGetNestedColumn(columnName, storage, filter);
                if (res) {
                    return *res;
                }
                return storage = ExpressionValue();
            },
            std::make_shared<AtomValueInfo>()
        };
    }

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep) override
    {
        std::vector<KnownColumn> columnsWithInfo;

        auto exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
        {
            const auto & row = scope.as<MongoRowScope>();
            StructValue result;
            result.reserve(row.expr.rowLength());

            const auto onCol = [&] (const PathElement & columnName,
                                    const ExpressionValue & val)
            {
                const auto & newColName = keep(columnName);
                if (!newColName.empty()) {
                    result.emplace_back(newColName.front(), val);
                }
                return true;
            };
            row.expr.forEachColumnDestructive(onCol);
            result.shrink_to_fit();
            return result;
        };
        GetAllColumnsOutput result;
        result.exec = exec;
        result.info = std::make_shared<RowValueInfo>(
            std::move(columnsWithInfo), SCHEMA_OPEN);
        return result;
    }

    BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope) override
    {
        if (functionName == "oid") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                {
                    const auto & row = scope.as<MongoRowScope>();
                    return ExpressionValue(row.oid,
                                           Date::negativeInfinity());
                },
                std::make_shared<IntegerValueInfo>() // TODO
            };
        }
        return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                              argScope);
    }

};

struct MongoImportConfig: ProcedureConfig {
    static constexpr const char * name = "mongodb.import";
    string connectionScheme;
    string collection;
    PolyConfigT<Dataset> outputDataset;

    int64_t limit;
    int64_t offset;
    bool ignoreBadLines; // TODO
    SelectExpression select;
    std::shared_ptr<SqlExpression> where;
    std::shared_ptr<SqlExpression> named;

    MongoImportConfig() :
          limit(-1),
          offset(0),
          ignoreBadLines(false),
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
             "MongoDB connection scheme. "
             "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database]]");
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
    addField("ignoreBadLines", &MongoImportConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. Any line "
             "with an invalid JSON object will cause an error.", false);
    addField("select", &MongoImportConfig::select,
             "Which columns to use.",
             SelectExpression::STAR);
    addField("where", &MongoImportConfig::where,
             "Which lines to use to create rows.",
             SqlExpression::TRUE);
    addField("named", &MongoImportConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name and that names cannot be objects.",
             SqlExpression::parse("oid()"));
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

    static CellValue bsonToCell(const bsoncxx::types::value & val)
    {
        switch (val.type()) {
        case bsoncxx::type::k_undefined:
        case bsoncxx::type::k_null:
            return CellValue{};
        case bsoncxx::type::k_double:
            return CellValue(val.get_double().value);
        case bsoncxx::type::k_utf8:
            return CellValue(val.get_utf8().value.to_string());
        case bsoncxx::type::k_binary: {
            auto bin = val.get_binary();
            return CellValue::blob((const char *)bin.bytes, bin.size);
        }
        case bsoncxx::type::k_oid:
            return CellValue(val.get_oid().value.to_string());
        case bsoncxx::type::k_bool:
            return CellValue(val.get_bool().value);
        case bsoncxx::type::k_date:
            return CellValue(val.get_date().value);
        case bsoncxx::type::k_timestamp:
            return CellValue(val.get_timestamp().timestamp);
        case bsoncxx::type::k_int32:
            return CellValue(val.get_int32().value);
        case bsoncxx::type::k_int64:
            return CellValue(val.get_int64().value);
        case bsoncxx::type::k_symbol:
            return CellValue(val.get_symbol().symbol.to_string());

        case bsoncxx::type::k_regex:
            throw HttpReturnException(500, "BSON regex conversion not done");

        case bsoncxx::type::k_array:
        case bsoncxx::type::k_document:
            ExcAssert(false);

        case bsoncxx::type::k_dbpointer:
        case bsoncxx::type::k_code:
        case bsoncxx::type::k_codewscope:
        case bsoncxx::type::k_maxkey:
        case bsoncxx::type::k_minkey:
            throw HttpReturnException(500, "BSON internal conversions not accepted");
        }


        throw HttpReturnException(500, "Unknown bson expression type");
    }

    static StructValue extract(const Date & ts,
                               const bsoncxx::document::view & doc)
    {
        StructValue row;
        for (auto & el: doc) {
            if (el.type() == bsoncxx::type::k_document) {
                row.emplace_back(
                    std::move(el.key().to_string()),
                    extract(ts, el.get_document().view()));
            }
            else if (el.type() == bsoncxx::type::k_array) {
                row.emplace_back(
                    std::move(el.key().to_string()),
                    extract(ts, el.get_array()));
            }
            else {
                row.emplace_back(
                    std::move(el.key().to_string()),
                    ExpressionValue(bsonToCell(el.get_value()), ts));
            }
        }
        return row;
    }

    static StructValue extract(const Date & ts,
                               const bsoncxx::array::view & arr)
    {
        int i = 0;
        StructValue row;
        for (auto it = arr.begin(); it != arr.end(); ++it, ++i) {
            bsoncxx::array::element el = *it;
            if (el.type() == bsoncxx::type::k_document) {
                row.emplace_back(i, extract(ts, el.get_document().view()));
            }
            else if (el.type() == bsoncxx::type::k_array) {
                row.emplace_back(i, extract(ts, el.get_array()));
            }
            else {
                row.emplace_back(
                    i, ExpressionValue(bsonToCell(el.get_value()), ts));
            }
        }
        return row;
    }


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
        bool useWhere = config.where != SqlExpression::TRUE;

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
                processor(*output.get(), doc);
            }
            logger->debug() << "Fetched " << i << " documents";
        }
        output->commit();
        return output->getStatus();
    }
};

static RegisterProcedureType<MongoImportProcedure, MongoImportConfig>
regMongodbImport(mongodbPackage(),
                 "Import a dataset from mongodb",
                 "MongoImport.md.html");

} // namespace MLDB
} // namespace Datacratic
