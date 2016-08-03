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

struct MongoImportConfig: ProcedureConfig {
    static constexpr const char * name = "mongodb.import";
    string connectionScheme;
    string collection;
    PolyConfigT<Dataset> outputDataset;

    MongoImportConfig() {
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
             "[mongodb://][username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database]]");
    addField("collection", &MongoImportConfig::collection,
             "The collection to import");
    addField("outputDataset", &MongoImportConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
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
        case bsoncxx::type::k_document: {
            //auto doc = val.get_document();
            //return bsonToCell(doc.view());
            throw HttpReturnException(500, "BSON doc conversion not done");
        }
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
            
        case bsoncxx::type::k_array:
            throw HttpReturnException(500, "BSON array conversion not done");
            
        case bsoncxx::type::k_regex:
            throw HttpReturnException(500, "BSON regex conversion not done");

        case bsoncxx::type::k_dbpointer:
        case bsoncxx::type::k_code:
        case bsoncxx::type::k_codewscope:
        case bsoncxx::type::k_maxkey:
        case bsoncxx::type::k_minkey:
            throw HttpReturnException(500, "BSON internal conversions not accepted");
        }
        
        throw HttpReturnException(500, "Unknown bson expression type");
    }

//     static ExpressionValue bsonToExpression(const bsoncxx::document::view & doc)
//     {
//         auto ts = Date::fromSecondsSinceEpoch(
//                 doc["_oid"].get_oid().value.get_time_t());
//         StructValue cols;
//         for (auto & el: doc) {
//             cols.emplace_back(PathElement(std::move(el.key().to_string())),
//                               bsonToExpression(el.get_value(), ts));
//         }
//         return std::move(cols);
//     }

    RunOutput run(const ProcedureRunConfig & run,
                  const std::function<bool (const Json::Value &)> & onProgress) const override
    {
        const auto runConfig = applyRunConfOverProcConf(config, run);
        mongocxx::client conn{mongocxx::uri{runConfig.connectionScheme}};
        int idx = runConfig.connectionScheme.rfind('/');
        string dbName = runConfig.connectionScheme.substr(idx + 1);
        auto db = conn[dbName];

        auto logger = MLDB::getMldbLog("MongoDbPluginLogger");
        logger->set_level(spdlog::level::debug);

        logger->debug() << "\n"
            << "Db name:    " << dbName << "\n"
            << "Collection: " << runConfig.collection;

        auto output = createDataset(server, runConfig.outputDataset,
                                    nullptr, true /*overwrite*/);

        typedef std::function<void (vector<Cell> &, const Date &, Path,
                                    bsoncxx::document::view)> PopulateRowFct;
                
        PopulateRowFct populateRow = [&] (
            vector<Cell> & row, const Date & ts,
            Path currPath,
            bsoncxx::document::view doc)
        {
            for (auto & el: doc) {
                if (currPath.empty() && el.key().to_string() == "_id") {
                    // top level _id, skip
                    continue;
                }
                if (el.type() == bsoncxx::type::k_document) {
                    populateRow(row,
                                ts,
                                currPath + std::move(el.key().to_string()),
                                el.get_document().view());
                    continue;
                }
                row.emplace_back(
                    currPath + PathElement(std::move(el.key().to_string())),
                    bsonToCell(el.get_value()),
                    ts);
            }
        };

        {
            auto cursor = db[runConfig.collection].find({});
            for (auto&& doc : cursor) {
                auto oid = doc["_id"].get_oid();
                auto ts = Date::fromSecondsSinceEpoch(oid.value.get_time_t());
                vector<Cell> row;
                Path curr;
                populateRow(row, ts, curr, doc);
                //cerr << bsoncxx::to_json(oid.get_value()) << endl;
                Path p(oid.value.to_string());
                output->recordRow(p, row);
            }
        }


#if 0
        vector<mongo::HostAndPort> hap;
        for (auto & hp: hostsAndPorts)
            hap.emplace_back(hp);
        
        conn.reset(new mongo::DBClientReplicaSet(replicaSetName,
                                                 hap, 100 /* timeout or something */));
  
        string err;
        if(!conn->auth(db, user, pwd, err)){
            throw ML::Exception("MongoDB connection failed with msg [" 
                                + err + "]");
        }

        this->db = db;
        this->collection = collection;

        mongo::BSONObjBuilder q;
        q << "uri" << BSON("$ne" << "");
        
        auto cursor = conn->query(collection + ".files", q.obj());
        while (cursor->more()) {
            auto o = cursor->next();
            
            if (o.hasField("$err")) {
                throw ML::Exception("mongo DB error querying old entries");
            }
        }
#endif
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
