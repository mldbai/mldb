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
//#include "mongo_dataset.h"
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

struct MongoImportConfig: ProcedureConfig {
    static constexpr const char * name = "mongodb.import";
    Utf8String connectionScheme;
    Utf8String collection;
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

    virtual Any getStatus() const
    {
        Json::Value result;
        result["ok"] = true;
        return result;
    }
    MongoImportConfig config;

    static ExpressionValue bsonToExpression(const bsoncxx::types::value & val,
                                            Date ts)
    {
        switch (val.type()) {
        case bsoncxx::type::k_undefined:
        case bsoncxx::type::k_null:
            return ExpressionValue::null(ts);
        case bsoncxx::type::k_double:
            return ExpressionValue(val.get_double().value, ts);
        case bsoncxx::type::k_utf8:
            return ExpressionValue(val.get_utf8().value.to_string(), ts);
        case bsoncxx::type::k_document: {
            auto doc = val.get_document();
            return bsonToExpression(doc.view());
        }
        case bsoncxx::type::k_binary: {
            auto bin = val.get_binary();
            return ExpressionValue(CellValue::blob((const char *)bin.bytes, bin.size),
                                   ts);
        }
        case bsoncxx::type::k_oid:
            return ExpressionValue(val.get_oid().value.to_string(), ts);
        case bsoncxx::type::k_bool:
            return ExpressionValue(val.get_bool().value, ts);
        case bsoncxx::type::k_date:
            return ExpressionValue(val.get_date().value, ts);
        case bsoncxx::type::k_timestamp:
            return ExpressionValue(val.get_timestamp().timestamp, ts);
        case bsoncxx::type::k_int32:
            return ExpressionValue(val.get_int32().value, ts);
        case bsoncxx::type::k_int64:
            return ExpressionValue(val.get_int64().value, ts);
        case bsoncxx::type::k_symbol:
            return ExpressionValue(val.get_symbol().symbol.to_string(), ts);
            
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

    static ExpressionValue bsonToExpression(const bsoncxx::document::view & doc)
    {
        auto ts = Date::fromSecondsSinceEpoch(doc["_oid"].get_oid().value.get_time_t());
        StructValue cols;
        for (auto & el: doc) {
            cols.emplace_back(PathElement(std::move(el.key().to_string())),
                              bsonToExpression(el.get_value(), ts));
        }
        return std::move(cols);
    }

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        const char * uri = getenv("MONGO_DB_URI");
        if (!uri)
            uri = "mongodb://localhost:27017";
        //cerr << "importing mongodb dataset" << endl;

        mongocxx::client conn{mongocxx::uri{uri}};

        auto db = conn["tutorial"];

        cerr << "connected to " << db.name() << endl;

        // Query for all the documents in a collection.
        {
            auto cursor = db["users"].find({});
            for (auto&& doc : cursor) {
                //auto expr = bsonToExpression(doc);
                std::cout << bsoncxx::to_json(doc) << std::endl;
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
        RunOutput result;
        return result;
    }
};

static RegisterProcedureType<MongoImportProcedure, MongoImportConfig>
regMongodbImport(mongodbPackage(),
                 "Import a dataset from mongodb",
                 "MongoImport.md.html");

} // namespace MLDB
} // namespace Datacratic
