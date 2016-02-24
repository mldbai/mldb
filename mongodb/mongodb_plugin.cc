/* mongodb_plugin.cc
   Jeremy Barnes, 23 February 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"

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


Datacratic::MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server)
{
    return nullptr;
}

namespace Datacratic {
namespace MLDB {

const Package & mongodbPackage()
{
    static const Package result("mongodb");
    return result;
}

struct MldbMongoLogger: public mongocxx::logger {
    virtual void operator()(mongocxx::log_level level, mongocxx::stdx::string_view domain,
                            mongocxx::stdx::string_view message) noexcept
    {
        cerr << "mongodb log " << (int)level << " " << domain << " "
             << message << endl;
    }
};

mongocxx::instance inst{std::unique_ptr<MldbMongoLogger>(new MldbMongoLogger())};


/*****************************************************************************/
/* MONGO DATASET                                                             */
/*****************************************************************************/

struct MongoDatasetConfig {
};

DECLARE_STRUCTURE_DESCRIPTION(MongoDatasetConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoDatasetConfig);

MongoDatasetConfigDescription::
MongoDatasetConfigDescription()
{
}

struct MongoDataset: public Dataset {

    MongoDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Dataset(owner)
    {
    }
    
    virtual ~MongoDataset()
    {
    }

    virtual Any getStatus() const
    {
        return string("ok");
    }

    virtual void recordRowItl(const RowName & rowName,
                              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
    }
    
    virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
    {
    }

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit()
    {
    }

    virtual std::pair<Date, Date> getTimestampRange() const
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    virtual Date quantizeTimestamp(Date timestamp) const
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    virtual std::shared_ptr<RowStream> getRowStream() const
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        throw HttpReturnException(400, "Mongo dataset is record-only");
    }
};


/*****************************************************************************/
/* MONGO IMPORT PROCEDURE                                                    */
/*****************************************************************************/

struct MongoImportConfig: public ProcedureConfig {
};

DECLARE_STRUCTURE_DESCRIPTION(MongoImportConfig);
DEFINE_STRUCTURE_DESCRIPTION(MongoImportConfig);

MongoImportConfigDescription::
MongoImportConfigDescription()
{
    addParent<ProcedureConfig>();
}

struct MongoImportProcedure: public Procedure {
    MongoImportProcedure(MldbServer * server,
                         const PolyConfig & config,
                         std::function<bool (Json::Value)> onProgress)
        : Procedure(server)
    {
    }

    virtual Any getStatus() const
    {
        Json::Value result;
        result["ok"] = true;
        return result;
    }

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        cerr << "importing mongodb dataset" << endl;

        mongocxx::client conn{mongocxx::uri{"mongodb://localhost:27017"}};

        auto db = conn["test"];

        cerr << "connected to " << db.name() << endl;

        // Query for all the documents in a collection.
        {
            auto cursor = db["restaurants"].find({});
            for (auto&& doc : cursor) {
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

static RegisterDatasetType<MongoDataset, MongoDatasetConfig>
regMongodbDataset(mongodbPackage(),
                 "mongodb.record",
                 "Dataset type that forwards records to a mongodb database",
                 "MongoRecord.md.html");

static RegisterProcedureType<MongoImportProcedure, MongoImportConfig>
regMongodbImport(mongodbPackage(),
                 "mongodb.import",
                 "Import a dataset from mongodb",
                 "MongoImport.md.html");

} // namespace MLDB
} // namespace Datacratic
