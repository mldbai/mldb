// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** bamboohr_dataset.cc
    Jeremy Barnes, 9 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Implementation of bamboohr dataset.
*/

#include "bamboohr_dataset.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/mldb_server.h"
#include "mldb/utils/log.h"
#include "mldb/base/hash.h"
#include "tabular_dataset.h"

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* BAMBOOHR DATASET CONFIG                                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(BambooHrDatasetConfig);

BambooHrDatasetConfigDescription::
BambooHrDatasetConfigDescription()
{
    addField("apiKey", &BambooHrDatasetConfig::apiKey,
             "API key for BambooHR API access");
    addField("subdomain", &BambooHrDatasetConfig::subdomain,
             "BambooHR subdomain used for access");
}

struct BambooFieldInfo {
    std::string id;
    Utf8String name;
    std::string type;
};

DECLARE_STRUCTURE_DESCRIPTION(BambooFieldInfo);
DEFINE_STRUCTURE_DESCRIPTION(BambooFieldInfo);

BambooFieldInfoDescription::BambooFieldInfoDescription()
{
    addField("id", &BambooFieldInfo::id, "");
    addField("name", &BambooFieldInfo::name, "");
    addField("type", &BambooFieldInfo::type, "");
}

struct BambooEmployeeRecord {
    Path id;
    std::map<Path, CellValue> fields;
};

DECLARE_STRUCTURE_DESCRIPTION(BambooEmployeeRecord);

DEFINE_STRUCTURE_DESCRIPTION(BambooEmployeeRecord);

BambooEmployeeRecordDescription::
BambooEmployeeRecordDescription()
{
    addField("id", &BambooEmployeeRecord::id, "");
    addField("fields", &BambooEmployeeRecord::fields, "");

    this->onUnknownField = [=] (BambooEmployeeRecord * obj,
                                JsonParsingContext & context)
        {
            CellValue val = jsonDecode<CellValue>(context.expectJson());
            obj->fields[Path(context.fieldNamePtr())]
                = std::move(val);
            return true;
        };
}

struct BambooEmployeeResponse {
    std::vector<BambooEmployeeRecord> employees;
    std::vector<BambooFieldInfo> fields;
};

DECLARE_STRUCTURE_DESCRIPTION(BambooEmployeeResponse);
DEFINE_STRUCTURE_DESCRIPTION(BambooEmployeeResponse);

BambooEmployeeResponseDescription::
BambooEmployeeResponseDescription()
{
    addField("employees", &BambooEmployeeResponse::employees, "");
    addField("fields", &BambooEmployeeResponse::fields, "");
}


/*****************************************************************************/
/* BAMBOOHR INTERNAL REPRESENTATION                                        */
/*****************************************************************************/

struct BambooHrDataset::Itl {
    Itl(BambooHrDataset * owner,
        MldbServer * server,
        const BambooHrDatasetConfig & config)
        : owner(owner),
          server(server),
          logger(MLDB::getMldbLog<BambooHrDataset>())
    {
        initRoutes();

        client.init("https://api.bamboohr.com/api/gateway.php/" + config.subdomain + "/v1");
        
        auth = "Basic " + base64Encode(config.apiKey + ":x");

        load();

#if 0

        auto response = client.get("/meta/fields",
                                   {}, // no query string
                                   { { "Accept", "application/json" },
                                     { "Authorization", auth} });

#endif

        //cerr << response.jsonBody() << endl;
                                      
    }

    ~Itl()
    {
    }

    BambooHrDataset * owner;
    MldbServer * server;

    RestRequestRouter router;

    HttpRestProxy client;
    std::string auth;

    void load()
    {
        TabularDatasetConfig tabularConfig;
        tabularConfig.unknownColumns = UC_ADD;  // add new columns

        PolyConfig config;
        config.type = "tabular";
        config.params = tabularConfig;

        auto dataset
            = std::make_shared<TabularDataset>(server, config, nullptr);

        auto response = client.get("/employees/directory",
                                   {}, // no query string
                                   { { "Accept", "application/json" },
                                     { "Authorization", auth} });
        
        //auto json = response.jsonBody();

        BambooEmployeeResponse responseParsed
            = jsonDecodeStr<BambooEmployeeResponse>(response.body());

        for (auto & e: responseParsed.employees) {

            cerr << e.id << endl;

#if 0
            auto employee = client.get("/employees/" + e.id.toUtf8String().rawString(),
                                       {},
                                       { { "Accept", "application/json" },
                                         { "Authorization", auth} });

            cerr << employee << endl;

            cerr << employee.jsonBody() << endl;
#endif

            Date ts = Date::now();
                                       

            std::vector<std::tuple<Path, CellValue, Date> > values;

            for (auto & f: e.fields) {
                values.emplace_back(std::move(f.first),
                                    std::move(f.second),
                                    ts);
            }

            dataset->recordRowExpr(std::move(e.id),
                                   std::move(values));
        }

        dataset->commit();

        owner->setUnderlying(std::move(dataset));
    }

    void initRoutes()
    {
    }

    Any
    getStatus() const
    {
        Json::Value result;
        return result;
    }

    shared_ptr<spdlog::logger> logger;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return router.processRequest(connection, request, context);
    }

    std::shared_ptr<ColumnIndex>
    getColumnIndex() const
    {
        throw HttpReturnException(600, "getColumnIndex");
    }

    std::shared_ptr<RowStream> 
    getRowStream() const
    {
        throw HttpReturnException(600, "getRowStream");
    }

    std::pair<Date, Date>
    getTimestampRange() const
    {
        throw HttpReturnException(600, "getTimestampRange");
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        throw HttpReturnException(600, "getRowPaths");
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        throw HttpReturnException(600, "getRowHashes");
    }

    virtual size_t getRowCount() const
    {
        throw HttpReturnException(600, "getRowCount");
    }

    virtual bool knownRow(const RowPath & row) const
    {
        throw HttpReturnException(600, "knownRow");
    }
    
    virtual MatrixNamedRow getRow(const RowPath & row) const
    {
        throw HttpReturnException(600, "getRow");
    }

    virtual RowPath getRowPath(const RowHash & row) const
    {
        throw HttpReturnException(600, "getRowPath");
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        throw HttpReturnException(600, "knownColumn");
    }
    
    virtual ColumnPath getColumnPath(ColumnHash column) const
    {
        throw HttpReturnException(600, "getColumnPath");
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnPaths() const
    {
        throw HttpReturnException(600, "getColumPaths");
    }

    /** Return the number of distinct columns known. */
    virtual size_t getColumnCount() const
    {
        throw HttpReturnException(600, "getColumnCount");
    }

};


/*****************************************************************************/
/* BAMBOOHR DATASET                                                        */
/*****************************************************************************/

BambooHrDataset::
BambooHrDataset(MldbServer * owner,
                PolyConfig config,
                const ProgressFunc & onProgress)
    : ForwardedDataset(owner)
{
    datasetConfig = config.params.convert<BambooHrDatasetConfig>();
    itl.reset(new Itl(this, owner, datasetConfig));
}
    
BambooHrDataset::
~BambooHrDataset()
{
}

#if 0
Any
BambooHrDataset::
getStatus() const
{
    return itl->getStatus();
}

std::pair<Date, Date>
BambooHrDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
BambooHrDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
BambooHrDataset::
getColumnIndex() const
{
    return itl->getColumnIndex();
}

std::shared_ptr<RowStream> 
BambooHrDataset::
getRowStream() const
{
    return itl->getRowStream();
}

#endif

#if 0
RestRequestMatchResult
BambooHrDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    return itl->handleRequest(connection, request, context);
}
#endif

static RegisterDatasetType<BambooHrDataset, BambooHrDatasetConfig>
regBambooHr(builtinPackage(),
              "bamboohr",
              "Dataset that represents the current state of Bamboo HR",
              "datasets/BambooHrDataset.md.html",
               nullptr,
               {MldbEntity::INTERNAL_ENTITY});

} // namespace MLDB
