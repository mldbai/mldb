// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** mldb_dataset_test.cc                                           -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Test for datasets.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/plugins/svd.h"
#include "mldb/plugins/sql_functions.h"
#include "mldb/http/http_rest_proxy.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    MldbServer server;
    
    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    PersistentDatasetConfig params;
    params.dataFileUrl = Url("file://airlines.beh");

    PolyConfig datasetConfig;
    datasetConfig.type = "beh";
    datasetConfig.params = params;
    
    auto createResult = proxy.put("/v1/datasets/airlines?sync=true",
                                  jsonEncode(datasetConfig));
    
    cerr << createResult << endl;

    // If we don't have the dataset, we can't do anything
    if (createResult.code() == 400)
        return;

    cerr << proxy.get("/v1/datasets/airlines/query",
                      { { "select", "aircrafttype, status, avg(vrate), avg(latitude), avg(longtitude), avg(altitude), min(latitude), max(latitude), min(longtitude), max(longtitude), count(1) AS cnt" },
                        { "where", "longtitude > 90" },
                        { "groupBy", "aircrafttype, status" },
                        { "orderBy", "cnt DESC" },
                        { "limit", "100" } }).jsonBody();
                                  
        //query = "SELECT aircrafttype, status, COUNT(*) FROM " + dataset.key + " WHERE status = 'Approach' OR status = 'Landing' GROUP BY aircrafttype, status ORDER BY count(*) DESC";

    // Get a  list of all columns
    auto columns = proxy.get("/v1/datasets/airlines/columns").jsonBody();
    Json::Value expectedColumns = { "groundspeed","longtitude","altitude","registration","modes","aircrafttype","status","latitude","track","vrate" };
    BOOST_CHECK_EQUAL(columns, expectedColumns);

    columns = proxy.get("/v1/datasets/airlines/columns?limit=2").jsonBody();
    expectedColumns = { "groundspeed","longtitude" };
    BOOST_CHECK_EQUAL(columns, expectedColumns);

    columns = proxy.get("/v1/datasets/airlines/columns?limit=2&offset=2").jsonBody();
    expectedColumns = { "altitude","registration" };
    BOOST_CHECK_EQUAL(columns, expectedColumns);

    // Get a list of columns 

    auto aircraftTypes = proxy.get("/v1/datasets/airlines/columns/aircrafttype/values");
    cerr << aircraftTypes << endl;

    auto aircraftCounts = proxy.get("/v1/datasets/airlines/columns/aircrafttype/valuecounts");
    cerr << aircraftCounts << endl;

    BOOST_CHECK_EQUAL(aircraftTypes.jsonBody().size(), aircraftCounts.jsonBody().size());

    return;


                                      
    // Find the distinct aircraft types
    auto aircraft = proxy.get("/v1/datasets/airlines/columns/aircrafttype/values");
    cerr << aircraft << endl;

    auto rows = proxy.get("/v1/datasets/airlines/rows?limit=10").jsonBody();
    cerr << rows << endl;

    //cerr << proxy.get("/v1/datasets/airlines/columns/status/values");

    SvdConfig svdConfig;
    svdConfig.dataset = TableExpression::parse(Utf8String("airlines"));

    PolyConfig procedureConfig;
    procedureConfig.type = "svd.train";
    procedureConfig.params = svdConfig;

    cerr << proxy.put("/v1/procedures/airlines_svd?sync=true",
                      jsonEncode(procedureConfig));
    
    // now train the SVD

    ProcedureRunConfig trainingConfig;

    //cerr << proxy.put("/v1/procedures/airlines_svd/runs/one?sync=true",
    //                  jsonEncode(trainingConfig));

    
    // Embed each aircraft type

#if 0
    LookupRowConfig lookupConfig;
    lookupConfig.datasetName = "airlines";

    SvdEmbedConfig embedConfig;
    embedConfig.modelFileUrl = "file://svd.json.gz";

    SerialFunctionConfig functions;
    functions.steps.emplace_back(makeStepConfig("dataset.lookupRow", lookupConfig));
    functions.steps.emplace_back(makeStepConfig("svd.embedRow", embedConfig));
    //functions.add("dataset.lookupRow", LookupRowConfig);
    //functions.add("svd.embedColumn", SvdEmbedConfig);


    // Goal: create a dataset with an SVD embedding for each aircraft type


    PolyConfig functionConfig;
    functionConfig.type = "serial";
    functionConfig.params = functions;

    cerr << "functionConfig = " << jsonEncode(functionConfig) << endl;
    
    cerr << proxy.put("/v1/functions/embedRows",
                      jsonEncode(makeFunctionConfig("serial", functions)));
    
    for (auto r: rows) {
        cerr << proxy.get("/v1/functions/embedRows/apply",
                          { { "encoding", "json" },
                            { "rowName", r.toString() },
                            { "keepPins", "embedding" } });
    }
#endif    

#if 0    

    // Finally, run them
    PolyConfig applyProcedureConfig;
    applyProcedureConfig.type = "dataset.applyFunctionToRows";
    applyProcedureConfig.params = applyFunctionToRowsConfig;

#endif
    // Find distinct aircraft types

    // Run t-SNE to lay them out
    

    // Run
    
}
