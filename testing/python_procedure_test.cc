// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** python_procedure_test.cc                                           -*- C++ -*-
    Francois Maillet, 9 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/core/procedure.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    MldbServer server;
    
    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    server.start();
    HttpRestProxy proxy(httpBoundAddress);


    PolyConfig pluginConfig;
    pluginConfig.type = "python";
    
    PluginResource plugRes;
    plugRes.source.main = R"foo(

def doTrain(mldb, trainingConfig):
    mldb.log(str(trainingConfig))
    print "wahou"
    return {"status": "OK"}

mldb.create_procedure("my_procedure", "description of my procedure", doTrain)
print "pwet";

)foo";
    pluginConfig.params = plugRes;
 
    auto putResult = proxy.put("/v1/plugins/myplugin",
                               jsonEncode(pluginConfig));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 201);


    // Check procedure was added successfully
    auto getResult = proxy.get("/v1/types/procedures");
    cerr << "getResult = " << getResult << endl;
    BOOST_REQUIRE(getResult.body().find("my_procedure") != string::npos);

//     BOOST_CHECK_EQUAL(getResult.jsonBody()["how"].asString(), "are you");


    Json::Value training;
    training["id"] = "my_procedure_train";
    training["type"] = "my_procedure";

    Json::Value customConf;
    customConf["param"] = 5;
    training["params"] = customConf;

    putResult = proxy.put("/v1/procedures/my_procedure_train",
            jsonEncode(training));

    cerr << putResult << endl;
    BOOST_CHECK_EQUAL(putResult.code(), 201);

    ProcedureRunConfig trainConf;
    trainConf.id = "1";

    putResult = proxy.put("/v1/procedures/my_procedure_train/runs/1", jsonEncode(trainConf));
    cerr << putResult << endl;
    BOOST_CHECK_EQUAL(putResult.code(), 201);



}
