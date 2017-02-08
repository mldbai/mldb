// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** MLDB-642_script_procedure_test.cc
    Francois Maillet, 10 juillet 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/plugins/script_procedure.h"

#include <chrono>
#include <thread>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/assert.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( script_procedure_test )
{
    MldbServer server;
    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    server.start();
    HttpRestProxy proxy(httpBoundAddress);


    /****
     * python script
     * ****/
    ScriptResource plugRes;
    plugRes.source = R"foo(
print "hoho"
mldb.log(str(mldb.script.args))
mldb.script.set_return("babang!")
)foo";

    ScriptProcedureConfig scriptProcConf;
    scriptProcConf.language = "python";
    scriptProcConf.scriptConfig = plugRes;

    PolyConfig runScriptConfig;
    runScriptConfig.type = "script.run";
    runScriptConfig.params = scriptProcConf;

    auto createProcedureOutput = proxy.put("/v1/procedures/test1",
                                          jsonEncode(runScriptConfig));
    
    cerr << createProcedureOutput << endl;

    BOOST_CHECK_EQUAL(createProcedureOutput.code(), 201);

    /* run with no args */
    {
        ProcedureRunConfig trainProcedureConfig;

        auto trainProcedureOutput = proxy.put("/v1/procedures/test1/runs/1",
                                             jsonEncode(trainProcedureConfig));

        cerr << trainProcedureOutput << endl;

        // Check it was created in the right place
        BOOST_CHECK_EQUAL(trainProcedureOutput.code(), 201);
        BOOST_CHECK_EQUAL(trainProcedureOutput.getHeader("location"),
                          "/v1/procedures/test1/runs/1");

        auto jsBody = trainProcedureOutput.jsonBody();

        cerr << "body from run" << endl;
        cerr << jsBody;

        auto jsDetails
            = proxy.get("/v1/procedures/test1/runs/1/details").jsonBody();

        cerr << "details from run" << endl;
        cerr << jsDetails;

        BOOST_CHECK_EQUAL(jsBody["status"], "babang!");
        BOOST_CHECK_EQUAL(jsDetails["logs"][0]["c"], "hoho");
    }
    
    /* run with args */
    {
        ProcedureRunConfig trainProcedureConfig;
        Json::Value jsVal("make it so!");
        Json::Value args;
        args["args"] = jsVal;
        trainProcedureConfig.params = args;

        auto trainProcedureOutput = proxy.put("/v1/procedures/test1/runs/2",
                                             jsonEncode(trainProcedureConfig));

        cerr << trainProcedureOutput << endl;

        // Check it was created in the right place
        BOOST_CHECK_EQUAL(trainProcedureOutput.code(), 201);
        BOOST_CHECK_EQUAL(trainProcedureOutput.getHeader("location"),
                          "/v1/procedures/test1/runs/2");

        auto jsBody = trainProcedureOutput.jsonBody();

        cerr << "body from run" << endl;
        cerr << jsBody;

        auto jsDetails
            = proxy.get("/v1/procedures/test1/runs/2/details").jsonBody();

        cerr << "details from run" << endl;
        cerr << jsDetails;

        BOOST_CHECK_EQUAL(jsBody["status"], "babang!");
        BOOST_CHECK_EQUAL(jsDetails["logs"][0]["c"], "hoho");
        BOOST_CHECK_EQUAL(jsDetails["logs"][1]["c"], "make it so!");
    }
    
    
    
    /****
     * js script
     * ****/
    plugRes.source = R"foo(
mldb.log("hoho")
mldb.log(plugin.args)
"babang!"
)foo";

    scriptProcConf = ScriptProcedureConfig();
    scriptProcConf.language = "javascript";
    scriptProcConf.scriptConfig = plugRes;

    runScriptConfig = PolyConfig();
    runScriptConfig.type = "script.run";
    runScriptConfig.params = scriptProcConf;

    createProcedureOutput = proxy.put("/v1/procedures/test_js",
                                      jsonEncode(runScriptConfig));
    
    cerr << createProcedureOutput << endl;

    BOOST_CHECK_EQUAL(createProcedureOutput.code(), 201);

    /* run with no args */
    {
        ProcedureRunConfig trainProcedureConfig;
        Json::Value jsVal("engage!");
        Json::Value args;
        args["args"] = jsVal;
        trainProcedureConfig.params = args;

        auto trainProcedureOutput = proxy.put("/v1/procedures/test_js/runs/1",
                                             jsonEncode(trainProcedureConfig));

        cerr << trainProcedureOutput << endl;

        // Check it was created in the right place
        BOOST_CHECK_EQUAL(trainProcedureOutput.code(), 201);
        BOOST_CHECK_EQUAL(trainProcedureOutput.getHeader("location"),
                          "/v1/procedures/test_js/runs/1");

        auto jsBody = trainProcedureOutput.jsonBody();

        cerr << "body from run" << endl;
        cerr << jsBody;

        auto jsDetails
            = proxy.get("/v1/procedures/test_js/runs/1/details").jsonBody();

        cerr << "details from run" << endl;
        cerr << jsDetails;

        BOOST_CHECK_EQUAL(jsBody["status"], "babang!");
        BOOST_CHECK_EQUAL(jsDetails["logs"][0]["c"], "hoho\n");
        BOOST_CHECK_EQUAL(jsDetails["logs"][1]["c"], "engage!\n");
    }
}
