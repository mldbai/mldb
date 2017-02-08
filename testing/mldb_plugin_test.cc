/** mldb_plugin_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include <signal.h>


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_plugin_loading )
{
    signal(SIGSEGV, SIG_DFL);
    signal(SIGABRT, SIG_DFL);

    MldbServer server;

    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");

    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    PluginResource plugRes;

    PolyConfig pluginConfig2;
    pluginConfig2.type = "javascript";
    plugRes.address = "file://mldb/testing/mldb_js_plugin_nocompile.js";
    pluginConfig2.params = plugRes;
 
    auto putResult = proxy.put("/v1/plugins/jsplugin_nocompile",
                               jsonEncode(pluginConfig2));
    cerr << putResult << endl;

    auto jsPutResult = putResult.jsonBody();
    cerr << "jsPutResult = " << jsPutResult << endl;
    BOOST_CHECK_EQUAL(jsPutResult["details"]["message"].asString(), "Uncaught SyntaxError: Unexpected identifier");

    BOOST_CHECK_EQUAL(putResult.code(), 400);

    auto status = jsonDecode<PolyStatus>(proxy.get("/v1/plugins/jsplugin_nocompile").jsonBody());

    cerr << "status = " << jsonEncode(status) << endl;



    // Check that we handle throwing in the status callback properly
    plugRes.address = "file://mldb/testing/mldb_js_plugin_statusexc.js";
    pluginConfig2.params = plugRes;
    putResult = proxy.put("/v1/plugins/jsplugin_statusexc",
                          jsonEncode(pluginConfig2));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 400);


    //status = jsonDecode<PolyStatus>(proxy.get("/v1/plugins/jsplugin_statusexc").jsonBody());
    //
    //cerr << "status = " << jsonEncode(status) << endl;


    // Check we don't crash when throwing in the request handler
    plugRes.address = "file://mldb/testing/mldb_js_plugin_requestexc.js";
    pluginConfig2.params = plugRes;
    putResult = proxy.put("/v1/plugins/jsplugin_requestexc",
                          jsonEncode(pluginConfig2));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 201);

    auto getResult = proxy.get("/v1/plugins/jsplugin_requestexc/routes/hello");

    cerr << getResult << endl;

    BOOST_CHECK_EQUAL(getResult.code(), 400);



    // Check a plugin that works properly... works properly
    plugRes.address = "file://mldb/testing/mldb_js_plugin";
    pluginConfig2.params = plugRes;

    auto putStatus = proxy.put("/v1/plugins/jsplugin",
                               jsonEncode(pluginConfig2));
    
    status = jsonDecode<PolyStatus>(proxy.get("/v1/plugins/jsplugin").jsonBody());
    
    cerr << "status = " << jsonEncode(status) << endl;

    BOOST_CHECK_EQUAL(jsonEncode(status)["status"]["message"].asString(), "A-OK");
    
    getResult = proxy.get("/v1/plugins/jsplugin/routes/hello");

    cerr << "getResult = " << getResult << endl;

    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK_EQUAL(getResult.jsonBody()["how"].asString(), "are you");

    getResult = proxy.get("/v1/plugins/jsplugin/routes/static/index.html");
    BOOST_CHECK_EQUAL(getResult.code(), 200);

    auto body = getResult.body();
    BOOST_CHECK_EQUAL(body.substr(0, body.length() -1), "patate");

}
