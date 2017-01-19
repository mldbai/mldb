// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mldb_plugin_delete_test.cc
  Francois Maillet, 18 mars 2015  
  Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_plugin_loading )
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
    plugRes.source.routes = R"foo(
print "Handling route in python"
rp = mldb.plugin.rest_params
if rp.verb == "GET" and rp.remaining == "/miRoute":
    mldb.plugin.set_return("bouya!")
)foo";
    pluginConfig.params = plugRes;
 
    auto putResult = proxy.put("/v1/plugins/myplugin",
                               jsonEncode(pluginConfig));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 201);


    auto getResult = proxy.get("/v1/plugins/myplugin/routes/miRoute");
    cout << getResult << endl;

    // delete it!
    auto delResult = proxy.perform("DELETE", "/v1/plugins/myplugin");
    cout << delResult << endl;

    // does the plugin still exist??
    getResult = proxy.get("/v1/plugins");
    BOOST_CHECK_EQUAL(getResult.jsonBody().size(), 0);

    getResult = proxy.get("/v1/plugins/myplugin/routes/miRoute");
    cout << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 404);

    // recreate!|
    plugRes.source.routes = R"foo(
print "Handling route in python"
rp = mldb.plugin.rest_params
if rp.verb == "GET" and rp.remaining == "/miRoute":
    mldb.plugin.set_return("bouya2!")
)foo";
    pluginConfig.params = plugRes;
    putResult = proxy.put("/v1/plugins/myplugin",
                               jsonEncode(pluginConfig));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 201);

    getResult = proxy.get("/v1/plugins/myplugin/routes/miRoute");
    cout << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.jsonBody().asString(), "bouya2!");


    // delete it!
    delResult = proxy.perform("DELETE", "/v1/plugins/myplugin");
    cout << "delResult 2:" << delResult << endl;

    // create a plugin with a syntax error
    plugRes.source.routes = R"foo(
print "Handling route in python"
rp = mldb.plugin.rest_params
if rp.verb == "GET" and rp.remaining == "/miRoute":
    mldb.plugin.set_return("bouya2!")

)foo";
   pluginConfig.params = plugRes;
    putResult = proxy.put("/v1/plugins/myplugin",
                               jsonEncode(pluginConfig));
    cerr << putResult << endl;
    // delete it!
    delResult = proxy.perform("DELETE", "/v1/plugins/myplugin");
    cout << delResult << endl;
//    cout << "code : " << delResult.code() << endl;
    BOOST_CHECK_EQUAL(delResult.code(), 204);

}
