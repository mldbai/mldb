// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mldb_collection_delete_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include <thread>
#include <chrono>


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_plugin_delete_while_loading )
{
    MldbServer server;
        
    server.init();
        
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
        
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();
        
    HttpRestProxy proxy(httpBoundAddress);
        
    // 1.  Add the JS script runner
        
    {
        PluginResource res;
        res.source.main = "'hello'";

        PolyConfig pluginConfig;
        pluginConfig.type = "javascript";
        pluginConfig.params = res;
            
        cerr << proxy.put("/v1/plugins/jsplugin", jsonEncode(pluginConfig));
    }
        
    auto res = proxy.perform("DELETE", "/v1/plugins/jsplugin");
    cerr << res << endl;
    BOOST_CHECK_EQUAL(res.code(), 204);

    cerr << proxy.get("/v1/plugins/jsplugin");

    BOOST_CHECK_EQUAL(proxy.get("/v1/plugins").jsonBody(),
                      Json::Value({}));
        
    server.shutdown();
}
