// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mldb_plugin_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/rest/collection_config_store.h"
#include <boost/filesystem.hpp>
#include <thread>
#include <chrono>


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_plugin_loading )
{
    string configPath = "file://tmp/mldb_config_persistence_test/";
    boost::filesystem::remove_all("./tmp/mldb_config_persistence_test");

    // 1.  Load a plugin and a dataset
    {
        MldbServer server;
        
        server.init(configPath);
        
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
            pluginConfig.persistent = true;
            
            cerr << proxy.put("/v1/plugins/jsplugin",
                              jsonEncode(pluginConfig));
        }
        
        BOOST_CHECK_EQUAL(proxy.get("/v1/plugins").jsonBody(),
                          Json::Value({ Json::Value("jsplugin") }));
        server.shutdown();
    }

    // 2.  Reload it and check it's there
    {
        MldbServer server;
        
        server.init(configPath);
        
        string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
        
        cerr << "http listening on " << httpBoundAddress << endl;

        server.start();
        
        HttpRestProxy proxy(httpBoundAddress);

        // Let it finish initializing.
        for (unsigned i = 0;  i < 10;  ++i) {
            auto res = proxy.perform("GET", "/v1/plugins/jsplugin");
            if (res.jsonBody()["state"] == "initializing") {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }

        BOOST_CHECK_EQUAL(proxy.get("/v1/plugins").jsonBody(),
                          Json::Value({ Json::Value("jsplugin") }));

        auto res = proxy.perform("DELETE", "/v1/plugins/jsplugin");
        cerr << res << endl;
        BOOST_CHECK_EQUAL(res.code(), 204);

        BOOST_CHECK_EQUAL(proxy.get("/v1/plugins").jsonBody(),
                          Json::Value({}));
        
        server.shutdown();
    }

    // 3.  Make sure it was properly erased
    {
        MldbServer server;
        
        server.init(configPath);
        
        string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
        
        cerr << "http listening on " << httpBoundAddress << endl;

        server.start();
        
        HttpRestProxy proxy(httpBoundAddress);

        BOOST_CHECK_EQUAL(proxy.get("/v1/plugins").jsonBody(),
                          Json::Value({}));
    }
}
