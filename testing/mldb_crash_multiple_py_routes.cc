// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/base/parallel.h"

#include <chrono>
#include <thread>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_python_loading )
{
    MldbServer server;
    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    server.start();
    HttpRestProxy proxy(httpBoundAddress);

    PluginResource plugRes;
    PolyConfig pluginConfig2;


    // ************************
    // Check a python plugin that does not exist
    pluginConfig2.type = "python";
    plugRes.address = "git://github.com/datacratic/mldb-cls-plugin.git";
    pluginConfig2.params = plugRes;

    auto putResult = proxy.put(MLDB::format("/v1/plugins/cls%d", 0),
                               jsonEncode(pluginConfig2));
    cerr << putResult << endl;
    auto jsPutResult = putResult.jsonBody();
    BOOST_CHECK_EQUAL(putResult.code(), 201);

    /******
     * Try to crash MLDB by calling multiple python custom routes at once
     * ****/

    vector<string> calls;
    int j = 0;
    string url = MLDB::format("curl %s/v1/plugins/cls%d", httpBoundAddress, j);
    for(int i=0; i<500; i++) {
        calls.push_back(url + "/routes/dataset-details");
        calls.push_back(url + "/routes/classifier-list");
    }

    std::atomic<int> pwet(0);
    auto doCall = [&] (int i)
        {
            int rtn = system(calls[i].c_str());
            if(rtn==0)
                cout << "wahou";
        };
   parallelMap(0, calls.size(), doCall);
}
