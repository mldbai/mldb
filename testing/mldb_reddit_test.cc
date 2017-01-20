// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mldb_dataset_test.cc                                           -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Test for datasets.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"


using namespace std;

using namespace MLDB;

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    MldbServer server;
    
    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "0.0.0.0");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    // 1.  Add the JS script runner

    {
        PolyConfig pluginConfig;
        pluginConfig.type = "javascript_runner";
        
        cerr << proxy.put("/v1/plugins/jsrunner",
                          jsonEncode(pluginConfig));
    }

    // 2.  Run the JS script that implements the example


    {
        PolyConfig pluginConfig;
        pluginConfig.type = "javascript";

        PluginResource plugRes;
        plugRes.address = "file://mldb/mldb/testing/reddit_example.js";

        pluginConfig.params = plugRes;

        cerr << proxy.put("/v1/plugins/reddit",
                          jsonEncode(pluginConfig));
    }

    // 3.  Get some coordinates out of a merged dataset, and check that they are all
    //     there

    auto getResult = proxy.get("/v1/query?q=select rowName(),embedding.\"0\",embedding.\"1\",cluster,x,y from reddit_embeddings limit 100").jsonBody();

    BOOST_CHECK_EQUAL(getResult.size(), 100);

    for (auto & r: getResult) {
        cerr << "r = " << r << endl;
        BOOST_CHECK_EQUAL(r["columns"].size(), 6);
        BOOST_CHECK_EQUAL(r["columns"][1][0], "embedding.0");
        BOOST_CHECK_EQUAL(r["columns"][2][0], "embedding.1");
        BOOST_CHECK_EQUAL(r["columns"][0][0], "cluster");
        BOOST_CHECK_EQUAL(r["columns"][4][0], "x");
        BOOST_CHECK_EQUAL(r["columns"][5][0], "y");
        BOOST_CHECK(r["columns"][1][1].isNumeric());
        BOOST_CHECK(r["columns"][2][1].isNumeric());
        BOOST_CHECK(r["columns"][0][1].isIntegral());
        BOOST_CHECK(r["columns"][4][1].isNumeric());
        BOOST_CHECK(r["columns"][5][1].isNumeric());
    }
}
