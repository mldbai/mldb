// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** 
  pyplugin_static_folder_test.cc
  Francois Maillet, 19 mars 2015  
  Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include <boost/algorithm/string.hpp>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/algorithm/string.hpp>
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
    plugRes.address = "file://mldb/testing/pyplugin_static_folder";

    int cnt = 0;
    auto testCase = [&] (const string & arg1, const string & arg2, string path = "static/a.html")
    {
        cout << "==== Trying case " << (++cnt) << endl;
        Json::Value args(Json::ValueType::arrayValue);
        args.append(arg1);
        args.append(arg2);
        plugRes.args = args;
        pluginConfig.params = plugRes;

        auto putResult = proxy.put("/v1/plugins/myplugin",
                                   jsonEncode(pluginConfig));
        cerr << putResult << endl;
        BOOST_CHECK_EQUAL(putResult.code(), 201);

        // can we fetch the static content?
        auto getResult = proxy.get("/v1/plugins/myplugin/routes/"+path);
        BOOST_CHECK(boost::starts_with(getResult.body(), "a"));

        auto delResult = proxy.perform("DELETE", "/v1/plugins/myplugin");
        BOOST_CHECK_EQUAL(delResult.code(), 204);
    };


    testCase("/static", "static");
    testCase("static",  "static");
    testCase("static",  "./static");
    testCase("static/", "static");
    testCase("bouya", ".", "bouya/static/a.html");
    testCase("bouya", "./", "bouya/static/a.html");



    // check invalid configp
    auto failTestCase = [&] (const string & arg1, const string & arg2)
    {
        Json::Value args(Json::ValueType::arrayValue);
        args.append(arg1);
        args.append(arg2);
        plugRes.args = args;
        pluginConfig.params = plugRes;

        auto putResult = proxy.put("/v1/plugins/myplugin",
                                   jsonEncode(pluginConfig));
        cout << putResult << endl;
        BOOST_CHECK_EQUAL(putResult.jsonBody()["details"]["exception"]["message"].asString(), 
                          "Route and static directory cannot be empty for serving static folder");

        auto delResult = proxy.perform("DELETE", "/v1/plugins/myplugin");
    };

    failTestCase("", ".");
    failTestCase(".", "");
    failTestCase("", "");
}
