// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include <boost/algorithm/string.hpp>

#include <chrono>
#include <thread>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

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

    // 1.  Run the Python script that implements the example

    Json::Value scriptConfig;
    scriptConfig["address"] = "file://mldb/testing/python_script_test1.py";

    auto output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    BOOST_CHECK_EQUAL(output.code(), 200);

    // Check python script with error
    scriptConfig["address"] = "";
    scriptConfig["source"] = R"foo(
print "hoho"
print datetime.datetime.datime.now()
)foo";
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    BOOST_CHECK_EQUAL(output.code(), 400);
    cout << output << endl;
    auto jsonOutput = output.jsonBody();
    auto excp = jsonOutput["exception"];
    cout << jsonOutput.toStyledString() << endl;
    BOOST_CHECK_EQUAL(excp["message"].asString(), "name 'datetime' is not defined");
    BOOST_CHECK_EQUAL(excp["lineNumber"].asInt(), 3);
    BOOST_CHECK_EQUAL(excp["stack"].size(), 3);
    BOOST_CHECK_EQUAL(excp["stack"][0]["where"].asString().find("Traceback"), 0);
    // make sure we're getting cout
    BOOST_CHECK_EQUAL(jsonOutput["logs"][0]["c"].asString(), "hoho");
    

    pluginConfig2.type = "python";
    plugRes.source.main = scriptConfig["source"].asString();
    pluginConfig2.params = plugRes;
    output = proxy.put("/v1/plugins/plugin_noimport", jsonEncode(pluginConfig2));
    cout << output << endl;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    output = proxy.get("/v1/plugins/plugin_noimport");
    jsonOutput = output.jsonBody();
    cout << jsonOutput.toStyledString() << endl;
    BOOST_CHECK_EQUAL(jsonOutput["progress"]["exception"]["details"]["logs"][0]["c"].asString(), "hoho");



    // Check python script with synthax error
    scriptConfig["address"] = "";
    scriptConfig["source"] = R"foo(
a b
)foo";
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    BOOST_CHECK_EQUAL(output.code(), 400);
    cout << output << endl;
    jsonOutput = output.jsonBody();
    excp = jsonOutput["exception"];
    BOOST_CHECK_EQUAL(excp["lineNumber"].asInt(), 2);
    BOOST_CHECK_EQUAL(excp["stack"].size(), 1);
    BOOST_CHECK_EQUAL(excp["stack"][0]["where"].asString().find("Syntax"), 0);



    // *****************
    // Check python script's return values and argument passing. the script will return what
    // is passed in as an argument
    scriptConfig["source"] = "";
    scriptConfig["address"] = "gist://gist.github.com/mailletf/24fa95ccf5b3b679345b";
        
    Json::Value args;
    args["a"] = 5;
    scriptConfig["args"] = args;

    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cout << output << endl;
    BOOST_CHECK_EQUAL(output.jsonBody()["logs"][0]["c"].asString(), "hoho");
    BOOST_CHECK_EQUAL(output.jsonBody()["result"]["a"].asInt(), 5);
    BOOST_CHECK_EQUAL(output.code(), 200);

    scriptConfig["args"] = Json::Value(5);

    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cout << output << endl;
    BOOST_CHECK_EQUAL(output.jsonBody()["result"].asInt(), 5);
    BOOST_CHECK_EQUAL(output.code(), 200);

    // try the same as before but getting the file from http
    scriptConfig["address"] = "https://gist.githubusercontent.com/mailletf/24fa95ccf5b3b679345b/raw/144930ac7a9cd20478e7ce37139916872928e4c0/main.py";

    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cout << output << endl;
    BOOST_CHECK_EQUAL(output.jsonBody()["result"].asInt(), 5);
    BOOST_CHECK_EQUAL(output.code(), 200);
    
    // try the same as before but getting the file from http
    scriptConfig["address"] = "https://blah";
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cout << output << endl;
    BOOST_CHECK_EQUAL(output.code(), 400);
    
    scriptConfig["address"] = "https://gist.githubusercontent.com/mailletf/24fa95ccf5b3b679345b/raw/144930ac7a9cd20478e7ce37139916872928e4c/main55.py";
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cout << output << endl;
    BOOST_CHECK_EQUAL(output.code(), 400);


    // *****************
    // Check python script does not add extra newlines
    scriptConfig["source"] = R"foo(
print "a\nb"
print "a"

)foo";
    scriptConfig["address"] = "";
 
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cout << output << endl;
    BOOST_CHECK_EQUAL(output.jsonBody()["logs"].size(), 2);
    BOOST_CHECK_EQUAL(output.jsonBody()["logs"][0]["s"].asString(), "stdout");
    BOOST_CHECK_EQUAL(output.jsonBody()["logs"][0]["c"].asString(), "a\nb");
    BOOST_CHECK_EQUAL(output.jsonBody()["logs"][1]["c"].asString(), "a");


    // ************************
    // Check a python plugin that does not exist
    pluginConfig2.type = "python";
    plugRes.source.main = "";
    plugRes.address = "http://blah";
    pluginConfig2.params = plugRes;
    auto putResult = proxy.put("/v1/plugins/pyplugin_noexist",
                               jsonEncode(pluginConfig2));
    cerr << putResult << endl;
    auto jsPutResult = putResult.jsonBody();
    BOOST_CHECK_EQUAL(putResult.code(), 400);

    
    // ************************
    // Check a python plugin that does not compile
    
    plugRes.address = "file://mldb/testing/mldb_py_plugin_nocompile.py";
    pluginConfig2.params = plugRes;
    putResult = proxy.put("/v1/plugins/pyplugin_nocompile",
                               jsonEncode(pluginConfig2));
    cerr << putResult << endl;
    jsPutResult = putResult.jsonBody();
    BOOST_CHECK(jsPutResult["details"]["exception"]["stack"][0]["where"].asString().find("SyntaxError") == 0);
    BOOST_CHECK_EQUAL(putResult.code(), 400);
    auto status = jsonDecode<PolyStatus>(proxy.get("/v1/plugins/pyplugin_nocompile").jsonBody());
    cerr << "status = " << jsonEncode(status) << endl;
    
    
    // Check we don't crash when throwing in the request handler
    plugRes.address = "file://mldb/testing/mldb_py_plugin_requestexc";
    pluginConfig2.params = plugRes;
    putResult = proxy.put("/v1/plugins/pyplugin_requestexc",
                          jsonEncode(pluginConfig2));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 201);

    // this should work
    auto getResult = proxy.get("/v1/plugins/pyplugin_requestexc/routes/pathExists");
    cerr << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 200);

    // this should throw
    getResult = proxy.get("/v1/plugins/pyplugin_requestexc/routes/hello");
    cerr << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 400);


    // ************************
    // Check a python plugin that works properly... works properly
    plugRes.source.main = "";
    plugRes.address = "file://mldb/testing/mldb_py_plugin";
    pluginConfig2.params = plugRes;

    auto putStatus = proxy.put("/v1/plugins/pyplugin",
                          jsonEncode(pluginConfig2));

    // check init output
    getResult = proxy.get("/v1/plugins/pyplugin/routes/lastoutput");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK_EQUAL(getResult.jsonBody()["logs"][0]["c"].asString(), "testing pluging for MLDB!!");

    status = jsonDecode<PolyStatus>(proxy.get("/v1/plugins/pyplugin").jsonBody());
    
    cerr << "status = " << jsonEncode(status) << endl;
    //BOOST_CHECK_EQUAL(jsonEncode(status)["status"]["message"].asString(), "A-OK");
    

    getResult = proxy.get("/v1/plugins/pyplugin/routes/hello");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK_EQUAL(getResult.jsonBody()["how"].asString(), "are you");
    
    // make sure we're getting a 200 return code for empty dict and list
    getResult = proxy.get("/v1/plugins/pyplugin/routes/emptyList");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK(getResult.jsonBody().isArray());
    
    getResult = proxy.get("/v1/plugins/pyplugin/routes/emptyDict");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 404);
//     BOOST_CHECK(getResult.jsonBody().isObject());
    
    // make sure we can return custom return codes
    getResult = proxy.get("/v1/plugins/pyplugin/routes/teaPot");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 418);
    
    // check last output
    getResult = proxy.get("/v1/plugins/pyplugin/routes/lastoutput");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK_EQUAL(getResult.jsonBody()["logs"][0]["c"].asString(), "in route!");
    
    getResult = proxy.get("/v1/plugins/pyplugin/routes/static/static.html");
    cerr << "getResult = " << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_REQUIRE(boost::starts_with(getResult.body(), "OK"));



    // **************************
    // cloning plugin from gist
    // TODO move code to other repo
    plugRes.address = "gist://gist.github.com/mailletf/fc41a2b177e6e66795b5";
    pluginConfig2.params = plugRes;

    putStatus = proxy.put("/v1/plugins/pyplugin_gist",
                               jsonEncode(pluginConfig2));
    cerr << "putStatus: " << putStatus << endl;

    scriptConfig["source"] = "";
    scriptConfig["address"] = "gist://gist.github.com/mailletf/fc41a2b177e6e66795b5";
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cerr << "postStatus: " << output << endl;
    BOOST_CHECK_EQUAL(output.code(), 200);

    scriptConfig["source"] = "";
    scriptConfig["address"] = "gist://gist.github.com/I_DONT_EXIST";
    output = proxy.post("/v1/types/plugins/python/routes/run", scriptConfig);
    cerr << "postStatus: " << output << endl;
    BOOST_CHECK_EQUAL(output.code(), 400);



    // **************************
    // cloning plugin from gist that has two files
    // TODO move code to other repo
    plugRes.address = "gist://gist.github.com/mailletf/acc75d112b35e36aa7b2";
    pluginConfig2.params = plugRes;

    putStatus = proxy.put("/v1/plugins/pyplugin_gist_multifile",
                               jsonEncode(pluginConfig2));
    cerr << "putStatus: " << putStatus << endl;

    getResult = proxy.get("/v1/plugins/pyplugin_gist_multifile/routes/func");
    cerr << getResult << endl;

    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK_EQUAL(getResult.jsonBody()["value"].asString(), "in function");
    
    getResult = proxy.get("/v1/plugins/pyplugin_gist_multifile/version");
    cerr << getResult << endl;
    
    BOOST_CHECK_EQUAL(getResult.jsonBody()["branch"].asString(), "master");



    // ********************************
    //  adsfasdf
    plugRes.address = "file://mldb/testing/local_plugin";
    pluginConfig2.params = plugRes;
    putStatus = proxy.put("/v1/plugins/pyplugin_local_multifile",
                               jsonEncode(pluginConfig2));
    cerr << "putStatus: " << putStatus << endl;

    getResult = proxy.get("/v1/plugins/pyplugin_local_multifile/routes/func");
    cerr << getResult << endl;

    BOOST_CHECK_EQUAL(getResult.code(), 200);
    BOOST_CHECK_EQUAL(getResult.jsonBody()["value"].asString(), "in function");

    getResult = proxy.get("/v1/plugins/pyplugin_local_multifile/routes/doc");
    cerr << getResult << endl;
    BOOST_CHECK_EQUAL(getResult.code(), 404);

    getResult = proxy.get("/v1/plugins/pyplugin_local_multifile/doc/index.html");
    cerr << getResult << endl;
    BOOST_REQUIRE(boost::starts_with(getResult.body(), "My doc!"));
    
    getResult = proxy.get("/v1/plugins/pyplugin_local_multifile/doc/index.md");
    cerr << getResult << endl;
    BOOST_CHECK(getResult.body().find("<h1>My markdown doc") != string::npos);

}
