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
    
    PolyConfig config;
    config.type = "testfunction";

    // create a function, which will increment tte counter to 1
    auto out = proxy.put("/v1/functions/myfunction", jsonEncode(config));
    cout << out << endl;

    out = proxy.get("/v1/functions/myfunction/application?input={}");
    cout << out << endl;
    
    BOOST_CHECK_EQUAL(out.jsonBody()["output"]["cnt"].asInt(), 1);
    
    // create another function, which will increment hte counter to 2
    out = proxy.put("/v1/functions/myfunctionbiz", jsonEncode(config));
    cout << out << endl;

    out = proxy.get("/v1/functions/myfunction/application?input={}");
    cout << out << endl;
    BOOST_CHECK_EQUAL(out.jsonBody()["output"]["cnt"].asInt(), 2);
    out = proxy.get("/v1/functions/myfunctionbiz/application?input={}");
    BOOST_CHECK_EQUAL(out.jsonBody()["output"]["cnt"].asInt(), 2);


    // delete a function, which should call the destructor, decrementing the counter
    out = proxy.perform("DELETE", "/v1/functions/myfunctionbiz");
    cout << out << endl;

    out = proxy.get("/v1/functions/myfunction/application?input={}");
    cout << out << endl;
    BOOST_CHECK_EQUAL(out.jsonBody()["output"]["cnt"].asInt(), 1);

    //     print conn.delete("/functions/explainFunction")

}
