// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** MLDB-204-circular-references-initialization.cc
    Jeremy Barnes, 19 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/utils/testing/watchdog.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_simple_recursion )
{
    MldbServer server;
    
    ML::Watchdog watchdog(5);

    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    server.start();
    HttpRestProxy proxy(httpBoundAddress);

    auto functionConfig = jsonDecodeStr<PolyConfig>(string("{\"id\": \"circular\", \"type\": \"serial\",\"params\": { \"steps\": [ { \"id\": \"circular\" } ] } }"));

    auto putResult = proxy.put("/v1/functions/circular?sync=true",
                               jsonEncode(functionConfig));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 400);
}

BOOST_AUTO_TEST_CASE( test_mutual_recursion )
{
    MldbServer server;
    
    ML::Watchdog watchdog(5);

    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    server.start();
    HttpRestProxy proxy(httpBoundAddress);

    auto functionConfig = jsonDecodeStr<PolyConfig>(string("{\"id\": \"circular1\", \"type\": \"serial\",\"params\": { \"steps\": [ { \"type\": \"serial\", \"id\": \"circular2\", \"params\": { \"steps\": [ { \"id\": \"circular1\" } ] } } ] } }"));

    auto putResult = proxy.put("/v1/functions/circular1?sync=true",
                               jsonEncode(functionConfig));
    cerr << putResult << endl;

    BOOST_CHECK_EQUAL(putResult.code(), 400);
}
