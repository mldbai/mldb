// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

 /** mldb_plugin_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

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
    for (unsigned i = 0;  i < 1000;  ++i) {

        MldbServer server;
    
        server.init();

        string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
        cerr << "http listening on " << httpBoundAddress << endl;

        server.start();

        HttpRestProxy proxy(httpBoundAddress);

        proxy.get("");

        server.shutdown();
    }
}
