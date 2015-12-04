// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* mldb_sql_test.cc                                                -*- C++ -*-
   Jeremy Barnes, 24 December 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Test of using SQL with MLDB.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/plugins/svd.h"
#include "mldb/http/http_rest_proxy.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    MldbServer server;
    
    server.init(PortRange(18000, 19000), "127.0.0.1");

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    // First, load the SQL plugin
    PolyConfig pluginConfig;
    pluginConfig.type = "sql";

    cerr << proxy.put("/v1/plugins/sql?sync=true",
                      jsonEncode(pluginConfig));
    
    // Now load up a dataset
    PolyConfig datasetConfig;
    datasetConfig.type = "beh";
    datasetConfig.address = "file://airlines.beh";
    
    cerr << proxy.put("/v1/datasets/airlines?sync=true",
                      jsonEncode(datasetConfig));

    cerr << "getting tables" << endl;
    cerr << proxy.get("/v1/plugins/sql/tables");
}
