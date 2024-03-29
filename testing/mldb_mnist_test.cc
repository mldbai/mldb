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
#include "mldb/builtin/plugin_resource.h"
#include "mldb/types/value_description.h"
#include <thread>
#include <chrono>


using namespace std;

using namespace MLDB;

int main(int argc, char ** argv)
{
    MldbServer server;
    
    cerr << "argc = " << argc << endl;
    if (argc > 1)
        cerr << "argv[1] = " << argv[1] << endl;

    bool listenForever = (argc > 1 && argv[1] == string("--listen-forever"));

    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    // 1.  Run the JS script that implements the example

    {
        PolyConfig pluginConfig;
        pluginConfig.type = "javascript";

        PluginResource plugRes;
        plugRes.address = "file://pro/testing/mnist_example.js";

        pluginConfig.params = plugRes;

        cerr << proxy.put("/v1/plugins/mnist",
                          jsonEncode(pluginConfig));
    }

    //cerr << proxy.get("/v1/plugins/mnist/routes/static/mnist.html");

    if (!listenForever)
        return 0;

    cerr << "GET "
         << httpBoundAddress
         << "/v1/plugins/mnist/routes/static/mnist.html"
         << " to connect to UI" << endl;

    for (;;) {
        std::this_thread::sleep_for(std::chrono::seconds(100));
    }
}
