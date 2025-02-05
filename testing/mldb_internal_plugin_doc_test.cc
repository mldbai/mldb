// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mldb_plugin_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/engine/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/builtin/plugin_resource.h"
#include "mldb/utils/string_functions.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#define xstr(a) xxstr(a)
#define xxstr(a) #a

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_plugin_loading )
{
    MldbServer server;

    server.init();

    // Load plugins, so we can also test them for documentation
    server.scanPlugins("file://build/" xstr(MLDB_ARCH) "-" xstr(MLDB_OSNAME) "/mldb_plugins/tensorflow");
    server.scanPlugins("file://build/" xstr(MLDB_ARCH) "-" xstr(MLDB_OSNAME) "/mldb_plugins/postgresql");
    server.scanPlugins("file://build/" xstr(MLDB_ARCH) "-" xstr(MLDB_OSNAME) "/mldb_plugins/mongodb");
    
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    HttpRestProxy proxy(httpBoundAddress);

    // For each instance of each plugin, we try to get the documentation
    for (string typeClass: { "plugins", "datasets", "functions", "procedures" }) {
        for (auto type: proxy.get("/v1/types/" + typeClass).jsonBody()) {
            string url = "/v1/types/" + typeClass + "/" + type.asString() + "/doc";
            auto doc = proxy.get(
                url, {}, {}, -1, true, nullptr, nullptr, true /* redirect */);
            string error;
            if (doc.code() != 200) {
                cerr << url << endl;
                error = MLDB::trim(doc.body());
            }
            //BOOST_CHECK_EQUAL(doc.code(), 200);
            BOOST_CHECK_EQUAL(error, "");
        }
    }
}
