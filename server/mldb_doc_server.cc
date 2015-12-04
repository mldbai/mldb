// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** mldb_doc_server.cc
    Jeremy Barnes, 4 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Server for MLDB documentation.
*/

#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

int main(int argc, char ** argv)
{
    MldbServer server;
    
    server.init();

    string httpBoundAddress = server.bindTcp(PortRange(17782,18000), "0.0.0.0");
    
    cerr << endl << endl << "Serving docs from " 
        << httpBoundAddress << "/static/assets/index.html" 
        << endl << endl << endl;

    server.start();

    for (;;) {
        sleep(10000);
    }
}
