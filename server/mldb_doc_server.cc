// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** mldb_doc_server.cc
    Jeremy Barnes, 4 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Server for MLDB documentation.
*/

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_collection.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/plugin_resource.h"

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

int main(int argc, char ** argv)
{
    using namespace boost::program_options;

    string httpBaseUrl = "";
    options_description all_opt;
    all_opt.add_options()
        ("help,h", "prints this help")
        ("http-base-url", value(&httpBaseUrl),
         "Prefix to prepend to all /doc urls.");

    variables_map vm;
    store(command_line_parser(argc, argv)
          .options(all_opt)
          .run(),
          vm);
    if (vm.count("help") || vm.count("h")) {
        cerr << all_opt << endl;
        return 0;
    }

    notify(vm);

    vector<string> pluginDirectory = { "file://build/x86_64/mldb_plugins" };

    MldbServer server;
    server.httpBaseUrl = httpBaseUrl;
    
    server.init();

    // Scan each of our plugin directories
    for (auto & d: pluginDirectory) {
        server.scanPlugins(d);
    }
    
    string httpBoundAddress = server.bindTcp(PortRange(17782,18000), "0.0.0.0");
    
    cerr << endl << endl << "Serving docs from " 
        << httpBoundAddress << "/static/assets/index.html" 
        << endl << endl << endl;

    server.start();

    for (;;) {
        sleep(10000);
    }
}
