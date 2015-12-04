// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** credentialsd.cc                                                -*- C++ -*-
    Jeremy Barnes, 11 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "credentials_daemon.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/thread/thread.hpp>
#include <signal.h>

#include "mldb/arch/futex.h"
#include "mldb/rest/collection_config_store.h"


using namespace std;
using namespace ML;
using namespace Datacratic;

int serviceShutdown = false;

void onSignal(int sig)
{
    serviceShutdown = true;
    ML::futex_wake(serviceShutdown);
}

int main(int argc, char ** argv)
{
    struct sigaction action;
    action.sa_handler = onSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction(SIGUSR2, &action, nullptr);

    using namespace boost::program_options;

    options_description configuration_options("Configuration options");

    // Defaults for operational characteristics
    std::string listenPortRange = "11301";
    std::string listenHost = "localhost";
    std::string credentialsPath;

    configuration_options.add_options()
        ("listen-port,p", value(&listenPortRange)->default_value(listenPortRange),
         "port (or range)to listen on")
        ("listen-host,H", value(&listenHost)->default_value(listenHost),
         "Host to bind on")
        ("credentials-path,c", value(&credentialsPath),
         "Path in which to store saved credentials and rules");

    options_description all_opt;
    all_opt
        .add(configuration_options);
    all_opt.add_options()
        ("help,h", "print this message");
   
    variables_map vm;
    store(command_line_parser(argc, argv)
          .options(all_opt)
          //.positional(p)
          .run(),
          vm);
    notify(vm);
    
    if (vm.count("help")) {
        cerr << all_opt << endl;
        exit(1);
    }

    CredentialsDaemon service;

    std::shared_ptr<CollectionConfigStore> configStore;
    if (!credentialsPath.empty()) {
        cerr << "persisting credentials in " << credentialsPath << endl;
        configStore.reset(new S3CollectionConfigStore(credentialsPath));
    }

    service.init(configStore);

    service.httpEndpoint.allowAllOrigins();
    string uri = service.bindTcp(listenPortRange, listenHost);
    cout << "Credentials available on " << uri << endl;

    while (!serviceShutdown) {
        ML::futex_wait(serviceShutdown, false, 10.0);
    }

    cerr << "shutting down" << endl;
    service.shutdown();
}
