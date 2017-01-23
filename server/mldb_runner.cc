// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* mldb_runner.cc
   Jeremy Barnes, 12 December 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Runner for MLDB.
*/

#include "mldb/arch/futex.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/server/credential_collection.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/config.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/soa/credentials/credentials.h"
#include <boost/filesystem.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/diagnostic_information.hpp> 
#include <signal.h>


using namespace std;

using namespace MLDB;
namespace fs = boost::filesystem;


constexpr int minimumWorkerThreads(4);

std::atomic<int> serviceShutdown(false);

void onSignal(int sig)
{
    serviceShutdown = true;
    ML::futex_wake(serviceShutdown);
}

struct CommandLineCredentialProvider: public CredentialProvider {

    CommandLineCredentialProvider(const std::vector<string> & credsStr)
    {
        for (auto & c: credsStr) {
            creds.emplace_back(jsonDecodeStr<StoredCredentials>(c));
        }
    }

    std::vector<StoredCredentials> creds;

    virtual std::vector<StoredCredentials>
    getCredentialsOfType(const std::string & resourceType) const
    {
        vector<StoredCredentials> matchingCreds;

        for (auto & cred: creds) {
            if (resourceType != cred.resourceType)
                continue;
            matchingCreds.push_back(cred);
        }

        return matchingCreds;
    }
};


int main(int argc, char ** argv)
{
    struct sigaction action;
    action.sa_handler = onSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction(SIGUSR2, &action, nullptr);

    using namespace boost::program_options;

    options_description configuration_options("Configuration options");
    options_description script_options("Script options");
    options_description plugin_options("Plugin options");

    int numThreads(16);
    // Defaults for operational characteristics
    string httpListenPort = "11700-18000";
    string httpListenHost = "0.0.0.0";
    string runScript;
    bool dontExitAfterScript = false;

    string cacheDir;
    string httpBaseUrl = "";

#if 0
    string peerListenPort = "18000-19000";
    string peerListenHost = "0.0.0.0";
    int peerPublishPort = -1;
    string peerPublishHost;
#endif
    string configPath;  // path to the configuration file
    std::string staticAssetsPath = "mldb/container_files/public_html/resources";
    std::string staticDocPath = "mldb/container_files/public_html/doc";

    string etcdUri;
    string etcdPath;

    string scriptArgs;
    string scriptArgsUrl;

    // List of credentials to add.  Each should be in JSON format as a
    //
    vector<string> addCredentials;
    string addCredentialsFromUrl;
    std::string credentialsPath;

    // List of directories to scan for plugins
    vector<string> pluginDirectory;

    bool muteFinalOutput = false;

    configuration_options.add_options()
#if 0
        ("etcd-uri", value(&etcdUri),
         "URI to connect to etcd")
        ("etcd-path", value(&etcdPath),
         "Base path in etcd")
#endif
        ("num-threads,t", value(&numThreads), "Number of HTTP worker threads")
        ("http-listen-port,p",
         value(&httpListenPort)->default_value(httpListenPort),
         "Port to listen on for HTTP")
        ("http-listen-host,h",
         value(&httpListenHost)->default_value(httpListenHost),
         "host to listen to")
        ("static-assets-path",
         value(&staticAssetsPath)->default_value(staticAssetsPath),
         "directory to serve static assets from")
        ("static-doc-path",
         value(&staticDocPath)->default_value(staticDocPath),
         "directory to serve documentation from")
        ("cache-dir", value(&cacheDir),
         "Cache directory to memory map large files and store downloads")

#if 0
        ("peer-listen-port,l",
         value(&peerListenPort)->default_value(peerListenPort),
         "port to listen to")
        ("peer-listen-host",
         value(&peerListenHost)->default_value(peerListenHost),
         "host to listen to")
        ("peer-publish-port,P",
         value(&peerPublishPort)->default_value(peerPublishPort),
         "port to publish for the outside world")
        ("peer-publish-host,H",
         value(&peerPublishHost)->default_value(peerPublishHost),
         "host to publish for the outside world")
#endif
        ("config-path",
         value(&configPath),
         "Path to the mldb configuration.  This is optional. Configuration option "
         "in that file have acceptable default values.")
        ("credentials-path,c", value(&credentialsPath),
         "Path in which to store saved credentials and rules "
         "(file:// for filesystem or s3:// for S3 uri)")
        ("hide-internal-entities",
         "Hide in the documentation entities that are not meant to be exposed")
        ("mute-final-output", bool_switch(&muteFinalOutput),
         "Mutes the output printed right before mldb_runner ends with an error"
         "condition")
        ("enable-access-log",
         "Enable the logging of each http request.  By default, the logging is disabled."
         "Specify this option to enable it.")
        ("http-base-url", value(&httpBaseUrl),
         "Prefix to prepend to all /doc urls.");

    script_options.add_options()
        ("run-script", value(&runScript),
         "Run the given script (JS or Python) and shutdown once done")
        ("script-args", value(&scriptArgs),
         "Provide the given (JSON) arguments to the script directly")
        ("script-args-url", value(&scriptArgsUrl),
         "Provide the given (JSON) arguments to the script loading from the given file")
        ("dont-exit-after-script", value(&dontExitAfterScript),
         "Don't exit immediately after running the script")
        ("add-credential", value(&addCredentials),
         "Add credential (see documentation for format) to MLDB before startup.  "
         "Multiple can be added per call.")
        ("add-credentials-from-url", value(&addCredentialsFromUrl),
         "Read credentials from file with list of JSON objects to add to MLDB before startup");

    plugin_options.add_options()
        ("plugin-directory", value(&pluginDirectory),
         "URL of directory to scan for plugins (can be added multiple times). "
         "Don't forget file://.");

    options_description all_opt;
    all_opt
        .add(configuration_options)
        .add(script_options)
        .add(plugin_options)
        .add_options()
        ("help", "print this message");

    variables_map vm;
    // command line has precendence over config
    try {
        store(command_line_parser(argc, argv)
              .options(all_opt)
              //.positional(p)
              .run(),
              vm);
    }
    catch (const boost::exception & exc) {
        cerr << boost::diagnostic_information(exc) << endl;
        return 1;
    }

    notify(vm);

    auto cmdConfig = Config::createFromProgramOptions(vm);

    if (vm.count("config-path")) {
        cerr << "reading configuration from file: '" << configPath << "'" << endl;
        auto parsed_options = parse_config_file<char>(configPath.c_str(), all_opt, true);
        store(parsed_options, vm);
        auto fileConfig = Config::createFromProgramOptions(parsed_options);
    }

    if (vm.count("help")) {
        cerr << all_opt << endl;
        exit(1);
    }

    if (numThreads < minimumWorkerThreads) {
        cerr << MLDB::format("'num-threads' cannot be less than %d: %d\n",
                           minimumWorkerThreads, numThreads);
        exit(1);
    }

    // Add these first so that if needed they can be used to load the credentials
    // file
    if (!addCredentials.empty()) {
        try {
            CredentialProvider::registerProvider
                (std::make_shared<CommandLineCredentialProvider>(addCredentials));
        }
        catch (const HttpReturnException & exc) {
            cerr << "error reading credentials from command line: "
                 << exc.what() << endl;
            cerr << jsonEncode(exc.details) << endl;
            return 1;
        }
        catch (const std::exception & exc) {
            cerr << "error reading credentials from command line: "
                 << exc.what() << endl;
            return 1;
        }
        MLDB_CATCH_ALL {
            cerr << "error reading credentials from command line: unknown error"
                 << endl;
            return 1;
        }
    }

    // Now load the credentials file
    if (!addCredentialsFromUrl.empty()) {
        try {
            filter_istream stream(addCredentialsFromUrl);
            vector<string> fileCredentials;
            Json::Value val = Json::parse(stream);
            if (val.type() == Json::arrayValue) {
                for (auto & v: val) {
                    fileCredentials.push_back(v.toString());
                }
            }
            else if (val.type() == Json::objectValue) {
                fileCredentials.push_back(val.toString());
            }
            else if (val.type() == Json::nullValue) {
                // skip
            }
            else throw MLDB::Exception("Couldn't understand credentials " + val.toString());

            if (!fileCredentials.empty()) {
                CredentialProvider::registerProvider
                    (std::make_shared<CommandLineCredentialProvider>(fileCredentials));
            }
        }
        catch (const HttpReturnException & exc) {
            cerr << "error reading credentials from file "
                 << addCredentialsFromUrl
                 << ": " << exc.what()
                 << endl;
            cerr << jsonEncode(exc.details) << endl;
            return 1;
        }
        catch (const std::exception & exc) {
            cerr << "error reading credentials from file "
                 << addCredentialsFromUrl
                 << ": " << exc.what()
                 << endl;
            return 1;
        }
        MLDB_CATCH_ALL {
            cerr << "error reading credentials from file "
                 << addCredentialsFromUrl << ": unknown error"
                 << endl;
            return 1;
        }
    }

    bool enableAccessLog = vm.count("enable-access-log");
    bool hideInternalEntities = vm.count("hide-internal-entities");

    MldbServer server("mldb", etcdUri, etcdPath, enableAccessLog, httpBaseUrl);
    bool initSuccess = server.init(credentialsPath, staticAssetsPath,
                                   staticDocPath, hideInternalEntities);

    // if the server initialization fails don't register plugins
    // but let MLDB starts with disabled features
    if (initSuccess) {
        // Set up the SSD cache, if configured
        if (!cacheDir.empty()) {
            server.setCacheDirectory(cacheDir);
        }

        // Scan each of our plugin directories
        for (auto & d: pluginDirectory) {
            server.scanPlugins(d);
        }
    }

    server.httpBoundAddress = server.bindTcp(httpListenPort, httpListenHost);
    server.router.addAutodocRoute("/autodoc", "/v1/help", "autodoc");
    server.threadPool->ensureThreads(numThreads);
    server.httpEndpoint->allowAllOrigins();

    server.start();

    cerr << "\n\nMLDB ready\n\n\n";

    if (!runScript.empty()) {
        // Run the script that implements the example

        fs::path path(runScript);
        string extension = path.extension().string();
        string runner = "";
        if     (extension == ".js")   runner = "javascript";
        else if(extension == ".py")   runner = "python";
        else throw MLDB::Exception("Unsupported extension '" +extension+ "'");

        HttpRestProxy proxy(server.httpBoundAddress);

        PluginResource config;
        if (runScript.find("://") == string::npos)
            config.address = "file://" + runScript;
        else config.address = runScript;

        // Get arguments
        if (!scriptArgsUrl.empty()) {
            try {
                filter_istream stream(scriptArgsUrl);
                Json::Value args = Json::parse(stream);
                config.args = args;
            } catch (const HttpReturnException & exc) {
                cerr << "error reading script arguments from url "
                     << scriptArgsUrl
                     << ": " << exc.what()
                     << endl;
                cerr << jsonEncode(exc.details) << endl;
                return 1;
            } catch (const std::exception & exc) {
                cerr << "error reading script arguments from url "
                     << scriptArgsUrl
                     << ": " << exc.what()
                     << endl;
                return 1;
            }
        }
        else if (!scriptArgs.empty()) {
            try {
                Json::Value args = Json::parse(scriptArgs);
                config.args = args;
            } catch (const std::exception & exc) {
                cerr << "error reading script arguments from command line: "
                     << exc.what() << endl;
                return 1;
            }
        }

        auto output = proxy.post("/v1/types/plugins/" + runner + "/routes/run",
                                 jsonEncode(config));

        bool success = false;

        auto result = output.jsonBody();
        if (result["result"] == "success")
            success = true;

        if (!success) {
            if (!muteFinalOutput) {
                cerr << output << endl;
            }
            return 1;  // startup script didn't succeed
        }

        if (!dontExitAfterScript) {
            return 0;
        }
    }

    while (!serviceShutdown) {
        ML::futex_wait(serviceShutdown, false, 10.0);
    }

    cerr << "shutting down" << endl;
    server.shutdown();
}
