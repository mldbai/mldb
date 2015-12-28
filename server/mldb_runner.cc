// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* mldb_runner.cc
   Jeremy Barnes, 12 December 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Runner for MLDB.
*/

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/algorithm/string.hpp>
#include "mldb/arch/futex.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/credentials/credentials_daemon.h"
#include "mldb/vfs/filter_streams.h"
#include <boost/filesystem.hpp>
#include <signal.h>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;
namespace fs = boost::filesystem;

std::atomic<int> serviceShutdown(false);

void onSignal(int sig)
{
    serviceShutdown = true;
    ML::futex_wake(serviceShutdown);
}

struct CommandLineCredentialProvider: public CredentialProvider {

    CommandLineCredentialProvider(const std::vector<string> & credsStr)
    {
        set<string> resourceTypeSet;
        for (auto & c: credsStr) {
            creds.emplace_back(jsonDecodeStr<StoredCredentials>(c));
            resourceTypeSet.insert(creds.back().resourceType);
        }

        resourceTypePrefixes.insert(resourceTypePrefixes.begin(),
                                    resourceTypeSet.begin(),
                                    resourceTypeSet.end());
    }

    std::vector<std::string> resourceTypePrefixes;
    std::vector<StoredCredentials> creds;

    virtual std::vector<std::string>
    getResourceTypePrefixes() const
    {
        return resourceTypePrefixes;
    }

    virtual std::vector<Credential>
    getSync(const std::string & resourceType,
            const std::string & resource,
            const CredentialContext & context,
            Json::Value extraData) const
    {
        vector<Credential> result;

        for (auto & c: creds) {
            if (resourceType != c.resourceType)
                continue;
            if (resource.find(c.resource) != 0)
                continue;
            result.push_back(c.credential);
        }

        return result;
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

    // Defaults for operational characteristics
    string httpListenPort = "11700-18000";
    string httpListenHost = "0.0.0.0";
    string runScript;
    bool dontExitAfterScript = false;

    string cacheDir;

#if 0
    string peerListenPort = "18000-19000";
    string peerListenHost = "0.0.0.0";
    int peerPublishPort = -1;
    string peerPublishHost;
#endif
    string configurationPath;
    std::string staticAssetsPath = "mldb/static";
    std::string staticDocPath = "mldb/container_files/assets/doc";

    string etcdUri;
    string etcdPath;

    string scriptArgs;
    string scriptArgsUrl;

    // List of credentials to add.  Each should be in JSON format as a
    // 
    vector<string> addCredentials;
    string addCredentialsFromUrl;

    // List of directories to scan for plugins
    vector<string> pluginDirectory;

    configuration_options.add_options()
#if 0
        ("etcd-uri", value(&etcdUri),
         "URI to connect to etcd")
        ("etcd-path", value(&etcdPath),
         "Base path in etcd")
#endif
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
        ("configuration-path,C",
         value(&configurationPath),
         "path that persistent configuration is stored to allow the service "
         "to stop and restart (file:// for filesystem or s3:// for S3 uri)")
        ("hide-internal-entities",
         "hide in the documentation entities that are not meant to be exposed");

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

    //if (!cacheDir.empty()) {
    //    throw ML::Exception("Cache dir is disabled");
    //}

    // Add these first so that if needed they can be used to load the credentials
    // file
    if (!addCredentials.empty()) {
        try {
            CredentialProvider::registerProvider
                ("mldbCommandLineCredentials",
                 std::make_shared<CommandLineCredentialProvider>(addCredentials));
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
        JML_CATCH_ALL {
            cerr << "error reading credentials from command line: unknown error"
                 << endl;
            return 1;
        }
    }

    // Now load the credentials file
    if (!addCredentialsFromUrl.empty()) {
        try {
            ML::filter_istream stream(addCredentialsFromUrl);
            vector<string> fileCredentials;
            while (stream) {
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
                else throw ML::Exception("Couldn't understand credentials " + val.toString());
            }
            
            if (!fileCredentials.empty()) {
                CredentialProvider::registerProvider
                    ("mldbCredentialsUrl",
                     std::make_shared<CommandLineCredentialProvider>(fileCredentials));
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
        JML_CATCH_ALL {
            cerr << "error reading credentials from file "
                 << addCredentialsFromUrl << ": unknown error"
                 << endl;
            return 1;
        }
    }


    MldbServer server("mldb", etcdUri, etcdPath);
    
    bool hideInternalEntities = vm.count("hide-internal-entities");

    server.init(configurationPath, staticAssetsPath, staticDocPath, hideInternalEntities);

    // Set up the SSD cache, if configured
    if (!cacheDir.empty()) {
        server.setCacheDirectory(cacheDir);
    }

    // Scan each of our plugin directories
    for (auto & d: pluginDirectory) {
        server.scanPlugins(d);
    }
    
    string httpBoundAddress = server.bindTcp(httpListenPort, httpListenHost);
    server.router.addAutodocRoute("/autodoc", "/v1/help", "autodoc");
    server.threadPool->ensureThreads(16);
    server.httpEndpoint->allowAllOrigins();

    cout << httpBoundAddress << endl;
    cerr << "http listening on " << httpBoundAddress << endl;

    server.start();

    cerr << "\n\nMLDB ready\n\n\n";

    if (!runScript.empty()) {
        // Run the script that implements the example

        fs::path path(runScript);
        string extension = path.extension().string();
        string runner = "";
        if     (extension == ".js")   runner = "javascript";
        else if(extension == ".py")   runner = "python";
        else throw ML::Exception("Unsupported extension '" +extension+ "'");

        HttpRestProxy proxy(httpBoundAddress);
        
        PluginResource config;
        if (runScript.find("://") == string::npos)
            config.address = "file://" + runScript;
        else config.address = runScript;

        // Get arguments
        if (!scriptArgsUrl.empty()) {
            try {
                ML::filter_istream stream(scriptArgsUrl);
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
            cerr << output << endl;
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
