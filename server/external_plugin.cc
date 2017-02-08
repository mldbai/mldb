/** external_plugin.cc
    Jeremy Barnes, 8 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    External plugin runner.
*/

#include "mldb/server/external_plugin.h"
#include "mldb/server/script_output.h"
#include "mldb/utils/runner.h"
#include "mldb/io/message_loop.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/arch/backtrace.h"
#include "mldb/rest/poly_collection_impl.h"
#include <future>
#include <boost/algorithm/string.hpp>
#include "mldb/types/any_impl.h"


using namespace std;
using namespace MLDB::PluginCommand;

namespace MLDB {


/*****************************************************************************/
/* HTTP EXTERNAL PLUGIN COMMUNICATION                                        */
/*****************************************************************************/

struct HttpExternalPluginCommunication::Itl {
    Itl(const std::string & uri)
        : proxy(uri)
    {
    }
    
    HttpRestProxy proxy;
};

HttpExternalPluginCommunication::
HttpExternalPluginCommunication(const std::string & uri)
    : itl(new Itl(uri))
{
}

bool
HttpExternalPluginCommunication::
pingSync() const
{
    auto resp = itl->proxy.get("/ping");
    
    //cerr << "ping returned " << resp << endl;

    return resp.code() == 200;
}

Any
HttpExternalPluginCommunication::
getStatus() const
{
    auto resp = itl->proxy.get("/status");
    return resp.jsonBody();
}
    
Any
HttpExternalPluginCommunication::
getVersion() const
{
    auto resp = itl->proxy.get("/version");
    return resp.jsonBody();
}

RestRequestMatchResult
HttpExternalPluginCommunication::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    double timeout = 5.0;
    bool followRedirect = false;

    auto resp = itl->proxy.perform(request.verb,
                                   ("/routes" + context.remaining).rawString(),
                                   request.payload,
                                   request.params,
                                   request.header.headers,
                                   timeout,
                                   true /* exceptions */,
                                   nullptr,
                                   nullptr,
                                   followRedirect);

    //cerr << "got response from request route " << resp << endl;

    connection.sendHttpResponse(resp.code(),
                                resp.body(),
                                resp.header().contentType,
                                resp.header().headers);

    return RestRequestRouter::MR_YES;
}

RestRequestMatchResult
HttpExternalPluginCommunication::
handleDocumentationRoute(RestConnection & connection,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const
{
    double timeout = 5.0;
    bool followRedirect = false;

    auto resp = itl->proxy.perform(request.verb,
                                   ("/doc/" + context.remaining).rawString(),
                                   request.payload,
                                   request.params,
                                   request.header.headers,
                                   timeout,
                                   true /* exceptions */,
                                   nullptr,
                                   nullptr,
                                   followRedirect);
    
    //cerr << "got response from doc route " << resp << endl;

    connection.sendHttpResponse(resp.code(),
                                resp.body(),
                                resp.getHeader("content-type"),
                                resp.header().headers);
    
    return RestRequestRouter::MR_YES;
}


/*****************************************************************************/
/* EXTERNAL PLUGIN CONFIG                                                    */
/*****************************************************************************/


DEFINE_STRUCTURE_DESCRIPTION(ExternalPluginConfig);

ExternalPluginConfigDescription::
ExternalPluginConfigDescription()
{
    addField("setup", &ExternalPluginConfig::setup,
             "How to set up the plugin");
    addField("startup", &ExternalPluginConfig::startup,
             "How to start up the plugin");
}


/*****************************************************************************/
/* EXTERNAL PLUGIN                                                           */
/*****************************************************************************/

ExternalPlugin::
ExternalPlugin(MldbServer * server,
             PolyConfig pconfig,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(server)
{
    // 1.  Perform the plugin setup

    auto config = pconfig.params.convert<ExternalPluginConfig>();

    if (config.startup.type.empty())
        throw HttpReturnException(400, "External plugin config must have startup section",
                                  "config", config);

    if (!config.setup.type.empty()) {
        setup
            = PolyCollection<ExternalPluginSetup>
            ::doConstruct(MldbEntity::getPeer(server), config.startup, onProgress);
        setup->setup();
    }
    
    startup
        = PolyCollection<ExternalPluginStartup>
        ::doConstruct(MldbEntity::getPeer(server), config.startup, onProgress);
    communication = startup->start();
    
    communication->pingSync();
}

ExternalPlugin::
~ExternalPlugin()
{
}

Any
ExternalPlugin::
getStatus() const
{
    return communication->getStatus();
}
    
Any
ExternalPlugin::
getVersion() const
{
    return communication->getVersion();
}

RestRequestMatchResult
ExternalPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    return communication->handleRequest(connection, request, context);
}

RestRequestMatchResult
ExternalPlugin::
handleDocumentationRoute(RestConnection & connection,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const
{
    return communication->handleDocumentationRoute(connection, request, context);
}


/*****************************************************************************/
/* SUBPROCESS EXTERNAL PLUGIN                                                */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SubprocessExternalPluginConfig);

SubprocessExternalPluginConfigDescription::
SubprocessExternalPluginConfigDescription()
{
    addField("command", &SubprocessExternalPluginConfig::command,
             "Command to run to launch external plugin");
}


/*****************************************************************************/
/* SUBPROCESS PLUGIN STARTUP                                                 */
/*****************************************************************************/

struct SubprocessPluginStartup::Itl {
    Itl()
        : stdin(&runner.getStdInSink()), shutdown(false)
    {
        messageLoop.addSource("runner", runner);
        messageLoop.start();
    }

    ~Itl()
    {
        shutdown = true;
        stdin->requestClose();
        runner.kill(SIGTERM, true);
        messageLoop.shutdown();
    }

    Runner runner;
    MessageLoop messageLoop;
    OutputSink * stdin;

    std::mutex mutex;
    RunResult result;

    std::vector<ScriptLogEntry> output;

    std::atomic<bool> shutdown;

    std::function<void (const std::string &) > onStdOut;

    void commandHasTerminated(const RunResult & result)
    {
        //cerr << "command has terminated with status " << jsonEncode(result) << endl;
        if (shutdown)
            return;
        std::unique_lock<std::mutex> guard(mutex);
        this->result = result;
        //cerr << jsonEncode(output) << endl;
    }

    void onStderrData(std::string && data)
    {
        //cerr << "stderr: " << data << endl;
        if (shutdown)
            return;
        std::unique_lock<std::mutex> guard(mutex);
        output.emplace_back(Date::now(), "stderr", std::move(data));
    }

    void onStderrClose()
    {
        //cerr << "stderr: closed" << endl;

        if (shutdown)
            return;
        std::unique_lock<std::mutex> guard(mutex);
        output.emplace_back(Date::now(), "stderr", true /* closed */);
    }

    void onStdoutData(std::string && data)
    {
        //cerr << "stdout: " << data << endl;
        if (shutdown)
            return;
        std::unique_lock<std::mutex> guard(mutex);
        if (onStdOut)
            onStdOut(data);
        output.emplace_back(Date::now(), "stdout", std::move(data));
    }

    void onStdoutClose()
    {
        //cerr << "stdout: closed" << endl;
        if (shutdown)
            return;
        std::unique_lock<std::mutex> guard(mutex);
        output.emplace_back(Date::now(), "stdout", true /* closed */);
    }

    std::shared_ptr<CallbackInputSink> getStdout()
    {
        auto onData = std::bind(&Itl::onStdoutData, this,
                                std::placeholders::_1);
        auto onClose = std::bind(&Itl::onStdoutClose, this);
        return std::make_shared<CallbackInputSink>(onData, onClose);
    }

    std::shared_ptr<CallbackInputSink> getStderr()
    {
        auto onData = std::bind(&Itl::onStderrData, this,
                                std::placeholders::_1);
        auto onClose = std::bind(&Itl::onStderrClose, this);
        return std::make_shared<CallbackInputSink>(onData, onClose);
    }
};

SubprocessPluginStartup::
SubprocessPluginStartup(MldbServer * server,
                        PolyConfig pconfig,
                        std::function<bool (const Json::Value & progress)> onProgress)
{
    config = pconfig.params.convert<SubprocessExternalPluginConfig>();
}

std::shared_ptr<ExternalPluginCommunication>
SubprocessPluginStartup::
start()
{
    itl.reset(new Itl());

    auto command = config.command({});

    auto onTerminate = std::bind(&Itl::commandHasTerminated, itl.get(),
                                 std::placeholders::_1);


    std::promise<std::string> gotAddress;

    auto onStdOut = [&] (const std::string & data)
        {
            //cerr << "got std out data " << data << endl;
            
            vector<string> lines;
            boost::split(lines, data, boost::is_any_of("\n"));

            for (auto & l: lines) {
                if (l.find("MLDB PLUGIN ") != 0)
                    continue;

                vector<string> fields;
                boost::split(fields, l, boost::is_any_of(" "));

                ExcAssertEqual(fields.size(), 4);
                ExcAssertEqual(fields[0], "MLDB");
                ExcAssertEqual(fields[1], "PLUGIN");
                ExcAssertEqual(fields[2], "http");
                string uri = fields[3];
                gotAddress.set_value(uri);
                
                // We don't need any more data back
                itl->onStdOut = nullptr;

                return;
            }
        };

    itl->onStdOut = onStdOut;

    //cerr << "running..." << endl;
    itl->runner.run(command.cmdLine,
                    onTerminate,
                    itl->getStdout(),
                    itl->getStderr());

    //cerr << "finished run call" << endl;


    /* Give it 5 seconds to launch */
    bool started = itl->runner.waitStart(5);

    //cerr << "started = " << started << endl;

    if (!started) {
        throw HttpReturnException(500, "Error starting external subprocess",
                                  "config", config,
                                  "subprocessState", itl->result);
    }

    // Now, loop until the process says it's started and returns parameters for
    // how to connect to it

    //cerr << jsonEncode(res);

    // Wait for the function to finish
    std::future<std::string> future = gotAddress.get_future();

    // Give it 15 seconds to initialize
    if (future.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
        throw HttpReturnException(500, "Error waiting for plugin initialization",
                                  "config", config);
    }
    
    // Get the address
    string address;
    try {
        address = future.get();
    } MLDB_CATCH_ALL {
        rethrowHttpException(-1, "Error getting plugin address",
                             "config", config);
    }

    cerr << "Got external subprocess on address " << address << endl;

    return std::make_shared<HttpExternalPluginCommunication>(address);
}

std::shared_ptr<PluginStartupType>
registerPluginStartupType(const Package & package,
                               const std::string & name,
                               const std::string & description,
                               std::function<ExternalPluginStartup * (RestDirectory *,
                                                                      PolyConfig,
                                                                      const std::function<bool (const Json::Value)> &)>
    createEntity,
                               TypeCustomRouteHandler docRoute,
                               TypeCustomRouteHandler customRoute,
                               std::shared_ptr<const ValueDescription> config)
{
    return PolyCollection<ExternalPluginStartup>
        ::registerType(package, name, description, createEntity,
                       docRoute, customRoute, config);
}


namespace {

struct AtInit {
    AtInit()
    {
        registerPluginType<ExternalPlugin, ExternalPluginConfig>
            (builtinPackage(),
             "experimental.external", "Run an external plugin",
             "plugins/External.md.html",
             nullptr,
            { MldbEntity::INTERNAL_ENTITY });

        registerPluginStartupType<SubprocessPluginStartup, SubprocessExternalPluginConfig>
            (builtinPackage(),
             "subprocess", "Start an external plugin by starting a subprocess",
             "plugins/External.md.html");

        //&SubprocessPlugin::handleTypeRoute);
    }
} atInit;

} // file scope

template class PolyCollection<MLDB::ExternalPluginSetup>;
template class PolyCollection<MLDB::ExternalPluginStartup>;

} // namespace MLDB


