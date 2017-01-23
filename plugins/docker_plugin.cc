// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** docker_plugin.cc
    Jeremy Barnes, 9 September 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "docker_plugin.h"
#include "mldb/server/external_plugin.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/utils/json_utils.h"
#include "mldb/vfs/filter_streams.h"

// dynamic loading support
#include <dlfcn.h>


using namespace std;



namespace MLDB {

namespace {
std::mutex dlopenMutex;
} // file scope

/*****************************************************************************/
/* DOCKER PLUGIN STARTUP CONFIG                                              */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DockerPluginStartupConfig);

DockerPluginStartupConfigDescription::
DockerPluginStartupConfigDescription()
{
    addField("repo", &DockerPluginStartupConfig::repo,
             "Repository under which the plugin is stored");
    addField("sharedLibrary", &DockerPluginStartupConfig::sharedLibrary,
             "Path to shared library in repo");
}


/*****************************************************************************/
/* INTERNAL PLUGIN COMMUNICATION                                             */
/*****************************************************************************/

struct InternalPluginCommunication: public ExternalPluginCommunication {
    InternalPluginCommunication(std::shared_ptr<Plugin> plugin)
        : plugin(std::move(plugin))
    {
    }

    virtual ~InternalPluginCommunication()
    {
    }

    std::shared_ptr<Plugin> plugin;

    /** Synchronously ping the plugin to make sure it's alive. */
    virtual bool pingSync() const
    {
        return true;
    }

    /** Return the status of the plugin. */
    virtual Any getStatus() const
    {
        return plugin->getStatus();
    }
    
    /** Return the version of the plugin. */
    virtual Any getVersion() const
    {
        return plugin->getVersion();
    }

    /** Handle the given request */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return plugin->handleRequest(connection, request, context);
    }

    /** Handle the given documentation request */
    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                             const RestRequest & request,
                             RestRequestParsingContext & context) const
    {
        return plugin->handleDocumentationRoute(connection, request, context);
    }
    
};


/*****************************************************************************/
/* DOCKER PLUGIN STARTUP                                                     */
/*****************************************************************************/

DockerPluginStartup::
DockerPluginStartup(MldbServer * server,
                    PolyConfig pconfig,
                    std::function<bool (const Json::Value & progress)> onProgress)
    : server(server)
{
    config = pconfig.params.convert<DockerPluginStartupConfig>();
}

std::shared_ptr<ExternalPluginCommunication>
DockerPluginStartup::
start()
{
    // Open the plugin
    Utf8String uri = "docker://" + config.repo + "/" + config.sharedLibrary;

    filter_istream stream(uri.rawString());

    std::string path
        = "tmp/plugin-"
        + MLDB::format("%08x", jsonHash(jsonEncode(config)))
        + ".so";

    {
        std::ofstream out(path.c_str());
        out << stream.rdbuf();
    }

    // Also exclude anyone else from using dlopen, since it's not thread
    // safe.
    std::unique_lock<std::mutex> guard2(dlopenMutex);

    dlerror();  // clear existing error
    void * handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        char * error = dlerror();
        ExcAssert(error);
        throw MLDB::Exception("couldn't load plugin %s: %s",
                            path.c_str(), error);
    }

    dlerror();  // clear existing error

    typedef Plugin * (*RegisterFnType) (MldbServer *);

    auto registerPlugin =  (RegisterFnType) dlsym(handle, "mldbPluginEnter");
    
    if (!registerPlugin)
        throw MLDB::Exception("plugin library doesn't expose mldb_plugin_enter: %s",
                            dlerror());
    
    std::shared_ptr<Plugin> plugin(registerPlugin(server));

    return std::make_shared<InternalPluginCommunication>(std::move(plugin));
}

namespace {

struct AtInit {
    AtInit()
    {
        registerPluginStartupType<DockerPluginStartup, DockerPluginStartupConfig>
            (builtinPackage(),
             "experimental.docker",
             "Obtain and start a plugin from Docker",
             "plugins/Docker.md.html");
    }
} atInit;

} // file scope

} // namespace MLDB

