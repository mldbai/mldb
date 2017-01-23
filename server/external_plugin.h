// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** external_plugin.h                                              -*- C++ -*-
    Jeremy Barnes, 8 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Setup a external plugin that lives out of process and communicates with MLDB
    via REST or an IPC mechanism.
*/

#pragma once


#include "mldb/core/plugin.h"
#include "mldb/utils/command_expression.h"


namespace MLDB {


/*****************************************************************************/
/* EXTERNAL PLUGIN COMMUNICATION                                             */
/*****************************************************************************/

/** This abstracts out how communication is performed with a external
    plugin.
*/

struct ExternalPluginCommunication: public MldbEntity {
    virtual ~ExternalPluginCommunication()
    {
    }

    /** Synchronously ping the plugin to make sure it's alive. */
    virtual bool pingSync() const = 0;

    virtual std::string getKind() const
    {
        return "pluginCommunication";
    }

    /** Return the status of the plugin. */
    virtual Any getStatus() const = 0;
    
    /** Return the version of the plugin. */
    virtual Any getVersion() const = 0;

    /** Handle the given request */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const = 0;

    /** Handle the given documentation request */
    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                             const RestRequest & request,
                             RestRequestParsingContext & context) const = 0;
    
};


/*****************************************************************************/
/* EXTERNAL PLUGIN SETUP                                                     */
/*****************************************************************************/

/** This abstracts out how we set up a external plugin. */

struct ExternalPluginSetup: public MldbEntity {
    virtual ~ExternalPluginSetup()
    {
    }

    virtual std::string getKind() const
    {
        return "pluginSetup";
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    virtual void setup() = 0;
};


/*****************************************************************************/
/* EXTERNAL PLUGIN STARTUP                                                   */
/*****************************************************************************/

/** This abstracts out how we start a external plugin. */

struct ExternalPluginStartup: public MldbEntity {
    virtual ~ExternalPluginStartup()
    {
    }

    virtual std::string getKind() const
    {
        return "pluginStartup";
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    /** Start a new instance of the external plugin, returning an object to
        be used to communicate with it.
    */
    virtual std::shared_ptr<ExternalPluginCommunication> start() = 0;
};


/*****************************************************************************/
/* HTTP EXTERNAL PLUGIN COMMUNICATION                                        */
/*****************************************************************************/

/** External plugin communication over HTTP REST.
*/

struct HttpExternalPluginCommunication: public ExternalPluginCommunication {
    HttpExternalPluginCommunication(const std::string & uri);

    virtual ~HttpExternalPluginCommunication()
    {
    }

    virtual bool pingSync() const;

    /** Return the status of the plugin. */
    virtual Any getStatus() const;
    
    /** Return the version of the plugin. */
    virtual Any getVersion() const;

    /** Handle the given request */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    /** Handle the given documentation request */
    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                             const RestRequest & request,
                             RestRequestParsingContext & context) const;

    struct Itl;
    std::shared_ptr<Itl> itl;
};


/*****************************************************************************/
/* EXTERNAL PLUGIN CONFIG                                                    */
/*****************************************************************************/

/** Configuration for a external plugin. */

struct ExternalPluginConfig {
    PolyConfig setup;   ///< Things to be done to set up the plugin
    PolyConfig startup;   ///< Things to be done to start the plugin
};

DECLARE_STRUCTURE_DESCRIPTION(ExternalPluginConfig);


/*****************************************************************************/
/* EXTERNAL PLUGIN                                                           */
/*****************************************************************************/

/** A external plugin is a plugin that runs outside of the address space of
    MLDB.

    A external plugin depends upon:
    1.  How is it set up?  Do we download something?  Do we mount something?
    2.  How it is started.  Is it a subprocess?  Run locally as a docker
        command?  Run on a external machine via an orchestration service?  Not
        run at all as it refers to a service managed elsewhere that is
        assumed to be always available?
    3.  How do we communicate with the service, and how does it communicate
        back with MLDB?  Normally this would be via REST, but for larger
        volumes of data there are other possibilities.
*/

struct ExternalPlugin: public Plugin {

    ExternalPlugin(MldbServer * server,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress);

    virtual ~ExternalPlugin();

    virtual Any getStatus() const;
    
    virtual Any getVersion() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                             const RestRequest & request,
                             RestRequestParsingContext & context) const;
    
    std::shared_ptr<ExternalPluginSetup> setup;
    std::shared_ptr<ExternalPluginStartup> startup;
    std::shared_ptr<ExternalPluginCommunication> communication;
};


/*****************************************************************************/
/* SUBPROCESS EXTERNAL PLUGIN                                                */
/*****************************************************************************/

/** External plugin that runs itself in a subprocess. */

struct SubprocessExternalPluginConfig {
    PluginCommand::CommandTemplate command;   ///< Command to run
};


DECLARE_STRUCTURE_DESCRIPTION(SubprocessExternalPluginConfig);


/*****************************************************************************/
/* SUBPROCESS PLUGIN STARTUP                                                 */
/*****************************************************************************/

struct SubprocessPluginStartup: public ExternalPluginStartup {

    SubprocessPluginStartup(MldbServer * server,
                            PolyConfig pconfig,
                            std::function<bool (const Json::Value & progress)> onProgress);

    SubprocessExternalPluginConfig config;
    
    /** Start a new instance of the external plugin, returning an object to
        be used to communicate with it.
    */
    virtual std::shared_ptr<ExternalPluginCommunication> start();

    struct Itl;
    std::shared_ptr<Itl> itl;
};

typedef EntityType<ExternalPluginStartup> PluginStartupType;

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
                          std::shared_ptr<const ValueDescription> config);

/** Register a new plugin kind.  This takes care of registering everything behind
    the scenes.
*/
template<typename T, typename Config>
std::shared_ptr<PluginStartupType>
registerPluginStartupType(const Package & package,
                          const std::string & name,
                          const std::string & description,
                          const std::string & docRoute,
                          TypeCustomRouteHandler customRoute = nullptr)
{
    return registerPluginStartupType
        (package, name, description,
         [] (RestDirectory * server,
             PolyConfig config,
             const std::function<bool (const Json::Value)> & onProgress)
         {
             return new T(T::getOwner(server), config, onProgress);
         },
         makeInternalDocRedirect(package, docRoute),
         customRoute,
         getDefaultDescriptionSharedT<Config>());
}

} // namespace MLDB

