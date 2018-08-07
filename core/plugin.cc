/* plugin.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Plugin support.
*/

#include "mldb/core/plugin.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/any_impl.h"

namespace MLDB {

std::shared_ptr<Plugin>
obtainPlugin(MldbEngine * engine,
             const PolyConfig & config,
             const MldbEngine::OnProgress & onProgress)
{
    return engine->obtainPluginSync(config, onProgress);
}

std::shared_ptr<Plugin>
createPlugin(MldbEngine * engine,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress,
             bool overwrite)
{
    return engine->createPluginSync(config, onProgress, overwrite);
}


/*****************************************************************************/
/* PLUGIN                                                                    */
/*****************************************************************************/

Plugin::
Plugin(MldbEngine * engine)
    : engine(static_cast<MldbEngine *>(engine))
{
}

Plugin::
~Plugin()
{
}

Any
Plugin::
getStatus() const
{
    return Any();
}
    
Any
Plugin::
getVersion() const
{
    return Any();
}

RestRequestMatchResult
Plugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    Json::Value error;
    error["error"] = "Plugin of type '" + MLDB::type_name(*this)
        + "' does not respond to custom route '" + context.remaining + "'";
    error["details"]["verb"] = request.verb;
    error["details"]["resource"] = request.resource;
    connection.sendErrorResponse(400, error);
    return RestRequestRouter::MR_ERROR;
}

RestRequestMatchResult
Plugin::
handleDocumentationRoute(RestConnection & connection,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const
{
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
Plugin::
handleStaticRoute(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
{
    return RestRequestRouter::MR_NO;
}


} // namespace MLDB


