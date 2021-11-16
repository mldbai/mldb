/** metal.cc
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Metal plugin, to allow execution of Metal code and Metal functions
    to be defined.
*/

#include "mldb/core/plugin.h"
#include "mldb/builtin/plugin_resource.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
#include "mldb/types/array_description.h"
#include "mldb/block/memory_region.h"
#include "mldb/ext/mtlpp/src/mtlpp.hpp"

#include <regex>


using namespace std;

namespace MLDB {




/*****************************************************************************/
/* METAL PLUGIN                                                             */
/*****************************************************************************/

struct MetalPlugin: public Plugin {
    MetalPlugin(MldbEngine * engine,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress);
    
    ~MetalPlugin();
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

#if 0
    static ScriptOutput
    runMetalScript(MldbEngine * engine,
                        const PluginResource & scriptConfig);
#endif
    
    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * engine, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    //std::unique_ptr<JsPluginContext> itl;
};

MetalPlugin::
MetalPlugin(MldbEngine * engine,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(engine)
{
}
    
MetalPlugin::
~MetalPlugin()
{
}
    
Any
MetalPlugin::
getStatus() const
{
    return Any();
}

RestRequestMatchResult
MetalPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
#if 0
    // First, check for a route
    auto res = itl->router.processRequest(connection, request, context);
    if (res != RestRequestRouter::MR_NO)
        return res;

    // Second, check for a generic request handler
    if (itl->handleRequest)
        return itl->handleRequest(connection, request, context);
#endif

    // Otherwise we simply don't handle it
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
MetalPlugin::
handleTypeRoute(RestDirectory * engine,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
#if 0
    //cerr << "context.remaining = " << context.remaining << endl;

    if (context.resources.back() == "run") {
        //cerr << "request.payload = " << request.payload << endl;
        
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();
        
        auto result = runMetalScript(static_cast<MldbEngine *>(engine),
                                          scriptConfig);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");
        return RestRequestRouter::MR_YES;
    }
#endif
    return RestRequestRouter::MR_NO;
}


/*****************************************************************************/
/* REGISTRY                                                                  */
/*****************************************************************************/

RegisterPluginType<MetalPlugin, PluginResource>
regMetal(builtinPackage(),
          "metal",
          "Metal plugin loader",
          "lang/metal.md.html",
          &MetalPlugin::handleTypeRoute);

} // namespace MLDB
