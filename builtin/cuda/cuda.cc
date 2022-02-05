/** cuda.cc
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Cuda plugin, to allow execution of Cuda code and Cuda functions
    to be defined.
*/

#include "mldb/core/plugin.h"
#include "mldb/builtin/plugin_resource.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
#include "mldb/types/array_description.h"
#include "mldb/block/memory_region.h"

#include <regex>


using namespace std;

namespace MLDB {




/*****************************************************************************/
/* CUDA PLUGIN                                                             */
/*****************************************************************************/

struct CudaPlugin: public Plugin {
    CudaPlugin(MldbEngine * engine,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress);
    
    ~CudaPlugin();
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

#if 0
    static ScriptOutput
    runCudaScript(MldbEngine * engine,
                        const PluginResource & scriptConfig);
#endif
    
    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * engine, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    //std::unique_ptr<JsPluginContext> itl;
};

CudaPlugin::
CudaPlugin(MldbEngine * engine,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(engine)
{
}
    
CudaPlugin::
~CudaPlugin()
{
}
    
Any
CudaPlugin::
getStatus() const
{
    return Any();
}

RestRequestMatchResult
CudaPlugin::
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
CudaPlugin::
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
        
        auto result = runCudaScript(static_cast<MldbEngine *>(engine),
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

RegisterPluginType<CudaPlugin, PluginResource>
regCuda(builtinPackage(),
          "cuda",
          "Cuda plugin loader",
          "lang/cuda.md.html",
          &CudaPlugin::handleTypeRoute);

} // namespace MLDB
