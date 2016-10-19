/** tensorflow_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/mldb_entity.h"
#include "mldb/core/plugin.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/utils/log.h"
#include "mldb/base/scope.h"
#include "mldb/base/exc_assert.h"

// libav OR ffmpeg includes
extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavdevice/avdevice.h>
} // extern "C"

using namespace std;

namespace MLDB {

const Package & avPackage()
{
    static const Package result("av");
    return result;
}

/*****************************************************************************/
/* PLUGIN                                                                    */
/*****************************************************************************/

// Plugin entry point.  This is called by MLDB once the plugin is loaded.

struct AudioVideoPlugin: public Plugin {

    static AudioVideoPlugin * staticPlugin;

    static void loggingCallback(void * obj, int level,
                                const char * fmt, va_list vl)
    {
        ExcAssert(staticPlugin);

        char * mem = nullptr;
        int res = vasprintf(&mem, fmt, vl);
        if (res < 0 || mem == nullptr) {
            return;  // can't print it
        }
        Scope_Exit(free(mem));
        string memBytes(mem, mem + res);

        ExcAssert(staticPlugin->logger.get());

        if (level <= AV_LOG_ERROR) {
            ERROR_MSG(staticPlugin->logger) << memBytes;
        }
        else if (level <= AV_LOG_WARNING) {
            WARNING_MSG(staticPlugin->logger) << memBytes;
        }
        else if (level <= AV_LOG_INFO) {
            INFO_MSG(staticPlugin->logger) << memBytes;
        }
        else {
            DEBUG_MSG(staticPlugin->logger) << memBytes;
        }
    }

    AudioVideoPlugin(MldbServer * server)
        : Plugin(server)
    {
        logger = getMldbLog("av plugin");
        // Needs to be done before anything else happens
        staticPlugin = this;
        av_log_set_callback(loggingCallback);

        av_register_all();
        avdevice_register_all();
        initRoutes();

    }

    ~AudioVideoPlugin()
    {
        av_log_set_callback(av_log_default_callback);
        staticPlugin = nullptr;
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    RestRequestRouter router;

    // Initialize the routes
    void initRoutes()
    {
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return router.processRequest(connection, request, context);
    }
};

AudioVideoPlugin * AudioVideoPlugin::staticPlugin = nullptr;


} // namespace MLDB

MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server)
{
    return new Datacratic::MLDB::AudioVideoPlugin(server);
}

