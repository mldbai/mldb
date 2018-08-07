/** shared_library_plugin.h                                         -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Shared library plugin style for MLDB.
*/

#pragma once

#include "mldb/core/plugin.h"

namespace MLDB {

/*****************************************************************************/
/* SHARED LIBRARY PLUGIN                                                     */
/*****************************************************************************/

/** Plugin that operates by exposing itself as a shared libary. */

struct SharedLibraryConfig {
    std::string address;        ///< Directory for the plugin
    std::string library;        ///< Library to load
    std::string doc;            ///< Documentation path to be served static
    std::string staticAssets;   ///< Path to static assets to be served
    std::string version;        ///< Version of plugin
    std::string apiVersion;     ///< Version of the API we require
    bool allowInsecureLoading;  ///< Must be set to true
};

DECLARE_STRUCTURE_DESCRIPTION(SharedLibraryConfig);

struct SharedLibraryPlugin: public Plugin {
    SharedLibraryPlugin(MldbEngine * engine,
                        PolyConfig config,
                        std::function<bool (const Json::Value & progress)> onProgress);

    ~SharedLibraryPlugin();

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

    virtual RestRequestMatchResult
    handleStaticRoute(RestConnection & connection,
                      const RestRequest & request,
                      RestRequestParsingContext & context) const;

    // Entry point.  It will attempt to call this when initializing
    // the plugin.  It is OK for the function to return a null pointer;
    // if not the given object will be used for the status, version
    // and request calls and for the documentation and static route calls
    // if no directory is configured in the configuration.
    typedef Plugin * (*MldbPluginEnterV100) (MldbEngine * engine);
private:
    struct Itl;

    std::unique_ptr<Itl> itl;
};


} // namespace MLDB

