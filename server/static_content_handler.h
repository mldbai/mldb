// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** static_content_handler.h                                       -*- C++ -*-
    Jeremy Barnes, 5 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Implementation of the static content handler for MLDB, including
    things like Markdown conversion.
*/

#include <string>
#include "mldb/rest/rest_request_router.h"



struct RestRequestRouter;

namespace MLDB {

struct MldbServer;

RestRequestRouter::OnProcessRequest
getStaticRouteHandler(std::string dir,
                      MldbServer * server,
                      bool hideInternalEntities = false);

/** Serve up the given directory for documentation.  This will transform
    any markdown files into HTML as they are served, including support
    for macros.
*/
void serveDocumentationDirectory(RestRequestRouter & parent,
                                 const std::string & route,
                                 const std::string & dir,
                                 MldbServer * server,
                                 bool hideInternalEntities = false);

} // namespace MLDB


