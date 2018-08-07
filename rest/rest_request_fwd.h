/** rest_request_fwd.h                                             -*- C++ -*-
    Jeremy Barnes, 26 April 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Forward definitions for REST requests.
*/

#pragma once

#include <functional>

namespace MLDB {

enum RestRequestMatchResult {
    MR_NO,     ///< Didn't match but can continue
    MR_YES,    ///< Did match
    MR_ERROR,  ///< Error
    MR_ASYNC   ///< Handled, but asynchronously
};    

struct RestConnection;
struct RestRequest;
struct RestRequestParsingContext;
struct RestRequestRouter;
struct RestRouteManager;
struct RestEntity;
struct RestDirectory;

/// Call back function to handle a REST request used in RestRequestRouter
typedef std::function<RestRequestMatchResult
                      (RestConnection & connection,
                       const RestRequest & request,
                       RestRequestParsingContext & context)>
OnProcessRestRequest;

 
} // namespace MLDB

