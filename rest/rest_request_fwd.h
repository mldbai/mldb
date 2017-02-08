// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** rest_request_fwd.h                                             -*- C++ -*-
    Jeremy Barnes, 26 April 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Forward definitions for REST requests.
*/

#pragma once

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


} // namespace MLDB

