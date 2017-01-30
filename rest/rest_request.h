// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_service_endpoint.h                                         -*- C++ -*-
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "mldb/http/http_header.h"

namespace MLDB {

/*****************************************************************************/
/* REST REQUEST                                                              */
/*****************************************************************************/

struct RestRequest {
    RestRequest()
    {
    }

    RestRequest(const HttpHeader & header,
                const std::string & payload)
        : header(header),
          verb(header.verb),
          resource(header.resource),
          params(header.queryParams),
          payload(payload)
    {
    }

    RestRequest(const std::string & verb,
                const std::string & resource,
                const RestParams & params,
                const std::string & payload)
        : verb(verb), resource(resource), params(params), payload(payload)
    {
    }

    RestRequest(const HttpHeader & header,
                const std::string & verb,
                const std::string & resource,
                const RestParams & params,
                const std::string & payload)
        : header(header), verb(verb), resource(resource), params(params), payload(payload)
    {
    }
    
    HttpHeader header;
    std::string verb;
    std::string resource;
    RestParams params;
    std::string payload;
};

std::ostream & operator << (std::ostream & stream, const RestRequest & request);

DECLARE_STRUCTURE_DESCRIPTION(RestRequest);

} // namespace MLDB

