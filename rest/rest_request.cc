// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_request.cc
   Jeremy Barnes, 11 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Endpoint to talk with a REST service.
*/

#include "mldb/rest/rest_request.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/types/structure_description.h"

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* REST REQUEST                                                              */
/*****************************************************************************/

std::ostream & operator << (std::ostream & stream, const RestRequest & request)
{
    return stream << request.verb << " " << request.resource << endl
                  << request.params << endl
                  << request.payload;
}

DEFINE_STRUCTURE_DESCRIPTION(RestRequest);

RestRequestDescription::
RestRequestDescription()
{
    addField("verb", &RestRequest::verb,
             "Verb of the request (what to do)");
    addField("resource", &RestRequest::resource,
             "Resource of the request (URI of entity to operate on)");
    addField("params", &RestRequest::params,
             "Query parameters of the request (GET parameters)");
    addField("header", &RestRequest::header,
             "Header of HTTP request");
    addField("payload", &RestRequest::payload,
             "Payload of request");
}


} // namespace MLDB
