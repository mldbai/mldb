/** http_client_impl.cc
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
*/

#include "mldb/http/http_client_impl.h"

using namespace std;
using namespace MLDB;


HttpClientImpl::
HttpClientImpl(const string & baseUrl, int numParallel, int queueSize)
    : AsyncEventSource()
{
}

HttpClientImpl::
~HttpClientImpl()
{
}
