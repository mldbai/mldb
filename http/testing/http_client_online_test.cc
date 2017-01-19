/* http_client_online_test.cc
   This file is part of MLDB.

   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014-2016 mldb.ai inc.  All rights reserved.

   Test for HttpClient that performs requests on online services (from the W3C
   for example)
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string>
#include <boost/test/unit_test.hpp>

#include "mldb/arch/futex.h"
#include "mldb/io/legacy_event_loop.h"
#include "mldb/http/http_client.h"

using namespace std;
using namespace MLDB;


#if 1
/* Use of chunked test from jigsaw.w3.org */
BOOST_AUTO_TEST_CASE( test_http_client_chunked_encoding )
{
    LegacyEventLoop loop;
    loop.start();

    int done(false);
    HttpClientError error;
    int status;
    string body;
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError newError, int newStatus,
                           string && headers, string && newBody) {
        error = newError;
        status = newStatus;
        body = move(newBody);
        done = true;
        ML::futex_wake(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    HttpClient client(loop, "http://jigsaw.w3.org");
    client.get("/HTTP/ChunkedScript", cbs);

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    BOOST_CHECK_EQUAL(error, HttpClientError::None);
    BOOST_CHECK_EQUAL(status, 200);
    BOOST_CHECK_EQUAL(body.size(), 72200);

    string expected
        = ("This output will be chunked encoded by the server,"
           " if your client is HTTP/1.1\n"
           "Below this line, is 1000 repeated lines of 0-9.\n"
           "-----------------------------------------------------------------"
           "--------\n");
    for (int i = 0; i < 1000; i++) {
        expected += ("01234567890123456789012345678901234567890123456789012345"
                     "678901234567890\n");
    }
    BOOST_CHECK_EQUAL(expected.size(), 72200);
    BOOST_CHECK_EQUAL(body, expected);
}
#endif
