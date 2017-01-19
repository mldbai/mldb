// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** rest_request_binding_test.cc
    Jeremy Barnes, 31 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/rest/rest_request_router.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/rest/in_process_rest_connection.h"


using namespace std;
using namespace ML;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_header_matching )
{
    RestRequestRouter router;

    struct TestObject {
        std::string call1()
        {
            return "hello";
        }

        std::string call2(std::string name)
        {
            return "hello " + name;
        }
    };

    TestObject testObject;

    addRouteSyncJsonReturn(router, "/test", { "PUT" }, "Call test object",
                           "hello",
                           &TestObject::call1, &testObject);

    addRouteSyncJsonReturn(router, "/test2", { "PUT" }, "Call test object",
                           "hello",
                           &TestObject::call2, &testObject,
                           RestParam<std::string>("name", "name of person to say hello to"));

    {
        InProcessRestConnection conn;
        router.handleRequest(conn, RestRequest("PUT", "/test", {}, ""));
        BOOST_CHECK_EQUAL(conn.response, "\"hello\"\n");
    }

    {
        InProcessRestConnection conn;
        router.handleRequest(conn, RestRequest("PUT", "/test", { { "param", "true" } }, ""));
        BOOST_CHECK_EQUAL(conn.responseCode, 400);
    }

    {
        cerr << "test 3" << endl;
        InProcessRestConnection conn;
        router.handleRequest(conn, RestRequest("PUT", "/test2", { { "name", "bob" } }, ""));
        BOOST_CHECK_EQUAL(conn.responseCode, 200);

        BOOST_CHECK_EQUAL(conn.response, "\"hello bob\"\n");
    }

    {
        cerr << "test 4" << endl;
        InProcessRestConnection conn;
        router.handleRequest(conn, RestRequest("PUT", "/test2", { { "name2", "bob" } }, ""));
        cerr << conn.response << endl;
        BOOST_CHECK_EQUAL(conn.responseCode, 400);
    }

}
