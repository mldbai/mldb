/**                                                                 -*- C++ -*-
 * mongo_temporary_server_test.cc
 * Sunil Rottoo, 2 September 2014
 * This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
 *
 * Test for our Mongo class.
 **/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <sys/syscall.h>
#include <boost/test/unit_test.hpp>

#include "../mongo_temporary_server.h"

using namespace std;
using namespace ML;

using namespace Mongo;

BOOST_AUTO_TEST_CASE( test_mongo_connection )
{
    MongoTemporaryServer mongo;

    std::this_thread::sleep_for(std::chrono::seconds(1));
    cerr << "Shutting down the mongo server " << endl;
    mongo.shutdown();
}
