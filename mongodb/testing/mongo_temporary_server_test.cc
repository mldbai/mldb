/**                                                                 -*- C++ -*-
 * mongo_temporary_server_test.cc
 * Sunil Rottoo, 2 September 2014
 * This file is part of MLDB. Copyright 2014 Datacratic. All rights reserved.
 *
 * Test for our Mongo class.
 **/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <linux/futex.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/function.hpp>

#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/arch/atomic_ops.h"
#include "mldb/arch/atomic_ops.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/futex.h"

#include "../mongo_temporary_server.h"

using namespace std;
using namespace ML;

using namespace Mongo;

BOOST_AUTO_TEST_CASE( test_mongo_connection )
{
    MongoTemporaryServer mongo;

    sleep(1.0);
    cerr << "Shutting down the mongo server " << endl;
    mongo.shutdown();
}
