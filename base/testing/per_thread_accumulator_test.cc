/** per_thread_accumulator.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Test of the thread pool.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>

#include "mldb/base/exc_assert.h"
#include "mldb/server/per_thread_accumulator.h"

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE (test_simple_usage)
{
    struct Accum {
    };

    PerThreadAccumulator<Accum> accum;

    Accum & a1 = accum.get();
    Accum & a2 = accum.get();

    BOOST_CHECK_EQUAL(&a1, &a2);
    BOOST_CHECK_EQUAL(accum.numThreads(), 1);
}
