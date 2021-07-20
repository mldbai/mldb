// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* epoll_test.cc
   Wolfgang Sourdeau, 17 November 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Assumption tests for epoll
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "mldb/io/timerfd.h"
#include "mldb/utils/testing/watchdog.h"
#include <sys/poll.h>

using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_timerfd_basics )
{
    TimerFD tfd;
    BOOST_CHECK(!tfd.initialized());
    tfd.init(TIMER_REALTIME);
    BOOST_CHECK(tfd.initialized());
    BOOST_CHECK_GE(tfd.fd(), 0);
}

BOOST_AUTO_TEST_CASE(check_timerfd_initialization)
{
    {
        TimerFD tfd(TIMER_REALTIME);
        BOOST_CHECK(tfd.initialized());
    }

    {
        TimerFD tfd(TIMER_MONOTONIC);
        BOOST_CHECK(tfd.initialized());
    }
}

BOOST_AUTO_TEST_CASE( test_timerfd_blocking_read )
{
    Watchdog watchdog(10.0);

    TimerFD tfd;
    BOOST_CHECK(!tfd.initialized());
    tfd.init(TIMER_REALTIME);
    BOOST_CHECK(tfd.initialized());
    BOOST_CHECK_GE(tfd.fd(), 0);

    auto start = std::chrono::high_resolution_clock::now();
    tfd.setTimeout(std::chrono::milliseconds(50));
    auto res = tfd.read();
    auto end = std::chrono::high_resolution_clock::now();

    auto msElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    cerr << "msElapsed = " << msElapsed << endl;
    BOOST_CHECK_GE(msElapsed, 50);
    BOOST_CHECK_EQUAL(res, 1);

    // Reading in between should return 0
    res = tfd.read(std::chrono::nanoseconds(0));
    BOOST_CHECK_EQUAL(res, 0);

    // Try a second timeout
    start = std::chrono::high_resolution_clock::now();
    tfd.setTimeout(std::chrono::milliseconds(50));
    res = tfd.read();
    end = std::chrono::high_resolution_clock::now();

    msElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    BOOST_CHECK_GE(msElapsed, 50);
    BOOST_CHECK_EQUAL(res, 1);
    
}
