/* futex_test.cc
   Wolfgang Sourdeau, october 10th 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of the futex utility functions
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <time.h>

#include <atomic>
#include <thread>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <thread>

#include "mldb/arch/wait_on_address.h"

using namespace std;

using MLDB::wait_on_address;
using MLDB::wake_by_address;

/* this helper ensures that the wait_on_address does not return before wake_by_address
 * is called */
template<typename T>
void
test_futex()
{
    T value(0);
    // To avoid LLVM 3.4 ICE

    std::function<void ()> wakerFn = [&] () {
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        value = 5;
        wake_by_address(value);
    };
    thread wakerTh(wakerFn);

    time_t start = ::time(nullptr);
    while (!value) {
        wait_on_address(value, 0);
    }
    time_t now = ::time(nullptr);
    BOOST_CHECK(now > start);

    wakerTh.join();
}

/* this helper ensures that the wait_on_address waits until timeout X when
 * specified and when the value does not change */ 
template<typename T>
void
test_futex_timeout()
{
    T value(0);
    time_t start = ::time(nullptr);
    MLDB::wait_on_address(value, 0, 2.0);
    time_t now = ::time(nullptr);
    BOOST_CHECK(now >= (start + 1));
}

// use the above helpers for "atomic<int>"
BOOST_AUTO_TEST_CASE( test_futex_atomic_int )
{
    test_futex<atomic<int>>();
    test_futex_timeout<atomic<int>>();
}
