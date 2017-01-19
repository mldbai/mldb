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

#include "mldb/arch/futex.h"

using namespace std;

using ML::futex_wait;
using ML::futex_wake;

/* this helper ensures that the futex_wait does not return before futex_wake
 * is called */
template<typename T>
void
test_futex()
{
    T value;
    // To avoid LLVM 3.4 ICE
    value = 0;

    std::function<void ()> wakerFn = [&] () {
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        value = 5;
        futex_wake(value);
    };
    thread wakerTh(wakerFn);

    time_t start = ::time(nullptr);
    while (!value) {
        futex_wait(value, 0);
    }
    time_t now = ::time(nullptr);
    BOOST_CHECK(now > start);

    wakerTh.join();
}

/* this helper ensures that the futex_wait waits until timeout X when
 * specified and when the value does not change */ 
template<typename T>
void
test_futex_timeout()
{
    T value(0);
    time_t start = ::time(nullptr);
    ML::futex_wait(value, 0, 2.0);
    time_t now = ::time(nullptr);
    BOOST_CHECK(now >= (start + 1));
}

// use the above helpers for "int"
BOOST_AUTO_TEST_CASE( test_futex_int )
{
    test_futex<int>();
    test_futex_timeout<int>();
}

// use the above helpers for "volatile int"
BOOST_AUTO_TEST_CASE( test_futex_volatile_int )
{
    test_futex<volatile int>();
    test_futex_timeout<volatile int>();
}

// use the above helpers for "atomic<int>"
BOOST_AUTO_TEST_CASE( test_futex_atomic_int )
{
    test_futex<atomic<int>>();
    test_futex_timeout<atomic<int>>();
}
