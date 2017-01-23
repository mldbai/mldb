/* logger_test.cc
   Guy Dumais, 7 April 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Test of logging interface.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/utils/log.h"
#include <boost/test/unit_test.hpp>
#include <chrono>

using namespace std;
using namespace std::chrono;
using namespace MLDB;

int fib(int x) {
    if (x == 1) {
        return 1;
    } else if (x == 2) {
        return 1;
    } else {
        return fib(x-1) + fib(x-2);
    }
}

std::chrono::duration<double> time(const std::function<void()> fn) {
    auto begin = high_resolution_clock::now();
    fn();
    return  std::chrono::duration<double>(high_resolution_clock::now() - begin);
}


BOOST_AUTO_TEST_CASE(test_log_speed)
{
    auto logger = MLDB::getMldbLog("test");
    logger->set_level(spdlog::level::info);

    // better not to output million of messages
    std::cout.setstate(std::ios::failbit);

    struct ExpensiveObjectToConstruct :
        public std::string {
        ExpensiveObjectToConstruct() : std::string("test ") {
            fib(15); // waste time
        }
    };

    int count = 1000000;

    double enabledWithMacro = time([&]() {
            for (int i = 0; i < count; ++i) {
                INFO_MSG(logger) << ExpensiveObjectToConstruct() << i;
            }
        }).count();

    cerr <<  "enabled level with macro " << enabledWithMacro << endl;

    double enabledWithoutMacro = time([&]() {
            for (int i = 0; i < count; ++i) {
                logger->info() << ExpensiveObjectToConstruct() << i;
            }
        }).count();

    cerr <<  "enabled level without macro " << enabledWithoutMacro << endl;

    double disabledWithoutMacro = time([&]() {
            for (int i = 0; i < count; ++i) {
                logger->debug() << ExpensiveObjectToConstruct() << i;
            }
        }).count();

    cerr <<  "disabled level without macro " << disabledWithoutMacro << endl;

    double disabledWithMacro = time([&]() {
            for (int i = 0; i < count; ++i) {
                DEBUG_MSG(logger) << ExpensiveObjectToConstruct() << i;
            }
        }).count();

    cerr <<  "disabled level with macro " << disabledWithMacro << endl;

    // these two are only marginally faster because the expensive part
    // here is the evaluation of the stream parameters
    BOOST_CHECK_MESSAGE(disabledWithoutMacro < enabledWithMacro,
                        "disabled logging should be faster than the enabled one with macro");
    BOOST_CHECK_MESSAGE(disabledWithoutMacro < enabledWithoutMacro,
                         "disabled logging should be faster than the enabled one");

    // this one is order of magniture faster because it does not evaluate
    // the stream parameters
    BOOST_CHECK_MESSAGE(disabledWithMacro < disabledWithoutMacro,
                        "disabled logging using the macro should orders of magnitude faster");

    std::cout.clear();
}
