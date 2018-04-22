/* filter_streams_test.cc
   Jeremy Barnes, 29 June 2011
   Copyright (c) 2011 mldb.ai inc.
   Copyright (c) 2011 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string.h>

#include "mldb/arch/userfault.h"
#include "mldb/arch/vm.h"
#include "mldb/utils/testing/watchdog.h"

#include <boost/test/unit_test.hpp>

#include <chrono>


using namespace MLDB;
using namespace std;

#if 0

bool isSlave()
{
    int argc = boost::unit_test::framework::master_test_suite().argc;
    auto argv = boost::unit_test::framework::master_test_suite().argv;

    for (int i = 1;  i < argc;  ++i) {
        if (strcmp(argv[i], "slave") == 0)
            return true;
    }
    return false;
}

void checkSignal(int expectedSignal)
{
#if 0
    int argc = boost::unit_test::framework::master_test_suite().argc;
    auto argv = boost::unit_test::framework::master_test_suite().argv;

    cerr << "argc = " << argc << endl;
    for (size_t i = 0;  i < argc;  ++i) {
        cerr << "argv = " << argv[i] << endl;
    }
#endif
    
    // find our executable
    char path[65536];
    ssize_t len = readlink("/proc/self/exe", path, 65536);
    path[len] = 0;

    string testName = boost::unit_test::framework::current_test_case().p_name;
    
    string cmdline = string(path) + " slave --catch_system_errors=no --run_test=" + testName;

    cerr << "cmdline = " << cmdline << endl;

    signal(SIGCHLD, SIG_DFL);
    
    int res = system(cmdline.c_str());
    if (res == -1)
        cerr << strerror(errno) << endl;

    //cerr << "res = " << res << endl;
    
    //cerr << "WIFEXITED " << WIFEXITED(res) << endl;
    //cerr << "WEXITSTATUS " << WEXITSTATUS(res) << endl;
    //cerr << "WIFSIGNALED " << WIFSIGNALED(res) << endl;
    //cerr << "WTERMSIG " << WTERMSIG(res) << endl;
    //cerr << "WTERMSIG(WIFSIGNALED()) " << WTERMSIG(WEXITSTATUS(res)) << endl;

    int signal = WTERMSIG(res);
    int exitStatus = WEXITSTATUS(res);
    int exitSig = WTERMSIG(exitStatus);
    
    cerr << "exitSig = " << exitSig << endl;

    if (WIFSIGNALED(res)) {
        BOOST_CHECK_EQUAL(signal, expectedSignal);
    }
    else {
        BOOST_CHECK(WIFEXITED(res));
        BOOST_CHECK_EQUAL(exitSig, expectedSignal);
    }

#if 0
    BOOST_CHECK((WIFSIGNALED(res) && WTERMSIG(res) == expectedSignal)
                || (WIFEXITED(res) && exitSig == expectedSignal));

    BOOST_CHECK_EQUAL(signal, expectedSignal);
#endif
}

BOOST_AUTO_TEST_CASE( test_simple_access )
{
    ML::Watchdog watchdog(5.0);

    auto & pfh = PageFaultHandler::instance();

    constexpr int NUM_PAGES = 8;
    
    auto mapping = pfh.addRange(NUM_PAGES * page_size);
    
    // Map null data in
    mapping->copyPages(0, NUM_PAGES);

    // verify that we can read back
    const char * data = mapping->getMappingStart();
    BOOST_CHECK_EQUAL(data[0], 0);
    BOOST_CHECK_EQUAL(data[NUM_PAGES * page_size - 1], 0);
}


BOOST_AUTO_TEST_CASE( test_check_signal )
{
    // Verify that our ability to run a test case to verify a signal works
    if (isSlave()) {
        abort();
    }
    else {
        checkSignal(SIGABRT);
    }
}

BOOST_AUTO_TEST_CASE( test_hang_on_no_backing_copied )
{
    if (isSlave()) {

        ML::Watchdog watchdog(0.1);
        auto & pfh = PageFaultHandler::instance();
        constexpr int NUM_PAGES = 8;
    
        auto mapping = pfh.addRange(NUM_PAGES * page_size);

        // Don't back the mapping; this should cause this to block and
        // the watchdog to fire.
        
        // Try to read back
        const char * data = mapping->getMappingStart();

        // This statement should never finish
        BOOST_CHECK_EQUAL(data[0], 0);
    }
    else {
        checkSignal(SIGABRT);
    }
}

BOOST_AUTO_TEST_CASE( test_segv_on_backing_failure )
{
    if (isSlave()) {

        ML::Watchdog watchdog(0.1);
        auto & pfh = PageFaultHandler::instance();
        constexpr int NUM_PAGES = 8;
    
        auto mapping = pfh.addRange(NUM_PAGES * page_size);

        // Try to read back
        const char * data = mapping->getMappingStart();

        mapping->failAccessWithSegv();
        
        // This statement should cause a segv
        BOOST_CHECK_EQUAL(data[0], 0);
    }
    else {
        checkSignal(SIGSEGV);
    }

}

BOOST_AUTO_TEST_CASE( test_segv_on_backing_failure_after_fault )
{
    if (isSlave()) {

        ML::Watchdog watchdog(0.1);
        auto & pfh = PageFaultHandler::instance();
        constexpr int NUM_PAGES = 8;
    
        auto mapping = pfh.addRange(NUM_PAGES * page_size);

        // Try to read back
        const char * data = mapping->getMappingStart();

        // Set up the access to fail AFTER the memory access is attempted
        std::thread t([=] () {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                mapping->failAccessWithSegv();
            });
        
        // This statement should cause a segv
        BOOST_CHECK_EQUAL(data[0], 0);
    }
    else {
        checkSignal(SIGSEGV);
    }
}

#endif

BOOST_AUTO_TEST_CASE( test_exception_on_access )
{
    cerr << "starting test 1" << endl;
    //ML::Watchdog watchdog(5.0);

    auto & pfh = PageFaultHandler::instance();

    constexpr int NUM_PAGES = 8;
    
    auto mapping = pfh.addRange(NUM_PAGES * page_size);

    char * data = (char *)mapping->getMappingStart();

    // Map null data in
    mapping->setExceptionOnAccess([] (const void *) { throw Exception("hello"); });
    int x = 1;
    MLDB_TRACE_EXCEPTIONS(false);
    cerr << "throwing first exception" << endl;
    BOOST_CHECK_THROW(x = data[0], MLDB::Exception);
    BOOST_CHECK_EQUAL(x, 1);

    // Try a second exception to prove that the stack is properly cleaned
    // up
    cerr << "throwing second exception" << endl;
    BOOST_CHECK_THROW(x = data[0], MLDB::Exception);
    BOOST_CHECK_EQUAL(x, 1);

    cerr << "done test 1" << endl;
}

#if 0
BOOST_AUTO_TEST_CASE( test_exception_on_access_partial )
{
    cerr << "starting test 2" << endl;

    ML::Watchdog watchdog(5.0);

    auto & pfh = PageFaultHandler::instance();

    constexpr int NUM_PAGES = 8;
    
    auto mapping = pfh.addRange(NUM_PAGES * page_size);

    char * data = (char *)mapping->getMappingStart();

    //mapping->copyPages(0, NUM_PAGES / 2);
    
    // Map null data in
    mapping->setExceptionOnAccess([] (const void *) { throw Exception("hello"); });
    MLDB_TRACE_EXCEPTIONS(false);
    int x = 1;
    BOOST_CHECK_THROW(x = data[0], MLDB::Exception);
    BOOST_CHECK_EQUAL(x, 1);
}
#endif
