/** thread_pool_test.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Test of the thread pool.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/base/thread_pool.h"
#include "mldb/base/thread_pool_impl.h"
#include "mldb/arch/timers.h"
#include "mldb/base/exc_assert.h"
#include "mldb/base/parallel.h"

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <thread>
#include <cassert>
#include <iostream>

using namespace std;
using namespace MLDB;

// MLDB-1579
BOOST_AUTO_TEST_CASE (test_threads_disappearing_jobs_run)
{
    ThreadPool threadPool(1);

    std::atomic<uint64_t> jobsSubmitted(0);
    std::atomic<uint64_t> jobsRun(0);

    auto doJob = [&] ()
        {
            ++jobsRun;
        };

    auto runThread = [&] ()
        {
            threadPool.add(doJob);
            ++jobsSubmitted;
            // Now exit our thread
        };

    for (int i = 0;  i < 1000;  ++i) {
        std::thread(runThread).detach();
    }

    while (jobsSubmitted < 1000) ;

    threadPool.waitForAll();
    
    BOOST_CHECK_EQUAL(jobsRun, 1000);
}

//Failing depending on availability of cores...
BOOST_AUTO_TEST_CASE (thread_pool_idle_cpu_usage)
{
    static const char * env_name = getenv("MLDB_NO_TIMING_TESTS");
    if (env_name)
        return;

    ThreadPool threadPool(32);
    // let it start up and settle down
    std::this_thread::sleep_for(std::chrono::seconds(1));
    Timer timer;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    double elapsedCpu = timer.elapsed_cpu();
    double elapsedWall = timer.elapsed_wall();
    double cores = elapsedCpu / elapsedWall;
    cerr << "idle thread pool used " << cores * 100
         << "% cores" << endl;
    BOOST_CHECK_LE(cores, 0.01);
}

BOOST_AUTO_TEST_CASE (thread_pool_startup_shutdown_one_job)
{
    ThreadPool threadPool(1);
    BOOST_CHECK_EQUAL(threadPool.jobsSubmitted(), 0);

    threadPool.add([] () {});
    
    BOOST_CHECK_EQUAL(threadPool.jobsSubmitted(), 1);

    cerr << "before wait" << endl;
    threadPool.waitForAll();
    cerr << "after wait" << endl;

    BOOST_CHECK_EQUAL(threadPool.jobsFinished(), 1);
    BOOST_CHECK_EQUAL(threadPool.jobsRunning(), 0);

    cerr << "destroying" << endl;
}

BOOST_AUTO_TEST_CASE (thread_pool_startup_shutdown)
{
    ThreadPool threadPool;
    BOOST_CHECK_EQUAL(threadPool.jobsRunning(), 0);
}

BOOST_AUTO_TEST_CASE (thread_pool_startup_shutdown_zero_threads)
{
    ThreadPool threadPool(0);
    BOOST_CHECK_EQUAL(threadPool.jobsRunning(), 0);
}

//Failing depending on availability of cores...
BOOST_AUTO_TEST_CASE (thread_pool_no_busy_looping)
{
    static const char * env_name = getenv("MLDB_NO_TIMING_TESTS");
    if (env_name)
        return;

    ThreadPool threadPool(10);
    BOOST_CHECK_EQUAL(threadPool.jobsSubmitted(), 0);
    std::atomic<int> finished(0);

    Timer timer;

    threadPool.add([&] () {std::this_thread::sleep_for(std::chrono::milliseconds(1000)); finished = 1;});
 
    threadPool.waitForAll();

    BOOST_CHECK_EQUAL(finished, 1);
    
    cerr << "elapsed " << timer.elapsed() << endl;

    double cpus = timer.elapsed_cpu() / timer.elapsed_wall();

    cerr << "ran on " << cpus << " cpus" << endl;

    // Check that we're spending less than a CPU, whcih means that we're
    // not busy waiting.
    BOOST_CHECK_LT(cpus, 0.8);
}

BOOST_AUTO_TEST_CASE(thread_pool_test)
{
    std::atomic<uint64_t> jobsDone(0);

    auto work = [&] ()
        {
            uint64_t doneBefore = jobsDone.fetch_add(1);
            if (doneBefore && doneBefore % 100000 == 0)
                cerr << "done " << doneBefore << endl;
        };
    
    ThreadPool pool(10);
    
    uint64_t numJobs = 1000000;

    Timer timer;

    for (uint64_t i = 0;  i < numJobs;  ++i)
        pool.add(work);

    pool.waitForAll();

    cerr << "elapsed " << timer.elapsed() << endl;

    cerr << "local: " << pool.jobsRunLocally() << " stolen: " << pool.jobsStolen()
         << " full queue: " << pool.jobsWithFullQueue() << endl;

    BOOST_CHECK_EQUAL(jobsDone.load(), numJobs);
}

BOOST_AUTO_TEST_CASE(threadPoolBindingAdd)
{
    std::atomic<int> result(0);

    auto job = [&] (int i)
        {
            result = i;
        };

    ThreadPool threadPool;
    threadPool.add(job, 1);
    threadPool.waitForAll();

    BOOST_CHECK_EQUAL(result, 1);

    threadPool.add(job, 2);
    threadPool.waitForAll();
    
    BOOST_CHECK_EQUAL(result, 2);
}

BOOST_AUTO_TEST_CASE(threadPoolExceptionCatching)
{
    ThreadWorkGroup workGroup;

    workGroup.add([] () { throw Exception("hello"); });

    bool caught = false;
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        workGroup.waitForAll();
    } catch (const Exception & exc) {
        caught = true;
        BOOST_CHECK_EQUAL(exc.what(), "hello");
    }

    BOOST_CHECK(caught);
}

BOOST_AUTO_TEST_CASE(threadPoolMultiExceptionCatching)
{
    ThreadWorkGroup workGroup;

    for (size_t i = 0;  i < 100;  ++i) {
        workGroup.add([] () { throw Exception("hello"); });
    }

    bool caught = false;
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        workGroup.waitForAll();
    } catch (const Exception & exc) {
        caught = true;
        BOOST_CHECK_EQUAL(exc.what(), "hello");
    }

    BOOST_CHECK(caught);
    BOOST_CHECK_EQUAL(workGroup.jobsFinishedWithException(), 100);
}

