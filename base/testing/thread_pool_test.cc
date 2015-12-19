/** thread_pool_test.cc
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Test of the thread pool.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/base/thread_pool.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/worker_task.h"

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <thread>
#include <cassert>
#include <iostream>

using namespace std;
using namespace Datacratic;

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

BOOST_AUTO_TEST_CASE (thread_pool_no_busy_looping)
{
    ThreadPool threadPool(10);
    BOOST_CHECK_EQUAL(threadPool.jobsSubmitted(), 0);
    std::atomic<int> finished(0);

    ML::Timer timer;

    threadPool.add([&] () {std::this_thread::sleep_for(std::chrono::milliseconds(100)); finished = 1;});
 
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

    ML::Timer timer;

    for (uint64_t i = 0;  i < numJobs;  ++i)
        pool.add(work);

    pool.waitForAll();

    cerr << "elapsed " << timer.elapsed() << endl;

    cerr << "local: " << pool.jobsRunLocally() << " stolen: " << pool.jobsStolen()
         << " full queue: " << pool.jobsWithFullQueue() << endl;

    BOOST_CHECK_EQUAL(jobsDone.load(), numJobs);
}

