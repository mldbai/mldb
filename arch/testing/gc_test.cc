/* gc_test.cc
   Jeremy Barnes, 23 February 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of the garbage collector locking.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "gc_lock_test_common.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/arch/thread_specific.h"
#include "mldb/arch/rwlock.h"
#include "mldb/arch/spinlock.h"
#include "mldb/arch/tick_counter.h"
#include <boost/test/unit_test.hpp>
#include <iostream>
#include <atomic>


using namespace ML;
using namespace MLDB;
using namespace std;

// Defined in gc_lock.cc
namespace MLDB {
extern int32_t gcLockStartingEpoch;
};

#if 1

BOOST_AUTO_TEST_CASE ( test_gc )
{
    GcLock gc;

    cerr << endl << "before lock" << endl;
    gc.dump();

    gc.lockShared();

    BOOST_CHECK(gc.isLockedShared());

    std::atomic<int> deferred(false);

    cerr << endl << "before defer" << endl;
    gc.dump();

    gc.defer([&] () { deferred = true; });

    cerr << endl << "after defer" << endl;
    gc.dump();

    gc.unlockShared();

    cerr << endl << "after unlock shared" << endl;
    gc.dump();

    BOOST_CHECK(!gc.isLockedShared());
    BOOST_CHECK(deferred);
}

BOOST_AUTO_TEST_CASE(test_mutual_exclusion)
{
    cerr << "testing mutual exclusion" << endl;

    GcLock lock;
    std::atomic<bool> finished(false);
    std::atomic<int> numExclusive(0);
    std::atomic<int> numShared(0);
    std::atomic<int> errors(0);
    std::atomic<int> multiShared(0);
    std::atomic<int> sharedIterations(0);
    std::atomic<uint64_t> exclusiveIterations(0);

    auto sharedThread = [&] ()
        {
            while (!finished) {
                GcLock::SharedGuard guard(lock);
                numShared += 1;

                if (numExclusive > 0) {
                    cerr << "exclusive and shared" << endl;
                    errors += 1;
                }
                if (numShared > 1) {
                    multiShared += 1;
                }

                numShared -= 1;
                sharedIterations += 1;
                std::atomic_thread_fence(std::memory_order_seq_cst);
            }
        };

    auto exclusiveThread = [&] ()
        {
            while (!finished) {
                GcLock::ExclusiveGuard guard(lock);
                numExclusive += 1;

                if (numExclusive > 1) {
                    cerr << "more than one exclusive" << endl;
                    errors += 1;
                }
                if (numShared > 0) {
                    cerr << "exclusive and shared" << endl;
                    multiShared += 1;
                }

                numExclusive -= 1;
                exclusiveIterations += 1;
                std::atomic_thread_fence(std::memory_order_seq_cst);
            }
        };

    lock.getEntry();

    int nthreads = 4;

    {
        cerr << "single shared" << endl;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        tg.create_thread(sharedThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "multi shared" << endl;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(sharedThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        if (nthreads > 1)
            BOOST_CHECK_GE(multiShared, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "single exclusive" << endl;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        tg.create_thread(exclusiveThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "multi exclusive" << endl;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(exclusiveThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "mixed shared and exclusive" << endl;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(sharedThread);
        for (unsigned i = 0;  i < nthreads;  ++i)
            tg.create_thread(exclusiveThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        if (nthreads > 1)
            BOOST_CHECK_GE(multiShared, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "overflow" << endl;
        gcLockStartingEpoch = 0xFFFFFFF0;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        tg.create_thread(sharedThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "INT_MIN to INT_MAX" << endl;
        gcLockStartingEpoch = 0x7FFFFFF0;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        tg.create_thread(sharedThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

    {
        cerr << "benign overflow" << endl;
        gcLockStartingEpoch = 0xBFFFFFF0;
        sharedIterations = exclusiveIterations = multiShared = finished = 0;
        ThreadGroup tg;
        tg.create_thread(sharedThread);
        sleep(1);
        finished = true;
        tg.join_all();
        BOOST_CHECK_EQUAL(errors, 0);
        cerr << "iterations: shared " << sharedIterations
             << " exclusive " << exclusiveIterations << endl;
        cerr << "multiShared = " << multiShared << endl;
    }

}

#endif

#if 1
BOOST_AUTO_TEST_CASE ( test_gc_sync_many_threads_contention )
{
    cerr << "testing contention synchronized GcLock with many threads" << endl;

    int nthreads = 8;
    int nSpinThreads = 16;
    int nblocks = 2;

    TestBase<GcLock> test(nthreads, nblocks, nSpinThreads);
    test.run(std::bind(&TestBase<GcLock>::allocThreadSync, &test,
                       std::placeholders::_1));
}
#endif

BOOST_AUTO_TEST_CASE ( test_gc_deferred_contention )
{
    cerr << "testing contended deferred GcLock" << endl;

    int nthreads = 8;
    int nSpinThreads = 0;//16;
    int nblocks = 2;

    TestBase<GcLock> test(nthreads, nblocks, nSpinThreads);
    test.run(std::bind(&TestBase<GcLock>::allocThreadDefer, &test,
                       std::placeholders::_1));
}


#if 1

BOOST_AUTO_TEST_CASE ( test_gc_sync )
{
    cerr << "testing synchronized GcLock" << endl;

    int nthreads = 2;
    int nblocks = 2;

    TestBase<GcLock> test(nthreads, nblocks);
    test.run(std::bind(&TestBase<GcLock>::allocThreadSync, &test,
                       std::placeholders::_1));
}

BOOST_AUTO_TEST_CASE ( test_gc_sync_many_threads )
{
    cerr << "testing synchronized GcLock with many threads" << endl;

    int nthreads = 8;
    int nblocks = 2;

    TestBase<GcLock> test(nthreads, nblocks);
    test.run(std::bind(&TestBase<GcLock>::allocThreadSync, &test,
                       std::placeholders::_1));
}

BOOST_AUTO_TEST_CASE ( test_gc_deferred )
{
    cerr << "testing deferred GcLock" << endl;

    int nthreads = 2;
    int nblocks = 2;

    TestBase<GcLock> test(nthreads, nblocks);
    test.run(std::bind(&TestBase<GcLock>::allocThreadDefer, &test,
                       std::placeholders::_1));
}

BOOST_AUTO_TEST_CASE ( test_defer_race )
{
    cerr << "testing defer race" << endl;
    GcLock gc;

    ThreadGroup tg;

    volatile bool finished = false;

    int nthreads = 0;

    std::atomic<int> numStarted(0);

    auto doTestThread = [&] ()
        {
            while (!finished) {
                numStarted += 1;
                while (numStarted != nthreads) ;

                gc.deferBarrier();

                numStarted -= 1;
                while (numStarted != 0) ;
            }
        };


    for (unsigned i = 0;  i < nthreads;  ++i)
        tg.create_thread(doTestThread);

    int runTime = 1;

    sleep(runTime);

    finished = true;

    tg.join_all();
}

#endif
