/* gc_test.cc
   Jeremy Barnes, 23 February 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of the garbage collector locking.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/shared_gc_lock.h"
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

struct SharedGcLockProxy : public SharedGcLock {
    static const char* name;
    SharedGcLockProxy() :
        SharedGcLock(GC_OPEN, name)
    {}
};
const char* SharedGcLockProxy::name = "gc_test.dat";

BOOST_AUTO_TEST_CASE( test_shared_lock_sync )
{
    cerr << "testing contention synchronized GcLock with shared lock" << endl;

    SharedGcLock lockGuard(GC_CREATE, SharedGcLockProxy::name);
    Call_Guard unlink_guard([&] { lockGuard.unlink(); });

    int nthreads = 8;
    int nSpinThreads = 16;
    int nblocks = 2;

    TestBase<SharedGcLockProxy> test(nthreads, nblocks, nSpinThreads);
    test.run(std::bind(
                    &TestBase<SharedGcLockProxy>::allocThreadSync, &test,
                    std::placeholders::_1));

}

BOOST_AUTO_TEST_CASE( test_shared_lock_defer )
{
    cerr << "testing contended deferred GcLock with shared lock" << endl;

    SharedGcLock lockGuard(GC_CREATE, SharedGcLockProxy::name);
    Call_Guard unlink_guard([&] { lockGuard.unlink(); });

    int nthreads = 8;
    int nSpinThreads = 16;
    int nblocks = 2;

    TestBase<SharedGcLockProxy> test(nthreads, nblocks, nSpinThreads);
    test.run(std::bind(
                    &TestBase<SharedGcLockProxy>::allocThreadSync, &test,
                    std::placeholders::_1));
}
