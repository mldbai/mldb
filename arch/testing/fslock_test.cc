// fslock_test.cc
// Wolfgang Sourdeau - Dec 2013
// Copyright (c) 2013 mldb.ai inc. All rights reserved.
// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
// 


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <unistd.h>
#include <sys/wait.h>
#include <atomic>
#include <mutex>
#include <thread>
#include <errno.h>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <thread>
#include "mldb/arch/fslock.h"
#include "mldb/arch/exception.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/utils/environment.h"


using namespace std;
using namespace MLDB;

EnvOption<std::string> TMP("TMP", "./tmp");

void
cleanupLock(const string basename)
{
    string lockName(basename + ".lock");

    ::unlink(lockName.c_str());
}


/* stress test ensuring that the file mutex implementation is effective */
BOOST_AUTO_TEST_CASE( test_lock_race )
{
    Watchdog watchdog(10.0);

    const int numBlocks(10);
    const int numThreads(10);

    string lockedFile = TMP.get() + "/fs_lock_test";

    atomic<int> active[numBlocks];

    for (int i = 0; i < numBlocks; i++) {
        active[i] = 0;
    }

    auto raceTestFn = [&] (int blockN, int numThread) {
        GuardedFsLock lock(lockedFile + to_string(blockN));
        lock_guard<GuardedFsLock> guard(lock);

        atomic<int> & localActive = active[blockN];
        localActive++;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (localActive != 1) {
            ::fprintf(stderr, "test_lock_race: inconsistency found\n");
        }
        BOOST_CHECK_EQUAL(localActive, 1);
        localActive--;
    };

    vector<std::thread> threads;

    for (int b = 0; b < numBlocks; b++) {
        active[b] = 0;
        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back(raceTestFn, b, t);
        }
    }

    for (std::thread & th: threads) {
        th.join();
    }

    for (int i = 0; i < numBlocks; i++) {
        cleanupLock(lockedFile + to_string(i));
    }
}

#if (0 && defined(__linux__)) // requires robust mutexes to work
/* ensure that stale locks are handled properly using tryLock */
BOOST_AUTO_TEST_CASE( test_stale_and_trylock )
{
    Watchdog watchdog(10.0);

    // TestFolderFixture dir("stale_trylock");
    string lockedFile = "tmp/some_file";

    pid_t childPid = ::fork();
    if (childPid == -1) {
        throw MLDB::Exception(errno, "fork");
    }
    else if (childPid == 0) {
        GuardedFsLock lock(lockedFile);
        lock.lock();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        _exit(0);
    }
    else {
        GuardedFsLock lock(lockedFile);
        int status;
        waitpid(childPid, &status, 0);

        BOOST_CHECK_EQUAL(lock.tryLock(), true);
        cleanupLock(lockedFile);
    }
}

/* ensure that stale locks are handled properly using lock */
BOOST_AUTO_TEST_CASE( test_stale_and_lock )
{
    Watchdog watchdog(10.0);

    // TestFolderFixture dir("stale_lock");
    string lockedFile = "tmp/some_file";

    pid_t childPid = ::fork();
    if (childPid == -1) {
        throw MLDB::Exception(errno, "fork");
    }
    else if (childPid == 0) {
        GuardedFsLock lock(lockedFile);
        lock.lock();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        _exit(0);
    }
    else {
        GuardedFsLock lock(lockedFile);
        int status;
        waitpid(childPid, &status, 0);

        lock.lock();
        cleanupLock(lockedFile);
    }
}
#endif // LINUX
