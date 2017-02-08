/** thread_pool.h                                                  -*- C++ -*-
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of the thread pool abstraction for when work needs to be
    spread over multiple threads.
*/

#pragma once

#include <functional>
#include <memory>

namespace MLDB {

typedef std::function<void () noexcept> ThreadJob;

/** Return the number of CPUs in the system. */
int numCpus();


/*****************************************************************************/
/* THREAD POOL                                                               */
/*****************************************************************************/

/** Thread pool abstraction, to allow work to be farmed out over multiple
    threads.
*/

struct ThreadPool {
    ThreadPool(ThreadPool & parent = instance(), int numThreads = numCpus());
    ThreadPool(int numThreads);
    ~ThreadPool();

    /** Add the given job to the thread pool.

        The job MUST NOT throw an exception; any exception thrown within the
        job WILL CRASH THE PROGRAM.  (The lambda will be marked noexcept at
        a later date).

        The job may either:
        1.  Be run before the add() function terminates
        2.  Be enqueued and run later

        If there is an exception setting the job up to run (eg, out of memory
        allocating an object), then the exception will be returned from this
        function and will NOT have run.

        If this function returns, the job WILL be eventually run, or has
        already been run.
    */
    void add(ThreadJob job);

    void waitForAll() const;

    void work() const;

    size_t numThreads() const;

    uint64_t jobsRunning() const;
    uint64_t jobsSubmitted() const;
    uint64_t jobsFinished() const;

    uint64_t jobsStolen() const;
    uint64_t jobsWithFullQueue() const;
    uint64_t jobsRunLocally() const;

    static ThreadPool & instance();
    
private:
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
