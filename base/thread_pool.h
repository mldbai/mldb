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

typedef std::function<void ()> ThreadJob;

/** Return the number of CPUs in the system. */
int numCpus();


/*****************************************************************************/
/* THREAD POOL                                                               */
/*****************************************************************************/

/** Thread pool abstraction, to allow work to be farmed out over multiple
    threads.
*/

struct ThreadPool {
    /** Create a thread pool as a subordinate of the parent thread pool.  Jobs
        added to this pool will be run on the parent's threads.
    */
        
    ThreadPool(ThreadPool & parent = instance(), int numThreads = numCpus(),
               bool handleExceptions = true);
    ThreadPool(int numThreads, bool handleExceptions = true);
    ~ThreadPool();

    /** Add the given job to the thread pool.

        If the ThreadPool is constructued with handleExceptions = false:

            The job MUST NOT throw an exception; any exception thrown within the
            job WILL CRASH THE PROGRAM.  (The lambda will be marked noexcept at
            a later date).

        If the ThreadPool is constructed with handleExceptions = true:

            The job MAY throw an exception.  If it does, then the exception will
            be caught and stored.  The first exception thrown will be rethrown
            from waitForAll().

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
    
    /** Add the given job, automatically binding the given arguments. */
    template<typename Fn, typename Arg0, typename... Args>
    void add(Fn && fn, Arg0 && arg0, Args&&... args)
    {
        ThreadJob job([=] () { fn(arg0, args...); });
        add(std::move(job));
    }

    /** Wait for all of the work in the thread pool to be done before returning.
        If an exception is or has been thrown in a job, then it will be rethrown
        from this function.

        POST: no thread is running any work that was added to this thread pool.

        Returns:
          - If a job threw an exception then the function doesn't return, instead
            the first job exception is rethrown.
          - If abort() was called, then returns true
          - Otherwise, returns false
    */
    bool waitForAll() const;

    /** Abort all work in the thread pool.  This may be called from any thread.
        It's undefined as to how long the abort will take or whether jobs that
        are running will be forcibly killed.

        Returns immediately.  To wait on the abort being finished, waitForAll()
        will need to be called.

        It's not valid to call this on the root thread pool, only a subordinate
        thread pool.

        It's still OK to add jobs to a thread pool once abort() has been called,
        as these jobs may be required to unblock jobs that are currently running.
    */
    void abort() const;

    /** Returns true iff the current thread pool has been aborted. */
    bool aborted() const;

    /** Returns true iff the current thread pool has a pending exception. */
    bool hasException() const;
    
    /** Lend the calling thread to the thread pool for one job. */
    void work() const;

    /** Lend the calling thread to the thread pool for one job, or return
        immediately if there is no work to do. */
    void tryWork() const;

    size_t numThreads() const;

    uint64_t jobsRunning() const;
    uint64_t jobsSubmitted() const;
    uint64_t jobsFinished() const;
    uint64_t jobsFinishedWithException() const;
    
    uint64_t jobsStolen() const;
    uint64_t jobsWithFullQueue() const;
    uint64_t jobsRunLocally() const;

    static ThreadPool & instance();
    
private:
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
