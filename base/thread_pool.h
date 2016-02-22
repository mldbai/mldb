/** thread_pool.h                                                  -*- C++ -*-
    Jeremy Barnes, 18 December 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Implementation of the thread pool abstraction for when work needs to be
    spread over multiple threads.
*/

#pragma once

#include <functional>
#include <memory>

namespace Datacratic {

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
    ThreadPool(ThreadPool & parent = instance(), int numThreads = numCpus());
    ThreadPool(int numThreads);
    ~ThreadPool();

    void add(ThreadJob job);

    void waitForAll() const;

    void work() const;

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

} // namespace Datacratic
