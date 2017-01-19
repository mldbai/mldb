// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_thread_pool.h                                              -*- C++ -*-
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Thread pool for ASIO workers.
*/

#pragma once

#include "mldb/watch/watch.h"


namespace MLDB {

/* Forward declarations */
struct EventLoop;


/*****************************************************************************/
/* ASIO THREAD POOL                                                          */
/*****************************************************************************/

/** This class supplies a pool of worker threads to the give ASIO IO service
    object.  The pool will be scaled automatically to keep the latency of
    submitted operations low.
*/

struct AsioThreadPool {
    AsioThreadPool(EventLoop & eventLoop,
                   double probeIntervalSeconds = 0.1);

    ~AsioThreadPool();

    void shutdown();

    /** Ensure that there are at least this number of threads active.
        Necessary to avoid deadlocks in some situations.
    */
    void ensureThreads(int minNumThreads);

    struct Stats {
        double duty;
        int numThreadsRunning;
        double numThreadsRequired;
        double latency;
    };

    WatchT<Stats> watchStats();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

DECLARE_STRUCTURE_DESCRIPTION_NAMED(AsioThreadPoolStatsDescription, AsioThreadPool::Stats);

} // namespace MLDB
