// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* asio_thread_pool.h                                              -*- C++ -*-
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Thread pool for ASIO workers.
*/

#pragma once

#include <memory>


namespace Datacratic {

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
    AsioThreadPool();

    ~AsioThreadPool();

    void shutdown();

    /** Ensure "minNumThreads" threads and event loops are active. Any
        value below 1 will throw an exception. This function must be
        called at least once in order to make event loops available to
        the caller.
    */
    void ensureThreads(int minNumThreads);

    /** Returns the next available EventLoop in the pool. Currently,
	loops are returned in a round-robin fashion. A call to this
	function without any available loop will throw an exception. */
    EventLoop & nextLoop();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace Datacratic
