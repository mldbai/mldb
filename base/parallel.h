/** parallel.h                                                     -*- C++ -*-
    Jeremy Barnes, 5 February 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include "thread_pool.h"
#include <atomic>
#include <mutex>

namespace Datacratic {

/** Run a set of jobs in multiple threads.  The iterator will be iterated
    through the range and the doWork function will be called with each
    value of the iterator in a different thread.
*/
template<typename It, typename It2, typename Fn>
void parallelMap(It first, It2 last, Fn doWork)
{
    std::atomic<int> has_exc(0);
    std::mutex exc_mutex;
    std::exception_ptr exc;

    // This creates a thread pool that runs jobs on the default thread pool
    ThreadPool tp;
    
    for (auto it = first;  it != last;  ++it) {
        tp.add([&,it] ()
               {
                   if (has_exc.load(std::memory_order_relaxed))
                       return;
                   try {
                       doWork(it);
                   } JML_CATCH_ALL {
                       has_exc = 1;
                       std::unique_lock<std::mutex> guard(exc_mutex);
                       if (!exc)
                           exc = std::current_exception();
                   }
               });
    }
    
    tp.waitForAll();

    if (exc)
        std::rethrow_exception(exc);
}

template<typename It, typename It2, typename Fn>
void parallelMapChunked(It first, It2 last, size_t chunkSize, Fn doWork)
{
    std::atomic<int> has_exc(0);
    std::mutex exc_mutex;
    std::exception_ptr exc;

    // This creates a thread pool that runs jobs on the default thread pool
    ThreadPool tp;

    for (auto it = first;  it < last;  it += chunkSize) {
        auto end = std::min<It>(it + chunkSize, last);
        tp.add([&,it,end] ()
               {
                   if (has_exc.load(std::memory_order_relaxed))
                       return;
                   try {
                       doWork(it, end);
                   } JML_CATCH_ALL {
                       has_exc = 1;
                       std::unique_lock<std::mutex> guard(exc_mutex);
                       if (!exc)
                           exc = std::current_exception();
                   }
               });
    }
    
    tp.waitForAll();

    if (exc)
        std::rethrow_exception(exc);
}

} // namespace Datacratic
