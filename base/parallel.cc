/** parallel.cc                                                    -*- C++ -*-
    Jeremy Barnes, 5 February 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "parallel.h"
#include "mldb/compiler/compiler.h"
#include "mldb/base/exc_assert.h"
#include "thread_pool.h"
#include <atomic>
#include <mutex>

namespace MLDB {


void parallelMap(size_t first, size_t last,
                 const std::function<void (size_t)> & doWork,
                 int occupancyLimit)
{
    ExcAssertGreaterEqual(last, first);
    ExcAssertLess((last - first), 1ULL << 31);

    std::atomic<int> hasException(0);
    std::atomic<size_t> index(first);
    std::mutex exc_mutex;
    std::exception_ptr exc;

    // This creates a thread pool that runs jobs on the default thread pool
    ThreadPool tp;

    if (occupancyLimit == -1)
        occupancyLimit = numCpus();
    if (occupancyLimit > (last - first))
        occupancyLimit = (last - first);

    auto worker = [&] ()
        {
            while (!hasException.load(std::memory_order_relaxed)) {
                size_t myindex = index.fetch_add(1);
                if (myindex >= last)
                    return;
                try {
                    doWork(myindex);
                } MLDB_CATCH_ALL {
                    if (hasException.fetch_add(1) == 0) {
                        ExcAssert(!exc);
                        exc = std::current_exception();
                    }
                }
            }
        };

    // Leave one set of work for this thread to do directly
    for (int i = 0;  i < occupancyLimit - 1;  ++i)
        tp.add(worker);
    
    // Do work until there is nothing left to do
    worker();

    // Wait for the rest of the work to be done
    tp.waitForAll();

    if (exc)
        std::rethrow_exception(exc);
}

bool parallelMapHaltable(size_t first, size_t last,
                         const std::function<bool (size_t)> & doWork,
                         int occupancyLimit)
{
    ExcAssertGreaterEqual(last, first);
    ExcAssertLess((last - first), 1ULL << 31);

    std::atomic<int> hasException(0);
    std::atomic<int> stop(0);
    std::atomic<size_t> index(first);
    std::mutex exc_mutex;
    std::exception_ptr exc;

    // This creates a thread pool that runs jobs on the default thread pool
    ThreadPool tp;

    if (occupancyLimit == -1)
        occupancyLimit = numCpus();
    if (occupancyLimit > (last - first))
        occupancyLimit = (last - first);

    auto worker = [&] ()
        {
            while (!stop.load(std::memory_order_relaxed)
                   && !hasException.load(std::memory_order_relaxed)) {
                size_t myindex = index.fetch_add(1);
                if (myindex >= last)
                    return;
                try {
                    if (!doWork(myindex)) {
                        stop = true;
                        return;
                    }
                } MLDB_CATCH_ALL {
                    if (hasException.fetch_add(1) == 0) {
                        ExcAssert(!exc);
                        exc = std::current_exception();
                    }
                    return;
                }
            }
        };

    // Leave one set of work for this thread to do directly
    for (int i = 0;  i < occupancyLimit - 1;  ++i)
        tp.add(worker);
    
    // Do work until there is nothing left to do
    worker();

    // Wait for the rest of the work to be done
    tp.waitForAll();

    if (exc)
        std::rethrow_exception(exc);

    return !stop;
}


void parallelMapChunked(size_t first, size_t last, size_t chunkSize,
                        const std::function<void (size_t, size_t)> & doWork,
                        int occupancyLimit)
{
    ExcAssertGreater(chunkSize, 0);
    ExcAssertGreater(last, first);
    ExcAssertLess((last - first) / chunkSize, 1ULL << 31);

    std::atomic<int> hasException(0);
    std::atomic<size_t> index(first);
    std::mutex exc_mutex;
    std::exception_ptr exc;

    // This creates a thread pool that runs jobs on the default thread pool
    ThreadPool tp;
    
    if (occupancyLimit == -1)
        occupancyLimit = numCpus();
    if (occupancyLimit > (last - first + chunkSize - 1) / chunkSize)
        occupancyLimit = (last - first + chunkSize - 1) / chunkSize;

    auto worker = [&] ()
        {
            while (!hasException.load(std::memory_order_relaxed)) {
                size_t myindex = index.fetch_add(chunkSize);
                if (myindex >= last)
                    return;
                size_t indexEnd = std::min(last, myindex + chunkSize);
                try {
                    doWork(myindex, indexEnd);
                } MLDB_CATCH_ALL {
                    if (hasException.fetch_add(1) == 0) {
                        ExcAssert(!exc);
                        exc = std::current_exception();
                    }
                }
            }
        };

    // Leave one set of work for this thread to do directly
    for (int i = 0;  i < occupancyLimit - 1;  ++i)
        tp.add(worker);
    
    // Do work until there is nothing left to do
    worker();
    
    // Wait for the rest of the work to be done
    tp.waitForAll();

    if (exc)
        std::rethrow_exception(exc);
}

} // namespace MLDB
