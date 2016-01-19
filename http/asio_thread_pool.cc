// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* asio_thread_pool.h                                              -*- C++ -*-
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Thread pool for ASIO workers.
*/

#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>
#include "mldb/base/exc_assert.h"
#include "asio_thread_pool.h"
#include "event_loop.h"


using namespace std;


namespace Datacratic {


/*****************************************************************************/
/* ASIO THREAD POOL                                                          */
/*****************************************************************************/

struct AsioThreadPool::Impl {
    Impl()
        : loopCnt(0)
    {
    }

    ~Impl()
    {
        try {
            for (size_t i = 0; i < threads.size(); i++) {
                loops[i]->terminate();
                threads[i].join();
            }
        }
        catch (...) {
            abort();
        }
    }

    void ensureThreads(int minNumThreads)
    {
        std::unique_lock<std::mutex> guard(threadsLock);
        for (size_t i = threads.size(); i < minNumThreads; i++) {
            loops.emplace_back(new EventLoop());
            EventLoop & loop = *loops.back();
            threads.emplace_back([&] () { loop.run(); });
        }
    }

    EventLoop & nextLoop()
    {
        std::unique_lock<std::mutex> guard(threadsLock);
        ExcAssert(loops.size() > 0);
        EventLoop & loop = *loops[loopCnt];
        loopCnt = (loopCnt + 1) % loops.size();
        return loop;
    }

    std::mutex threadsLock;
    std::vector<std::thread> threads;
    std::vector<std::unique_ptr<EventLoop> > loops;
    unsigned int loopCnt;
};

AsioThreadPool::
AsioThreadPool()
    : impl(new Impl())
{
}

AsioThreadPool::
~AsioThreadPool()
{
}

void
AsioThreadPool::
shutdown()
{
    impl.reset();
}

void
AsioThreadPool::
ensureThreads(int numThreads)
{
    ExcAssertGreaterEqual(numThreads, 1);
    impl->ensureThreads(numThreads);
}

EventLoop &
AsioThreadPool::
nextLoop()
{
    return impl->nextLoop();
}

} // namespace Datacratic
