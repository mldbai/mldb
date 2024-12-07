// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* call_me_back.cc
   Jeremy Banres, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "call_me_back.h"
#include <iostream>
#include <mutex>
#include "mldb/ext/concurrentqueue/blockingconcurrentqueue.h"
#include "mldb/base/exc_assert.h"

using namespace std;
using moodycamel::BlockingConcurrentQueue;

namespace MLDB {


/*****************************************************************************/
/* CALL ME BACK LATER                                                        */
/*****************************************************************************/

struct CallMeBackLater::Itl {
    Itl()
        : shutdown_(true), queue(1024), thread(nullptr)
    {
    }

    ~Itl()
    {
        shutdown();
    }
    
    std::mutex startMutex;
    bool shutdown_;
    BlockingConcurrentQueue<std::function<void ()> > queue;
    std::thread * thread;
    
    void start()
    {
        if (thread) return;
        std::unique_lock<std::mutex> guard(startMutex);
        if (thread) return;

        std::unique_ptr<std::thread> newThread;
        shutdown_ = false;
        newThread.reset(new std::thread([=,this] () { this->runMainThread(); }));

        thread = newThread.release();
    }

    void shutdown()
    {
        std::unique_lock<std::mutex> guard(startMutex);

        if (!thread)
            return;
        shutdown_ = true;
        add(nullptr);  // Make sure the thread wakes up
        thread->join();
        thread = 0;
    }

    void add(std::function<void ()> fn)
    {
        if (!thread) {
            start();
        }
        
        queue.enqueue(std::move(fn));
    }

    void runMainThread()
    {
        while (!shutdown_) {
            std::function<void ()> fn;
            queue.wait_dequeue(fn);
            if (!fn)
                return;
            if (shutdown_)
                return;

            try {
                fn();
            } catch (const std::exception & exc) {
                cerr << "ERROR: function in CallMeBackLater threw: "
                     << exc.what() << endl;
                abort();
            } catch (...) {
                cerr << "ERROR: function in CallMeBackLater threw" << endl;
                abort();
            }
        }
    }
};

CallMeBackLater::
CallMeBackLater()
    : itl(new Itl())
{
}

CallMeBackLater::
~CallMeBackLater()
{
}

void
CallMeBackLater::
add(std::function<void ()> fn)
{
    ExcAssert(fn);
    itl->add(std::move(fn));
}


CallMeBackLater callMeBackLater;

} // namespace MLDB
