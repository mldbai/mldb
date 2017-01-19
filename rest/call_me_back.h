// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* call_me_back.h                                                  -*- C++ -*-
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Code to implement functionality to call a call back later in another
   thread.  Used to defer processing that can't happen in a given thread.
*/

#pragma once

#include <thread>
#include <functional>
#include <mutex>
#include "mldb/jml/utils/ring_buffer.h"


namespace MLDB {


/*****************************************************************************/
/* CALL ME BACK LATER                                                        */
/*****************************************************************************/

/** Simple class that allows something to ask a callback to be made in a
    different thread later on.
*/

struct CallMeBackLater {
    CallMeBackLater();

    ~CallMeBackLater();

    /** Run the given function some time later (but probably very soon)
        in a different thread from the calling thread.  See add().
    */
    void operator () (std::function<void () noexcept> fn)
    {
        add(std::move(fn));
    }

    /** Run the given function some time later (but probably very soon)
        in a different thread from the calling thread.  The function should
        be fast executing.

        This is non-blocking and lock-free and will throw an exception if
        the queue is full and it can't be enqueued.

        If the function throws an exception while executing, the program
        will be aborted.  You should catch any exceptions and handle them
        within the callback function.
    */
    void add(std::function<void () noexcept> fn);

    /** Start up the main thread.  Normally this will be done on the first
        call to add().  Thread safe.
    */
    void start();

    /** Shutdown the main thread.  Normally done on destruction.  Not thread
        safe.
    */
    void shutdown();

private:
    std::mutex startMutex;
    bool shutdown_;
    ML::RingBufferSRMW<std::function<void ()> > queue;
    std::thread * thread;

    void runMainThread();
};

extern CallMeBackLater callMeBackLater;


} // namespace MLDB
