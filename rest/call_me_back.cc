// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* call_me_back.cc
   Jeremy Banres, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "call_me_back.h"
#include <iostream>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* CALL ME BACK LATER                                                        */
/*****************************************************************************/

CallMeBackLater::
CallMeBackLater()
    : shutdown_(true), queue(1024), thread(nullptr)
{
}

CallMeBackLater::
~CallMeBackLater()
{
    shutdown();
}

void
CallMeBackLater::
add(std::function<void () noexcept> fn)
{
    if (!thread) {
        start();
    }

    if (!queue.tryPush(std::move(fn)))
        throw std::runtime_error("no space in CallMeBackLater queue");
}

void
CallMeBackLater::
start()
{
    if (thread) return;
    std::unique_lock<std::mutex> guard(startMutex);
    if (thread) return;

    std::unique_ptr<std::thread> newThread;
    shutdown_ = false;
    newThread.reset(new std::thread([=] () { this->runMainThread(); }));

    thread = newThread.release();
}

void
CallMeBackLater::
shutdown()
{
    std::unique_lock<std::mutex> guard(startMutex);

    if (!thread)
        return;
    shutdown_ = true;
    add(nullptr);  // Make sure the thread wakes up
    thread->join();
    thread = 0;
}

void
CallMeBackLater::
runMainThread()
{
    while (!shutdown_) {
        auto fn = queue.pop();
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

CallMeBackLater callMeBackLater;

} // namespace MLDB
