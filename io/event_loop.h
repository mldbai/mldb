// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* event_loop.h                                                    -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   An event loop.
*/

#pragma once

#include <memory>


namespace MLDB {

/* Forward declarations */
struct EventLoopImpl;


/****************************************************************************/
/* EVENT LOOP                                                               */
/****************************************************************************/

struct EventLoop {
    typedef std::function<void ()> JobFn;

    EventLoop();
    ~EventLoop();

    EventLoopImpl & impl();

    /** Wait for and process events indefinitely or until "terminate" is
     * called. */
    void run();

    /** Requests the event loop to stop as soon as no event are left to
       process. */
    void terminate();

    /** Post a job to be executed when the loop is idle. */
    void post(const JobFn & jobFn);

private:
    std::unique_ptr<EventLoopImpl> impl_;
};

} // namespace MLDB
