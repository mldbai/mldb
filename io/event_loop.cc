// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* event_loop.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "event_loop_impl.h"
#include "event_loop.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* EvENT LOOP                                                               */
/****************************************************************************/

EventLoop::
EventLoop()
    : impl_(new EventLoopImpl())
{
}

EventLoop::
~EventLoop()
{
}

EventLoopImpl &
EventLoop::
impl()
{
    return *impl_;
}

void
EventLoop::
run()
{
    impl_->run();
}

void
EventLoop::
terminate()
{
    impl_->terminate();
}

void
EventLoop::
post(const JobFn & jobFn)
{
    impl_->post(jobFn);
}
