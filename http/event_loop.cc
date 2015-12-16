// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* event_loop.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.
*/

#include <iostream>
#include "event_loop_impl.h"
#include "event_loop.h"

using namespace std;
using namespace Datacratic;


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
