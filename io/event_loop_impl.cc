// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* event_loop_impl.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "mldb/io/event_loop.h"
#include "event_loop_impl.h"

using namespace std;
using namespace boost;
using namespace MLDB;


/****************************************************************************/
/* EVENT LOOP IMPL                                                          */
/****************************************************************************/

EventLoopImpl::
EventLoopImpl()
    : work_(boost::asio::make_work_guard(ioContext_))
{
}

void
EventLoopImpl::
run()
{
    ioContext_.run();
}

void
EventLoopImpl::
terminate()
{
    // Not sure we really need to post this, I suspect we can just call work_.reset() directly
    auto clearFn = [&] {
        work_.reset();
    };
    boost::asio::post(ioContext_, clearFn);
    ioContext_.stop();
}
