/* event_loop_impl.h                                               -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/version.hpp>
#include <boost/asio/post.hpp>
#include "mldb/io/event_loop.h"


namespace MLDB {



/****************************************************************************/
/* EVENT LOOP IMPL                                                          */
/****************************************************************************/

struct EventLoopImpl {
    EventLoopImpl();

    boost::asio::io_context & ioContext()
    {
        return ioContext_;
    };

    void run();
    void terminate();

    void post(const EventLoop::JobFn & jobFn)
    {
        boost::asio::post(ioContext_, jobFn);
    }

private:
    boost::asio::io_context ioContext_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
};

} // namespace MLDB
