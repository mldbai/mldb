/* event_loop_impl.h                                               -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <boost/asio/io_context.hpp>
#include "mldb/io/event_loop.h"


namespace MLDB {

/****************************************************************************/
/* EVENT LOOP IMPL                                                          */
/****************************************************************************/

struct EventLoopImpl {
    boost::asio::io_context & ioContext()
    {
        return ioContext_;
    };

    void run();
    void terminate();

    void post(const EventLoop::JobFn & jobFn)
    {
        ioContext_.post(jobFn);
    }

private:
    std::unique_ptr<boost::asio::io_context::work> work_;
    boost::asio::io_context ioContext_;
};

} // namespace MLDB
