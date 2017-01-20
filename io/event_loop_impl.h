// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* event_loop_impl.h                                               -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include <boost/asio/io_service.hpp>
#include "mldb/io/event_loop.h"


namespace MLDB {

/****************************************************************************/
/* EVENT LOOP IMPL                                                          */
/****************************************************************************/

struct EventLoopImpl {
    boost::asio::io_service & ioService()
    {
        return ioService_;
    };

    void run();
    void terminate();

    void post(const EventLoop::JobFn & jobFn)
    {
        ioService_.post(jobFn);
    }

private:
    std::unique_ptr<boost::asio::io_service::work> work_;
    boost::asio::io_service ioService_;
};

} // namespace MLDB
