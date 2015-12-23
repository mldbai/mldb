/* standalone_peer_server.cc
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "standalone_peer_server.h"
#include "mldb/http/asio_thread_pool.h"
#include "mldb/http/asio_timer.h"
#include "mldb/http/event_loop.h"
#include "mldb/http/event_loop_impl.h"
#include "mldb/jml/utils/string_functions.h"
#include <atomic>

using namespace std;


namespace Datacratic {

/*****************************************************************************/
/* STANDALONE PEER SERVER                                                    */
/*****************************************************************************/

struct StandalonePeerServer::Impl {
    Impl(StandalonePeerServer * server)
        : server(server),
          shutdown_(false)
    {
        threads.ensureThreads(4);  // So connect and accept can happen
    }

    ~Impl()
    {
        shutdown();
    }

    StandalonePeerServer * server;

    void shutdown()
    {
        shutdown_ = true;
        eventLoop.terminate();
        threads.shutdown();
    }

    WatchT<Date> getTimer(Date expiry, double period,
                          std::function<void (Date)> toBind)
    {
        return Datacratic::getTimer(expiry, period, eventLoop, toBind);
    }

    EventLoop eventLoop;
    AsioThreadPool threads;
    std::atomic<int> shutdown_;
};

StandalonePeerServer::
StandalonePeerServer()
    : impl(new Impl(this))
{
}

StandalonePeerServer::
~StandalonePeerServer()
{
}

PeerInfo
StandalonePeerServer::
listen(PeerInfo info)
{
    return info;
}

void
StandalonePeerServer::
shutdown()
{
    impl->shutdown();
}

std::shared_ptr<PeerConnection>
StandalonePeerServer::
connect(const PeerInfo & info)
{
    return connectToSelf();
    //throw ML::Exception("StandalonePeerServer can only connect to itself");
}

std::shared_ptr<PeerConnection>
StandalonePeerServer::
connectToSelf()
{
    auto & ioService = impl->eventLoop.impl().ioService();
    return std::make_shared<MirrorPeerConnection>(ioService);
}

void
StandalonePeerServer::
postWork(std::function<void ()> work)
{
    impl->eventLoop.post(work);
}

void
StandalonePeerServer::
setNewConnectionHandler(std::function<void (std::shared_ptr<PeerConnection>)> onNewConnection)
{
}

WatchT<Date>
StandalonePeerServer::
getTimer(Date expiry, double period,
         std::function<void (Date)> toBind)
{
    return impl->getTimer(expiry, period, toBind);
}

} // namespace Datacratic
