// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_connection.cc
    Jeremy Barnes, 1 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "peer_connection.h"
#include "asio_peer_connection.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/asio_timer.h"
#include "mldb/watch/watch_impl.h"
#include "asio_peer_server.h"
#include <boost/asio.hpp>


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* PEER CONNECTION                                                           */
/*****************************************************************************/

void
PeerConnection::
startReading(std::function<bool (std::string && data)> onRecv)
{
    this->onRecv = onRecv;
}

void
PeerConnection::
stopReading()
{
    this->onRecv = nullptr;
}


/*****************************************************************************/
/* MIRROR PEER CONNECTION                                                    */
/*****************************************************************************/

struct MirrorPeerConnection::Impl {
    Impl(boost::asio::io_service & ioService)
        : strand(ioService)
    {
    }

    ~Impl()
    {
    }
    
    boost::asio::strand strand;
};

MirrorPeerConnection::
MirrorPeerConnection(boost::asio::io_service & ioService)
{
    impl.reset(new Impl(ioService));
}

MirrorPeerConnection::
~MirrorPeerConnection()
{
    shutdown();
}

PeerConnectionStatus
MirrorPeerConnection::
getStatus() const
{
    PeerConnectionStatus result;
    result.local.addr = result.remote.addr = "localhost";
    result.local.port = result.remote.port = -1;
    result.fd = -1;
    result.state = ST_CONNECTED;
    result.style = "mirror";
    result.rttMs = 0;
    result.rttVarianceMs = 0;
    return result;
}

void
MirrorPeerConnection::
startReading(std::function<bool (std::string && data)> onRecv)
{
    this->onRecv = onRecv;
}

void
MirrorPeerConnection::
stopReading()
{
    throw MLDB::Exception("MirrorPeerConnection::stopReading() not implemented");
}

void
MirrorPeerConnection::
shutdown()
{
    impl.reset();
    stateWatches.trigger(ST_CLOSED);
}

void
MirrorPeerConnection::
send(std::string && data)
{
    if (!dataWatches.empty())
        dataWatches.trigger(data);
    if (onRecv)
        onRecv(std::move(data));
}

void
MirrorPeerConnection::
startWriting(std::function<bool (std::string & data)> onSend)
{
    ExcAssert(onRecv);

    std::string data;
    while (onSend(data)) {
        onRecv(std::move(data));
    }
}

void
MirrorPeerConnection::
stopWriting()
{
}

WatchT<Date>
MirrorPeerConnection::
getTimer(Date expiry, double period, std::function<void (Date)> toBind)
{
    return MLDB::getTimer(expiry, period, StrandHolder(impl->strand),
                                toBind);
}

void
MirrorPeerConnection::
postWorkSync(std::function<void ()> work)
{
    impl->strand.post(work);
}

void
MirrorPeerConnection::
postWorkAsync(std::function<void ()> work)
{
    impl->strand.get_io_service().post(work);
}


/*****************************************************************************/
/* PEER SERVICE                                                              */
/*****************************************************************************/


/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION(PeerConnectionState);

PeerConnectionStateDescription::
PeerConnectionStateDescription()
{
    addValue("UNKNOWN", ST_UNKNOWN);
    addValue("CONNECTED", ST_CONNECTED);
    addValue("ACCEPTED", ST_ACCEPTED);
    addValue("CLOSED", ST_CLOSED);
}

DEFINE_STRUCTURE_DESCRIPTION(PeerConnectionEndpoint);

PeerConnectionEndpointDescription::
PeerConnectionEndpointDescription()
{
    addField("addr", &PeerConnectionEndpoint::addr,
             "remote address");
    addField("port", &PeerConnectionEndpoint::port,
             "remote port");
}

DEFINE_STRUCTURE_DESCRIPTION(PeerConnectionStatus);

PeerConnectionStatusDescription::
PeerConnectionStatusDescription()
{
    addField("local", &PeerConnectionStatus::local,
             "Local address for connection");
    addField("remote", &PeerConnectionStatus::remote,
             "Remote address for connection");
    addField("fd", &PeerConnectionStatus::fd,
             "File descriptor for connection", -1);
    addField("rttMs", &PeerConnectionStatus::rttMs,
             "Round trip time in milliseconds", -0.0);
    addField("rttVarianceMs", &PeerConnectionStatus::rttVarianceMs,
             "Round trip time variance in milliseconds", -0.0);
    addField("bound", &PeerConnectionStatus::bound,
             "Bound address");
    addField("state", &PeerConnectionStatus::state,
             "State of peer connection");
    addField("style", &PeerConnectionStatus::style,
             "Style of peer connection");
}

} // namespace MLDB
