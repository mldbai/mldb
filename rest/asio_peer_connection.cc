// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_peer_connection.cc                                         -*- C++ -*-
   Jeremy Barnes, 2 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "asio_peer_connection.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/arch/backtrace.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/asio_timer.h"
#include "mldb/watch/watch_impl.h"
#include <atomic>
#include <boost/asio.hpp>

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* ASIO PEER CONNECTION                                                      */
/*****************************************************************************/

AsioPeerConnection::Itl::
Itl(std::shared_ptr<boost::asio::ip::tcp::socket> sock,
    AsioPeerConnection * connection)
    : strand(sock->get_io_service()),
      connection(connection)
{
    this->sock = sock;

    // Minimise latency by turning off Nagle's algorithm.  Only really makes
    // sense for real-time calls.
    this->sock->set_option(boost::asio::ip::tcp::no_delay(true));

    currentlyReading = false;
    currentlyWriting = false;
    currentState = (sock->is_open() ? ST_CONNECTED : ST_CLOSED);
    currentError = "";
    shutdown_ = false;
}

AsioPeerConnection::
AsioPeerConnection(std::shared_ptr<boost::asio::ip::tcp::socket> sock)
    : itl(new Itl(sock, this))
{
}

AsioPeerConnection::
~AsioPeerConnection()
{
    shutdown();
}

PeerConnectionStatus
AsioPeerConnection::
getStatus() const
{
    return itl->getStatus();
}

PeerConnectionStatus
AsioPeerConnection::Itl::
getStatus() const
{
    std::unique_lock<std::recursive_mutex> guard(mutex);

    PeerConnectionStatus result;
    result.state = currentState;
    result.fd = sock->native_handle();
    result.style = "socket(asio)";

    boost::system::error_code err;
    auto local_ep = sock->local_endpoint(err);
    if (!err) {
        result.local.addr = local_ep.address().to_string();
        result.local.port = local_ep.port();
    } else {
        result.local.addr = err.message();
        result.local.port = -1;
    }

    auto remote_ep = sock->remote_endpoint(err);
    if (!err) {
        result.remote.addr = remote_ep.address().to_string();
        result.remote.port = remote_ep.port();
    } else {
        result.remote.addr = err.message();
        result.remote.port = -1;
    }

    {
        tcp_info tcpinfo;
        socklen_t len = sizeof(tcpinfo);
        int res = getsockopt(result.fd, IPPROTO_TCP, TCP_INFO, &tcpinfo, &len);
        if (res != -1) {
            result.rttMs = tcpinfo.tcpi_rtt / 1000.0;
            result.rttVarianceMs = tcpinfo.tcpi_rttvar / 1000.0;
        }
    }

    // endpoints
    return result;
}

void
AsioPeerConnection::
shutdown()
{
    std::unique_lock<std::recursive_mutex> guard(mutex);

    itl->shutdown_ = true;
    // First, cancel outstanding operations
    boost::system::error_code error;
    itl->sock->cancel(error);
    
    // Swallow the error; we're trying to shut down

    itl->currentState = ST_CLOSED;
    itl->sock->close();
    // ...

    // Now wait for everything to catch up
}

void
AsioPeerConnection::
send(std::string && data)
{
    std::unique_lock<std::recursive_mutex> guard(mutex);

    ExcAssert(!itl->currentlyWriting);
    ExcAssert(itl->sock);
    //boost::system::error_code err;

    uint64_t length = data.length();

    itl->sock->send(boost::asio::buffer(&length, sizeof(length)));
    itl->sock->send(boost::asio::buffer(data.c_str(), data.size()));
}

void
AsioPeerConnection::
startWriting(std::function<bool (std::string & data)> onSend)
{
    std::unique_lock<std::recursive_mutex> guard(mutex);

    ExcAssert(onSend);
    this->itl->onSend = onSend;

    if (!itl->currentlyWriting) {
        doStartWriting(itl);
    }
}

void
AsioPeerConnection::
doStartWriting(std::shared_ptr<Itl> itl)
{
    ExcAssert(itl->onSend);

    std::string newPacket;
    bool hasPacket = itl->onSend(newPacket);

    if (!hasPacket) {
        itl->currentlyWriting = false;
        return;
    }

    struct Packet {
        uint64_t length;
        string payload;
    };

    auto packet = std::make_shared<Packet>();
    packet->length = newPacket.length();
    packet->payload = std::move(newPacket);

    // Create a lambda to capture the packet so that it stays put until the
    // write is finished
    auto onWriteDoneCb = [packet,itl] (boost::system::error_code err,
                                       size_t bytesDone)
        {
            if (err) {
                cerr << "onWriteDoneCb: erro = " << err.message() << endl;
            }
            else {
                ExcAssertEqual(bytesDone, packet->length + 8);
            }
            //cerr << "write done callback: " << bytesDone << " bytes done"
            //     << endl;
            onWriteDone(err, bytesDone, itl);
            (void)packet.get();
        };

    std::vector<boost::asio::const_buffer> buffers = {
        boost::asio::buffer((const void *)(&packet->length), 8),
        boost::asio::buffer(packet->payload)
    };

    //cerr << "writing " << boost::asio::buffer_size(packet->buffers)
    //     << " bytes for packet of length " << packet->length << endl;

    boost::asio::
        async_write(*itl->sock, buffers, itl->strand.wrap(onWriteDoneCb));

    //cerr << "done async write" << endl;

    itl->currentlyWriting = true;
}

void
AsioPeerConnection::
onWriteDone(boost::system::error_code err, size_t bytesDone,
            std::shared_ptr<Itl> itl)
{
#if 0
    if (err) {
        cerr << "Peer write had error " << err.message() << endl;
        return;
    }
#endif

    //cerr << "wrote " << bytesDone << " bytes" << endl;

    if (itl->shutdown_)
        return;

    if (err) {
        cerr << "Peer write had error " << err.message() << endl;
        notifyError(err, itl);
        return;
    }

    std::unique_lock<std::recursive_mutex> guard(mutex);
    if (itl->shutdown_)
        return;
    doStartWriting(itl);
}

void
AsioPeerConnection::
stopWriting()
{
    std::unique_lock<std::recursive_mutex> guard(mutex);

    itl->onSend = nullptr;
    itl->currentlyWriting = false;
}

void
AsioPeerConnection::
startReading(std::function<bool (std::string && data)> onRecv)
{
    std::unique_lock<std::recursive_mutex> guard(mutex);

    ExcAssert(onRecv);
    bool hadOnRecv = !!this->onRecv;
    this->onRecv = onRecv;

    if (itl->bufferedRead) {
        if (!this->onRecv(std::move(*itl->bufferedRead)))
            this->onRecv = nullptr;
        itl->bufferedRead.reset();
    }

    if (!hadOnRecv && this->onRecv) {
        doStartReading(itl);
    }
}

void
AsioPeerConnection::
stopReading()
{
    std::unique_lock<std::recursive_mutex> guard(mutex);
    onRecv = nullptr;
    itl->currentlyReading = false;
}

void
AsioPeerConnection::
doStartReading(std::shared_ptr<Itl> itl)
{
    boost::asio::async_read(*itl->sock, boost::asio::buffer(&itl->currentPacketLength, 8),
                            itl->strand.wrap(std::bind(&AsioPeerConnection::onReadLengthDone,
                                                       std::placeholders::_1,
                                                       std::placeholders::_2,
                                                       itl)));
    itl->currentlyReading = true;
}

void
AsioPeerConnection::
onReadLengthDone(boost::system::error_code err, size_t bytesDone,
                 std::shared_ptr<Itl> itl)
{
    if (err == boost::system::errc::operation_canceled
        || err == boost::asio::error::operation_aborted) {
        return;
    }

    if (itl->shutdown_)
        return;

    if (err) {
        cerr << "Peer read length had error " << err.message() << endl;
        notifyError(err, itl);
        return;
    }

    if (bytesDone != 8) {
        // This is a logic error; it should never happen.  It's OK to
        // crash the world.
        throw MLDB::Exception("wrong number of length bytes read in packet");
    }

    //cerr << "itl->currentPacketLength = " << itl->currentPacketLength << endl;

    // Create the space to read the packet into
    itl->currentPacket.clear();
    itl->currentPacket.resize(itl->currentPacketLength, 0);

    boost::asio::async_read(*itl->sock, boost::asio::buffer(&itl->currentPacket[0], itl->currentPacketLength),
                            itl->strand.wrap(std::bind(&AsioPeerConnection::onReadDataDone,
                                                  std::placeholders::_1,
                                                       std::placeholders::_2,
                                                       itl)));

}

void
AsioPeerConnection::
onReadDataDone(boost::system::error_code err, size_t bytesDone,
               std::shared_ptr<Itl> itl)
{
    if (err == boost::system::errc::operation_canceled
        || err == boost::asio::error::operation_aborted) {
        return;
    }

    if (err) {
        cerr << "Peer read had error " << err.message() << endl;
    }
    if (itl->shutdown_)
        return;

    std::unique_lock<std::recursive_mutex> guard(mutex);

    if (itl->shutdown_)
        return;

    //cerr << "onReadDataDone" << endl;
    //cerr << "got packet " << currentPacket << endl;

    if (bytesDone == itl->currentPacketLength) {
        if (itl->connection->onRecv) {
            if (!itl->connection->onRecv(std::move(itl->currentPacket))) {
                itl->connection->onRecv = nullptr;
                itl->currentlyReading = false;
            }
        }
        else {
            ExcAssert(!itl->bufferedRead);
            itl->bufferedRead.reset(new std::string(std::move(itl->currentPacket)));
        }
    }
    else {
        ExcAssert(err);
    }
    if (err) {
        cerr << "Peer read had error " << err.message() << endl;
        notifyError(err, itl);
        return;
    }

    if (itl->currentlyReading)
        doStartReading(itl);
}

void
AsioPeerConnection::
setState(PeerConnectionState newState)
{
    if (itl->currentState == newState)
        return;

    cerr << "peer connection state is at " << newState << endl;

    itl->currentState = newState;
    stateWatches.trigger(newState);
}

void
AsioPeerConnection::
notifyError(boost::system::error_code err, std::shared_ptr<Itl> itl)
{
    cerr << "Peer connection had error " << err.message() << endl;

    if (err == boost::asio::error::eof
        || err == boost::asio::error::connection_reset
        || err == boost::asio::error::broken_pipe) {
        itl->connection->setState(ST_CLOSED);
        return;
    }
    //state = ST_CLOSED;
    abort();
}


WatchT<Date>
AsioPeerConnection::
getTimer(Date expiry, double period,
         std::function<void (Date)> toBind)
{
    return MLDB::getTimer(expiry, period, StrandHolder(itl->strand), toBind);
}

void
AsioPeerConnection::
postWorkSync(std::function<void ()> work)
{
    itl->strand.post(work);
}

void
AsioPeerConnection::
postWorkAsync(std::function<void ()> work)
{
    itl->strand.get_io_service().post(work);
}


} // namespace MLDB

