// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* peer_connection.h                                               -*- C++ -*-
   Jeremy Barnes, 31 May 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Connection between two peers.
*/

#pragma once

#include "mldb/watch/watch.h"
#include "peer_info.h"
#include "peer_message.h"


namespace MLDB {

/** State of a connection to a peer. */
enum PeerConnectionState {
    ST_UNKNOWN,
    ST_CONNECTED,
    ST_ACCEPTED,
    ST_CLOSED
};

DECLARE_ENUM_DESCRIPTION(PeerConnectionState);

/** Describes an endpoint of a connection. */
struct PeerConnectionEndpoint {
    std::string addr;
    int port;
};

DECLARE_STRUCTURE_DESCRIPTION(PeerConnectionEndpoint);

/** Describes a connection between peers. */
struct PeerConnectionStatus {
    PeerConnectionStatus()
        : fd(-1), rttMs(-0.0), rttVarianceMs(-0.0),
          state(ST_UNKNOWN)
    {
    }

    PeerConnectionEndpoint local;
    PeerConnectionEndpoint remote;
    int fd;
    double rttMs;
    double rttVarianceMs;
    std::string bound;
    PeerConnectionState state;
    std::string style;
};

DECLARE_STRUCTURE_DESCRIPTION(PeerConnectionStatus);


/*****************************************************************************/
/* PEER CONNECTION                                                           */
/*****************************************************************************/

/** Abstraction of a connection between two peers.  Its job is to establish
    and monitor a connection between two peers, to transmit messages to
    the remote peer, and to receive message from the remote peer.
*/

struct PeerConnection {

    virtual void startReading(std::function<bool (std::string && data)> onRecv);
    virtual void stopReading();

    virtual PeerConnectionStatus getStatus() const = 0;

    // Synchronous send
    virtual void send(std::string && data) = 0;

    // Asynchronous send.  onSend should return false if there is nothing left
    // to send, in which case writing will stop until startWriting is called
    // again.  Calling startWriting with writing already happening is a nop.
    virtual void startWriting(std::function<bool (std::string & data)> onSend) = 0;
    virtual void stopWriting() = 0;

    virtual void shutdown() = 0;

    virtual void postWorkSync(std::function<void ()> work) = 0;
    virtual void postWorkAsync(std::function<void ()> work) = 0;

    /** Return a timer that triggers at the given expiry and optionally
        resets to fire periodically.
    */
    virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                  std::function<void (Date)> toBind = nullptr) = 0;

    /** Function called when we get a data packet.  It returns true to continue
        reading, or false to stop receiving any more packets.  In that case it
        will need to call startReading again to continue reading more data.
    */
    std::function<bool (std::string && data)> onRecv;

    WatchesT<std::string> dataWatches;
    WatchesT<PeerConnectionState> stateWatches;
    //WatchT<bool> startSendingWatches;
};


/*****************************************************************************/
/* LOCAL PEER CONNECTION                                                     */
/*****************************************************************************/

/** Connection to a local peer (which is known) via direct in-memory passing
    of objects.
*/

struct LocalPeerConnection: public PeerConnection {
    std::shared_ptr<PeerConnection> outgoing;
    std::shared_ptr<PeerConnection> incoming;
};


/*****************************************************************************/
/* MIRROR PEER CONNECTION                                                    */
/*****************************************************************************/

/** Peer connection for in-process use where messages sent are immediately
    received on the other end.
*/

struct MirrorPeerConnection : public PeerConnection {

    MirrorPeerConnection(boost::asio::io_service & ioService);

    ~MirrorPeerConnection();

    virtual PeerConnectionStatus getStatus() const;

    virtual void shutdown();

    std::function<void (std::string && data)> onRecv;

    virtual void startReading(std::function<bool (std::string && data)> onRecv);
    virtual void stopReading();

    virtual void send(std::string && data);

    virtual void startWriting(std::function<bool (std::string & data)> onSend);
    virtual void stopWriting();

    virtual void postWorkSync(std::function<void ()> work);
    virtual void postWorkAsync(std::function<void ()> work);

    /** Return a timer that triggers at the given expiry and optionally
        resets to fire periodically.

        If a function is passed in then bind that function.
    */
    virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                  std::function<void (Date)> toBind = nullptr);

    struct Impl;
    std::unique_ptr<Impl> impl;
};


/*****************************************************************************/
/* PEER SERVER                                                               */
/*****************************************************************************/

/** Server that creates connections to remote peers. */

struct PeerServer {

    /** Listem, updating our peer info for this server. */
    virtual PeerInfo listen(PeerInfo info) = 0;

    /** Shutdown the peer server. */
    virtual void shutdown() = 0;

    /** Return the loopback connection with the local peer. */
    virtual std::shared_ptr<PeerConnection>
    connectToSelf() = 0;

    virtual std::shared_ptr<PeerConnection>
    connect(const PeerInfo & peer) = 0;

    /** Can we connect externally, or is it only able to connect to itself
        (false)?
    */
    virtual bool supportsExternalConnections() const = 0;

    /** Return a timer that triggers at the given expiry and optionally
        resets to fire periodically.
    */
    virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                  std::function<void (Date)> toBind = nullptr) = 0;

    /** Post some work to be done later in a different thread. */
    virtual void postWork(std::function<void ()> work) = 0;

    /** Called when we got a new connection. */
    virtual void setNewConnectionHandler(std::function<void (std::shared_ptr<PeerConnection>)> onNewConnection) = 0;
};

} // namespace MLDB
