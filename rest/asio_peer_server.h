// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_peer_server.h                                          -*- C++ -*-
   Jeremy Barnes, 1 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "peer_connection.h"
#include "mldb/io/port_range_service.h"

namespace MLDB {


/*****************************************************************************/
/* ASIO PEER SERVER                                                          */
/*****************************************************************************/

struct AsioPeerServer: public PeerServer {

    AsioPeerServer();
    ~AsioPeerServer();

    void init(PortRange bindPort, const std::string & bindHost,
              int publishPort = -1, std::string publishHost = "");

    /** Listen, updating our peer info for this server. */
    virtual PeerInfo listen(PeerInfo info);

    /** Shutdown the peer server. */
    virtual void shutdown();

    virtual std::shared_ptr<PeerConnection>
    connect(const PeerInfo & peer);

    /** Return the loopback connection with the local peer. */
    virtual std::shared_ptr<PeerConnection>
    connectToSelf();

    /** Can we connect externally, or is it only able to connect to itself
        (false)?
    */
    virtual bool supportsExternalConnections() const
    {
        return true;
    }

    /** Return a timer that triggers at the given expiry and optionally
        resets to fire periodically.
    */
    virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                  std::function<void (Date)> toBind = nullptr);

    /** Return the FD we're accepting on.  Mostly useful for unit tests. */
    int acceptFd() const;
    
    /** Post some work to be done later in a different thread. */
    virtual void postWork(std::function<void ()> work);

    /** Called when we got a new connection. */
    virtual void setNewConnectionHandler(std::function<void (std::shared_ptr<PeerConnection>)> onNewConnection);

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace MLDB

