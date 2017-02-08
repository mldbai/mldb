// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* standalone_peer_server.h                                          -*- C++ -*-
   Jeremy Barnes, 1 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "peer_connection.h"

namespace MLDB {


/*****************************************************************************/
/* STANDALONE PEER SERVER                                                    */
/*****************************************************************************/

struct StandalonePeerServer: public PeerServer {

    StandalonePeerServer();
    ~StandalonePeerServer();

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
        return false;
    }

    /** Return a timer that triggers at the given expiry and optionally
        resets to fire periodically.
    */
    virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                  std::function<void (Date)> toBind = nullptr);

    /** Post some work to be done later in a different thread. */
    virtual void postWork(std::function<void ()> work);

    /** Called when we got a new connection. */
    virtual void setNewConnectionHandler(std::function<void (std::shared_ptr<PeerConnection>)> onNewConnection);

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace MLDB

