// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_discovery.h                                               -*- C++ -*-
    Jeremy Barnes, 1 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Service for peer discovery.
*/

#pragma once

#include "peer_info.h"
#include "rest_collection.h"

namespace MLDB {


/*****************************************************************************/
/* PEER DISCOVERY                                                            */
/*****************************************************************************/

struct PeerDiscovery {

    PeerDiscovery(RestEntity * owner);

    virtual void publish(PeerInfo info) = 0;

    virtual void shutdown() = 0;

    /** Wakeup the discovery service as something may have changed. */
    virtual void wakeup() = 0;

    /** Errors will be posted to this watch.  First argument is an error
        message, second is the exception that caused it (if available). */
    WatchesT<std::string, std::exception_ptr> errors;
    
    /** Collection of known peers. */
    RestCollection<std::string, PeerInfo> knownPeers;
};

extern template class RestCollection<std::string, PeerInfo>;


/*****************************************************************************/
/* SINGLE PEER DISCOVERY                                                     */
/*****************************************************************************/

/** Peer discovery service that needs no backend because it only discovers
    itself.
*/

struct SinglePeerDiscovery: public PeerDiscovery {

    SinglePeerDiscovery(RestEntity * owner);

    virtual void publish(PeerInfo info);

    virtual void shutdown();

    /** Wakeup the discovery service as something may have changed. */
    virtual void wakeup();
};

} // namespace MLDB
