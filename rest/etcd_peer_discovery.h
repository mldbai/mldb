// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_discovery_etcd.h                                          -*- C++ -*-
    Jeremy Barnes, 1 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include "peer_discovery.h"
#include "etcd_client.h"


namespace MLDB {


/*****************************************************************************/
/* ETCD PEER DISCOVERY                                                       */
/*****************************************************************************/

/** Peer discovery that works using etcd. */

struct EtcdPeerDiscovery: public PeerDiscovery {

    EtcdPeerDiscovery(RestEntity * owner,
                      const std::string & etcdUri,
                      const std::string & etcdPath);
    
    virtual ~EtcdPeerDiscovery();

    /** Start the service, publishing the given PeerInfo for our local
        info.
    */
    virtual void publish(PeerInfo info);
  
    /** Wakeup the discovery loop so it can re-scan the available
        services.
    */
    virtual void wakeup();

    virtual void shutdown();


private:
    
    /** Information for our peer that we want to publish. */
    PeerInfo ourInfo;

    /** Thread we run to discover peer servers. */
    void runDiscoveryThread();
    std::unique_ptr<std::thread> discoveryThread;

    /** Client to the etcd service used for peer discovery. */
    EtcdClient etcd;

    int shutdown_;
};

} // namespace MLDB
