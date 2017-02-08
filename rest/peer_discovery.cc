// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_discovery.cc
    Jeremy Barnes, 1 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "peer_discovery.h"
#include "rest_collection_impl.h"

namespace MLDB {


/*****************************************************************************/
/* PEER DISCOVERY                                                            */
/*****************************************************************************/

PeerDiscovery::
PeerDiscovery(RestEntity * owner)
    : knownPeers("peers", "peer", owner)
{
}

template class RestCollection<std::string, PeerInfo>;


/*****************************************************************************/
/* SINGLE PEER DISCOVERY                                                     */
/*****************************************************************************/

SinglePeerDiscovery::
SinglePeerDiscovery(RestEntity * owner)
    : PeerDiscovery(owner)
{
}

void
SinglePeerDiscovery::
publish(PeerInfo info)
{
    ExcAssert(knownPeers.addEntry(info.peerName,
                                  std::make_shared<PeerInfo>(info)));
}

void
SinglePeerDiscovery::
shutdown()
{
    knownPeers.clear();
}

void
SinglePeerDiscovery::
wakeup()
{
    // no op
}

} // namespace MLDB
