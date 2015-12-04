// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** peer_info.h                                                    -*- C++ -*-
    Jeremy Barnes, 1 June 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Information about a peer.
*/

#pragma once

#include "mldb/types/value_description.h"


namespace Datacratic {


/*****************************************************************************/
/* PEER INFO                                                                 */
/*****************************************************************************/

/** Basic information sent about a peer for discovery. */

struct PeerInfo {
    PeerInfo();

    std::string peerName;
    std::string uri;
    std::string location;
    std::string serviceType;

    std::string epoch;
};

DECLARE_STRUCTURE_DESCRIPTION(PeerInfo);


} // namespace Datacratic
