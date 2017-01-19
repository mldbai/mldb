// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_info.cc
    Jeremy Barnes, 2 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "peer_info.h"
#include "mldb/types/structure_description.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/info.h"
#include "mldb/types/date.h"
#include <unistd.h>

namespace MLDB {

const Date startupDate = Date::now();

PeerInfo::
PeerInfo()
{
    epoch = format("%s-%d-%lld-%d",
                   hostname().c_str(),
                   getpid(),
                   (long long)Date::now().secondsSinceEpoch() * 1000000,
                   random());
}

DEFINE_STRUCTURE_DESCRIPTION(PeerInfo);

PeerInfoDescription::
PeerInfoDescription()
{
    addField("peerName", &PeerInfo::peerName, "Name of peer");
    addField("uri", &PeerInfo::uri, "URI to connect to peer");
    addField("location", &PeerInfo::location, "Network location of peer");
    addField("serviceType", &PeerInfo::serviceType, "Service type of peer");
    addField("epoch", &PeerInfo::epoch, "Epoch of peer");
}

} // namespace MLDB
