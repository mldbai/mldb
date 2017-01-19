// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_test.cc
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "temporary_etcd_server.h"
#include "mldb/rest/etcd_peer_discovery.h"


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;

using std::to_string;

BOOST_AUTO_TEST_CASE( test_two_members_discovery )
{
    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    EtcdPeerDiscovery discovery1(nullptr, etcdUri, etcdPath);

    PeerInfo info1;
    info1.peerName = "peer1";
    info1.serviceType = "service1";

    discovery1.publish(info1);


    EtcdPeerDiscovery discovery2(nullptr, etcdUri, etcdPath);

    PeerInfo info2;
    info2.peerName = "peer2";
    info2.serviceType = "service1";

    discovery2.publish(info2);



}
