// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_test.cc
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "temporary_etcd_server.h"
#include "mldb/rest/service_peer.h"
#include "mldb/rest/etcd_client.h"
#include "mldb/utils/runner.h"
#include "mldb/rest/rest_service_endpoint.h"
#include "mldb/rest/rest_request_router.h"
#include <boost/algorithm/string.hpp>
#include <sys/wait.h>
#include "test_peer.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;


// Checks that a second ServicePeer with the same name as another can't start up
BOOST_AUTO_TEST_CASE( test_name_clash_leads_to_error )
{
    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    cerr << "----------- peer1 is started" << endl;

    // This one has the same name as the last one, and should cause an error
    auto peer2 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer2->init(PortRange(15000, 16000), "127.0.0.1");
    cerr << "----------- peers is now starting" << endl;
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(peer2->start(), std::exception);
    }

    peer1->shutdown();
    peer2->shutdown();
    peer1.reset();
    peer2.reset();
}

