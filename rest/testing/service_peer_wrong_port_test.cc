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


// Checks that the ServicePeer won't start up with the wrong port published
BOOST_AUTO_TEST_CASE( test_publish_wrong_port )
{
    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();

    for (unsigned i = 0;  i < 100;  ++i) {
        clearEtcd(etcdUri, etcdPath);

        auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);

        // We're publishing port 13000, which we're not listening ont... should cause an
        // exception when starting
        peer1->init(PortRange(15000, 16000), "127.0.0.1", 13000);

        {
            MLDB_TRACE_EXCEPTIONS(false);
            BOOST_CHECK_THROW(peer1->start(), std::exception);
        }

        peer1->shutdown();
        peer1.reset();
    }
}

