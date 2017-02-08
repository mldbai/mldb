// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#include "test_peer.h"

using namespace MLDB;
using namespace std;

int main(int argc, char ** argv)
{
    // arg 1 = peer name
    // arg 2 = etcd uri
    // arg 3 = etcd path

    if (argc != 4)
        throw MLDB::Exception("wrong arguments: need peer name and etcd uri and etcd path; got %d",
                            argc);

    TestPeer peer(argv[1], argv[2], argv[3]);
    peer.init(PortRange(15000, 16000), "127.0.0.1");
    peer.start();
    string boundAddress = peer.bindTcp(PortRange(13000, 14000));
    cout << boundAddress << endl;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}
