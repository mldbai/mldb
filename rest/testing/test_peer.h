// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** test_peer.h                                                    -*- C++ -*-
    Jeremy Barnes, 13 April 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Peer for testing purposes.
*/

#include "mldb/rest/service_peer.h"
#include "mldb/rest/etcd_peer_discovery.h"
#include "mldb/rest/asio_peer_server.h"
#include "mldb/utils/runner.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/utils/command.h"
#include "mldb/rest/rest_collection_impl.h"
#include <chrono>
#include <thread>
#include "mldb/io/message_loop.h"
#include <boost/algorithm/string.hpp>
#include "mldb/jml/utils/string_functions.h"

#include <sys/wait.h>

namespace MLDB {

using namespace std;
using namespace MLDB;


/*****************************************************************************/
/* TEST PEER                                                                 */
/*****************************************************************************/


struct TestPeer: public ServicePeer {
    TestPeer(const std::string & peerName,
             const std::string & etcdUri,
             const std::string & etcdPath)
        : ServicePeer(peerName, "TestPeer", "global"),
          valueCollection("value", "values", this),
          subscriptions(getPath())
    {
        addRoutes();
        if (etcdUri != "")
            initDiscovery(std::make_shared<EtcdPeerDiscovery>(this, etcdUri, etcdPath));
        else
            initDiscovery(std::make_shared<SinglePeerDiscovery>(this));
    }

    ~TestPeer()
    {
        shutdown();
    }

    void init(PortRange bindPort, const std::string & bindHost,
              int publishPort = -1, std::string publishHost = "")
    {
        auto server = std::make_shared<AsioPeerServer>();
        server->init(bindPort, bindHost, publishPort, publishHost);

        initServer(server);

        addEntity("values", valueCollection);

        pulseValue = 0;
    }

    void start()
    {
        shutdown_ = 0;

        ServicePeer::start();

        pulseValue = 0;

        auto runPulseThread = [&] ()
            {
                while (!shutdown_) {
                    ML::futex_wait(shutdown_, 0, 0.1);
                    if (shutdown_)
                        return;

                    pulse.trigger(pulseValue++);
                }
            };

        pulseThread.reset(new std::thread(runPulseThread));
    }

    void shutdown()
    {
        ServicePeer::shutdown();
        shutdown_ = true;
        ML::futex_wake(shutdown_);
        if (pulseThread) {
            pulseThread->join();
            pulseThread.reset();
        }
    }

    virtual void handlePeerMessage(RemotePeer * peer, PeerMessage && msg)
    {
        cerr << "handleMessageFromPeer with type " << msg.type
             << " and ID " << msg.messageId << endl;
        
        if (msg.type == 1) {
            // Message that never gets a response
            return;
        }
        else if (msg.type == 2) {
            // Message the sleeps for 0.1 seconds before responding
            cerr << "Got type 2 with id " << msg.messageId << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            cerr << "Done sleeping on type 2 for id " << msg.messageId << endl;

            msg.payload.resize(1);
            msg.payload[0] = jsonEncodeStr(true);
            peer->sendResponse(std::move(msg));
            
            cerr << "Responded on type 2 for id " << msg.messageId << endl;
            return;
        }
        else if (msg.type == 3) {
            // Throws an exception
            throw MLDB::Exception("exception handling message");
        }
        else if (msg.type == 4) {
            // Send back a typed object
            throw MLDB::Exception("sendPeerResponseObject");

            //sendPeerResponseObject(returnAddress,
            //                       header,
            //                       string("hello sunshine"));
            return;
        }
    }

    std::string bindTcp(const PortRange & portRange = PortRange(18000, 19000),
                        const std::string & host = "127.0.0.1")
    {
        return httpEndpoint->bindTcp(portRange, host);
    }

    std::pair<const std::type_info *,
              std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec)
    {
        if (spec.empty())
            throw MLDB::Exception("no type for empty spec");

        if (spec.size() == 1 && spec[0].channel == "pulse")
            return make_pair(&typeid(tuple<int>), nullptr);

        return ServicePeer::getWatchBoundType(spec);
    }
    
    Watch watchChannel(const Utf8String & channel,
                       const Utf8String & filter,
                       bool catchUp,
                       Any info)
    {
        if (channel == "pulse") {
            auto result = pulse.add(info);
            if (catchUp)
                result.trigger(pulseValue);
            return std::move(result);
        }
        return ServicePeer::watchChannel(channel, filter, catchUp, info);
    }
    
    virtual std::shared_ptr<EntityLinkToken>
    acceptLink(const std::vector<Utf8String> & sourcePath,
               const std::vector<Utf8String> & targetPath,
               const std::string & linkType,
               Any linkParams)
    {

        if (!targetPath.empty()) {
            return ServicePeer::acceptLink(sourcePath, targetPath, linkType,
                                           linkParams);
        }
        if (linkType != "subscription")
            throw MLDB::Exception("unknown link type '%s'", linkType.c_str());

        cerr << "sourcePath = " << jsonEncode(sourcePath)
             << " targetPath = " << jsonEncode(targetPath) << endl;

        return subscriptions.accept(sourcePath, linkParams);
    }

    WatchesT<int> pulse;

    int pulseValue;
    std::unique_ptr<std::thread> pulseThread;
    int shutdown_;

    RestCollection<string, string> valueCollection;

    EntityLinks subscriptions;
};


#if 0
struct ForkedTestPeer {

    ForkedTestPeer(const std::string & peerName,
                   const std::string & etcdUri)
    {
        cerr << "in ForkedTestPeer" << endl;

        int startPipe[2];
        int res = ::pipe(startPipe);
        if (res == -1)
            throw MLDB::Exception(errno, "pipe()");

        childPid = fork();

        cerr << "afterForkedTestPeer childPid = " << childPid << endl;

        if (childPid == -1)
            throw MLDB::Exception(errno, "fork()");

        //cerr << "pipe: read fd " << startPipe[0] << " write fd " << startPipe[1] << endl;

        if (childPid == 0) {
            ::close(startPipe[0]);
            cerr << "*** CHILD START" << endl;
            // Child.  We start a test peer and then hang around forever.
            TestPeer peer(peerName, std::make_shared<ServiceProxies>(), etcdUri);
            cerr << "*** PEER UP" << endl;
            peer.init(PortRange(15000, 16000), "127.0.0.1");
            peer.start();
            cerr << "*** PEER INITIALIZED" << endl;
            string boundAddress = peer.bindTcp();
            int res = ::write(startPipe[1], boundAddress.c_str(), boundAddress.length() + 1);
            cerr << "write res is " << res << endl;

            if (res == -1)
                throw MLDB::Exception(errno, "write of boundAddress");
            if (res != boundAddress.length() + 1)
                throw MLDB::Exception("didn't write whole address");
            
            cerr << "*** PEER READY" << endl;
            ::close(startPipe[1]);

            // Never exit
            while (true) {
                ML::sleep(10.0);
            }
        }

        ::close(startPipe[1]);
        char buf[1024];
        buf[0] = 0;
        
        res = ::read(startPipe[0], buf, 1024);
        ::close(startPipe[0]);
        cerr << "read res is " << res << endl;
        if (res == -1)
            throw MLDB::Exception(errno, "read() of bound address");
        cerr << "res = " << res << endl;

        cerr << "http address is " << buf << endl;

        httpAddr = buf;
    }

    ~ForkedTestPeer()
    {
        kill(SIGKILL);
        wait();
    }

    int childPid;
    std::string httpAddr;

    int kill(int sig)
    {
        return ::kill(childPid, sig);
    }

    int wait()
    {
        return 0;
        //return ::waidpid(childPid);
    }
};

#else
struct ForkedTestPeer: public Runner {
    
    MessageLoop loop;
    std::mutex runResultLock;
    RunResult runResult;
    std::mutex started;

    ForkedTestPeer(const std::string & peerName,
                   const std::string & etcdUri,
                   const std::string & etcdPath)
    {
        started.lock();

        auto onTerminate = [=] (const RunResult & result)
            {
                cerr << "Test peer has terminated" << endl;
                std::unique_lock<std::mutex> guard(runResultLock);
                runResult = result;
                started.unlock();
            };

        auto onData = [&] (std::string && data)
        {
            cerr << "test peer wrote output " << data << endl;
            std::unique_lock<std::mutex> guard(runResultLock);
            httpAddr = ML::trim(data);
            started.unlock();
        };

        auto onClose = [] (){};

        auto stdOutSink = make_shared<CallbackInputSink>(onData, onClose);
        loop.addSource("runner", *this);
        loop.start();

        string BIN = getenv("BIN") ? getenv("BIN") : "./build/x86_64/bin";

        vector<string> command = {
            BIN + "/test_peer_runner",
            peerName,
            etcdUri,
            etcdPath
        };

        cerr << "running command " << command << endl;

        run(command, onTerminate, stdOutSink);

        bool wasStarted = waitStart();
        if (!wasStarted)
            throw MLDB::Exception("didn't start");

        started.lock();
        started.unlock();

        if (httpAddr.empty())
            throw MLDB::Exception("peer didn't return its HTTP address");

    }

    ~ForkedTestPeer()
    {
        kill(SIGKILL, false /* must succeed */);
        waitTermination();
    }

    std::string httpAddr;
};
#endif

} // namespace MLDB
