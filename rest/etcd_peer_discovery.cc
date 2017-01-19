// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_discovery.cc
    Jeremy Barnes, 1 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "etcd_peer_discovery.h"
#include "mldb/arch/futex.h"
#include "mldb/jml/utils/info.h"
#include "mldb/logging/logging.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/jml/utils/string_functions.h"
#include <chrono>
#include <thread>


using namespace std;
using namespace ML;


namespace MLDB {

Logging::Category logEtcd("etcd", false /* enabled by default */);
//Logger::Category etcdErrors(etcd, "errors");
//Logger::Category etcdRequests(etcd, "errors");


/*****************************************************************************/
/* ETCD PEER DISCOVERY                                                       */
/*****************************************************************************/

EtcdPeerDiscovery::
EtcdPeerDiscovery(RestEntity * parent, const std::string & etcdUri,
                  const std::string & etcdPath)
    : PeerDiscovery(parent), etcd(etcdUri, etcdPath), shutdown_(false)
{
}

EtcdPeerDiscovery::
~EtcdPeerDiscovery()
{
    shutdown();
}

void
EtcdPeerDiscovery::
publish(PeerInfo info)
{
    //etcd.proxy.debug = true;

    ExcAssert(!discoveryThread);

    ourInfo = info;
    
    ExcAssertNotEqual(ourInfo.serviceType, "");
    ExcAssertNotEqual(ourInfo.peerName, "");

    LOG(logEtcd)
        << endl << endl << endl
        << "-------------------------------------------------------" << endl
        << "publishing " << jsonEncode(info) << endl;

    auto res = etcd.createDir("peers/" + ourInfo.serviceType);

    LOG(logEtcd) << "createDir res = " << jsonEncode(res) << endl;


    string key = "peers/" + ourInfo.serviceType + "/"
        + ourInfo.peerName + "/locked";

    LOG(logEtcd) << "setting key " << key << endl;

    res = etcd.setIfNotPresent("peers/" + ourInfo.serviceType + "/"
                               + ourInfo.peerName + "/locked",
                               jsonEncodeStr(ourInfo));
    
    LOG(logEtcd) << "set key res = " << jsonEncode(res) << endl;

    if (res.errorCode != 0) {
        throw MLDB::Exception("peer name appears to be already used");
    }

    LOG(logEtcd) << jsonEncode(etcd.get("peers/" + ourInfo.serviceType + "/"
                                + ourInfo.peerName + "/locked"));

    // Set up etcd
    res = etcd.refreshDir("peers/" + ourInfo.serviceType + "/" + ourInfo.peerName,
                          5.0 /* seconds */);
    
    if (res.errorCode != 0) {
        throw MLDB::Exception("error refreshing peer directory");
    }
    
    res = etcd.set("peers/" + ourInfo.serviceType + "/" + ourInfo.peerName + "/p2p",
                   jsonEncodeStr(ourInfo));
    if (res.errorCode != 0)
        throw MLDB::Exception("couldn't set our address");

    discoveryThread.reset
        (new std::thread([=] () { this->runDiscoveryThread(); }));
}

void
EtcdPeerDiscovery::
wakeup()
{
    ML::futex_wake(shutdown_);
}

void
EtcdPeerDiscovery::
shutdown()
{
    shutdown_ = true;
    wakeup();
    if (discoveryThread) {
        discoveryThread->join();
        discoveryThread.reset();
    }

    if (ourInfo.serviceType != "" && ourInfo.peerName != "") {
        string etcdDirectory = "peers/" + ourInfo.serviceType + "/" + ourInfo.peerName;
        
        auto resp = etcd.eraseDir(etcdDirectory, true /* recursive */);
        //cerr << "resp = " << jsonEncode(resp) << endl;
    }
}

void
EtcdPeerDiscovery::
runDiscoveryThread()
{
    // Every now and again, scan to see what new things have the behaviours
    // that we're after.
    //etcd.proxy.debug = true;

    int64_t lastKnownIndex = 0;
    Date lastRefreshed = Date::now();

    string serviceType = ourInfo.serviceType;
    string localPeerName = ourInfo.peerName;
    string fromPeersAddress = ourInfo.uri;

    while (!shutdown_) {

        //cerr << "discovery loop starting; lastKnownIndex = " << lastKnownIndex
        //     << endl;
        // Wait for either a watch event or for one second to elapse.  We don't actually
        // take the output of the watch; we just use it to know when to wake up.

        //cerr << "lastKnownIndex = " << lastKnownIndex
        //     << " res = " << jsonEncode(res) << endl;

        // 1.  Update our node on etcd, giving ourselves another 5 seconds
        //     We simply publish the tcp address and port of our zeromq
        //     connection.
        if (lastRefreshed.plusSeconds(0.5) < Date::now()) {

            double timeSinceRefresh = Date::now().secondsSince(lastRefreshed);
            if (timeSinceRefresh > 1.5)
                cerr << "warning: timeSinceRefresh = " << timeSinceRefresh
                     << endl;

            EtcdResponse res;
            try {
                res = etcd.refreshDir("peers/" + serviceType + "/" + localPeerName,
                                      5.0 /* seconds */);
            } catch (const std::exception & exc) {
                res.errorCode = 100;
            }

            if (res.errorCode != 0) {
                cerr << "timeSinceRefresh = " << timeSinceRefresh << endl;

                try {
                    res = etcd.createDir("peers/" + serviceType + "/" + localPeerName,
                                         5.0 /* seconds */);
            
                    Json::Value v;
                    v["pid"] = getpid();
                    v["host"] = hostname();
                    v["user"] = username();
                    v["uid"] = userid();
                    v["addr"] = MLDB::format("%016p", this);

                    string myId = v.toString();

                    auto res = etcd.setIfNotPresent("peers/" + serviceType
                                                    + "/" + localPeerName + "/locked",
                                                    myId);
                
                    //cerr << "res = " << jsonEncode(res) << endl;

                    if (res.errorCode != 0) {
                        cerr << "Peer name " << localPeerName
                             << " appears to be taken; sleeping 10 seconds" << endl;
                        std::this_thread::sleep_for(std::chrono::seconds(10));
                        continue;
                    }

                    etcd.set("peers/" + serviceType + "/" + localPeerName + "/p2p",
                             jsonEncodeStr(ourInfo));
                } catch (const std::exception & exc) {
                    cerr << "error refreshing etcd: " << exc.what() << endl;
                    errors.trigger("error refreshing on etcd", std::current_exception());
                    continue;
                }
            }
            lastRefreshed = Date::now();
        }
        
        // 1.  Get a list of everything that is running
        // Go to the configuration service

        EtcdResponse watchResult;
        if (lastKnownIndex != 0) {
            double secondsToWait
                = 1.0 - Date::now().secondsSince(lastRefreshed);
            if (secondsToWait > 0) { 
                try {
                    //cerr << "watching for " << secondsToWait << " seconds"
                    //     << endl;
                    //Date beforeWatch = Date::now();
                    watchResult
                        = etcd.watch("peers/" + serviceType, true,
                                     secondsToWait,
                                     lastKnownIndex + 1);
                    //Date afterWatch = Date::now();

                    //cerr << "watch for " << localPeerName << " returned in "
                    //     << afterWatch.secondsSince(beforeWatch) << "s: "
                    //     << jsonEncode(watchResult);
                
                    //cerr << "watch elapsed " << afterWatch.secondsSince(beforeWatch)
                    //<< " seconds; action is " << watchResult.action
                    //     << endl;
                } catch (const std::exception & exc) {
                    cerr << "error watching with etcd: " << exc.what() << endl;
                    errors.trigger("error watching etcd", std::current_exception());
                    // TODO: notify of error
                    continue;
                }
            }
        }

        if (shutdown_)
            continue;

        // Only do the rest of the watch found that something actually changed.
        if (lastKnownIndex != 0 && watchResult.action.empty())
            continue;

        // 3.  Look for new peers
        // (peer name, path in config service) pairs, one per peer
        vector<pair<string, PeerInfo> > endpoints;

        
        try {
            auto foundPeers = etcd.listDir("peers/" + serviceType, true);

            //cerr << "foundPeers returned "
            //     << jsonEncode(foundPeers)
            //     << endl;

            lastKnownIndex = foundPeers.index;

            for (auto & n: foundPeers.node->nodes) {
                vector<string> nameComponents = ML::split(n.key, '/');
                //cerr << "nameComponents = " << nameComponents << endl;
                if (nameComponents.size() < 4) {
                    cerr << "warning: invalid entry in peers etcd: "
                         << n.key << endl;
                    continue;
                }

                string peerName = nameComponents.back();

                //cerr << "peer " << peerName << endl;
                string p2pUri;
                for (auto & c: n.nodes) {
                    if (c.key.rfind("/p2p") == string::npos)
                        continue;
                    p2pUri = c.value;
                    break;
                }

                if (!p2pUri.empty())
                    endpoints.push_back(make_pair(peerName, jsonDecodeStr<PeerInfo>(p2pUri)));
            }

            //cerr << "foundPeers " << jsonEncode(foundPeers) << endl;
            //cerr << "endpoints are " << endpoints << endl;

            std::set<std::string> peersDone;

            // For each endpoint, find how to connect to its zeromq socket
            for (auto & e: endpoints) {

                string peerName = e.first;
                const PeerInfo & info = e.second;
                peersDone.insert(peerName);

                auto entry = knownPeers.tryGetExistingEntry(peerName);
                if (entry) {
                    if (jsonEncode(*entry) == jsonEncode(info))
                        continue;   // still the same as before
                    else {
                        // Info has changed.  We remove the old one
                        knownPeers.replaceEntry(peerName, std::make_shared<PeerInfo>(info));
                    }
                }
                else {
                    // New entry
                    knownPeers.addEntry(peerName, std::make_shared<PeerInfo>(info));
                }
            }

            std::vector<std::string> peersToDelete;

            auto onPeer = [&] (std::string peerName, const PeerInfo & peer)
                {
                    //cerr << "checking peer " << peerName << endl;
                    if (peersDone.count(peerName))
                        return true;

                    // Disconnect from the peer
                    peersToDelete.push_back(peerName);

                    return true;
                };

            knownPeers.forEachEntry(onPeer);

            for (auto & p: peersToDelete) {
                knownPeers.deleteEntry(p);
            }
        } catch (const std::exception & exc) {
            cerr << "error scanning peers: " << exc.what() << endl;
            errors.trigger("error scanning peers", std::current_exception());
            continue;
        }
    }
}

} // namespace MLDB
