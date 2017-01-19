// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer.cc
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/types/value_description.h"
#include "service_peer.h"
#include "mldb/arch/futex.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/jml/utils/info.h"
#include "mldb/jml/utils/hex_dump.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/utils/log.h"
#include "rest_collection_impl.h"
#include <future>


using namespace std;
using namespace ML;

namespace MLDB {


/*****************************************************************************/
/* SERVICE PEER                                                              */
/*****************************************************************************/

ServicePeer::
ServicePeer(const std::string & localPeerName,
            const std::string & serviceType,
            const std::string & serviceLocation,
            bool enableLogging)
    : RestDirectory(this, "ROOT"),
      HttpRestService(enableLogging),
      peers(this),
      logger(MLDB::getMldbLog<ServicePeer>())
{
    peerInfo.peerName = localPeerName;
    peerInfo.serviceType = serviceType;
    peerInfo.location = serviceLocation;

    // We know, at a minimum, about our peers
    addEntity("peers", peers);

    //addRoutes();

    HttpRestService::init();
}

void
ServicePeer::
addRoutes()
{
    onHandleRequest = router.requestHandler();

    router.description = "Service Peer REST API";

    router.addHelpRoute("/", "GET");
    
    RestRequestRouter::OnProcessRequest pingRoute
        = [] (RestConnection & connection,
              const RestRequest & request,
              const RestRequestParsingContext & context) {
        connection.sendResponse(200, "1");
        return RestRequestRouter::MR_YES;
    };
    
    router.addRoute("/ping", "GET", "Ping the availability of the endpoint",
                    pingRoute,
                    Json::Value());
    
    auto getPeerCollection = [=] (const RestRequestParsingContext & context)
        {
            return &this->peers;
        };

    // Add our peer routes
    auto peerRouteManager
        = std::make_shared<PeerCollection::RouteManager>(router, 1, getPeerCollection,
                                                         "peer", "peers");
    peers.initRoutes(*peerRouteManager);


#if 0
    addRouteSyncJsonReturn(router, "/peerStatus", { "GET" },
                           "Get information about all peers",
                           "Array of information about each peer",
                           &ServicePeer::getPeerStatuses,
                           this);

    addRouteSyncJsonReturn(router, "/peerConnections", { "GET" },
                           "Get information about all peer connections",
                           "Array of information about each peer connectino",
                           &ServicePeer::getPeerConnections,
                           this);
#endif
    
    router.addRoute("/v1/watches", "POST",
                    "Create a watch (long-running connection)",
                    std::bind(&ServicePeer::createHttpWatch, this,
                              std::placeholders::_1,
                              std::placeholders::_2,
                              std::placeholders::_3),
                    Json::Value() /* help */);
}

RestRequestMatchResult
ServicePeer::
createHttpWatch(RestConnection & connection,
                const RestRequest & req,
                const RestRequestParsingContext & context)
{
            
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        auto resource = jsonDecodeStr<ResourceSpec>(req.payload);

        struct Data {
            Watch watch;
            HttpRestConnection connection;
        };

        auto data = std::make_shared<Data>();
        data->connection = dynamic_cast<HttpRestConnection &>(connection);

        // Plain pointer for capture in lambdas we don't want to take a reference
        // to data
        Data * dataPtr = data.get();

        DEBUG_MSG(logger) << "watching " << jsonEncode(resource);

        // Create the watch which will receive the information we need
        data->watch = this->watch(resource, true /* catchUp */,
                                  data->connection.http->getPeerName());

        auto onFire = [=] (const Any & valueIn)
            {
                DEBUG_MSG(logger) << "watchRouter onFire" << jsonEncode(valueIn)
                << " shutdown " << this->shutdown_;
                if (this->shutdown_)
                    return;
                if (!dataPtr->connection.isConnected())
                    return;
                dataPtr->connection.sendPayload(jsonEncode(valueIn).toString());
            };

#if 0
        auto onError = [=] (const WatchError & error)
            {
                if (this->shutdown_)
                    return;
                DEBUG_MSG(logger) << "watchRouter onError" << jsonEncode(error);
            };
#endif

        // Send a header for chunked encoding
        data->connection.sendHttpResponseHeader(200, "application/json",
                                                RestConnection::
                                                CHUNKED_ENCODING);
                
        // Bind everything in so we can get the new status values
        data->watch.bindGeneric(onFire /*, onError*/);

        // Save the state associated with the watch on the connection
        data->connection.piggyBack.push_back(data);

        return RestRequestRouter::MR_ASYNC;
    } catch (const std::exception & exc) {
        connection.sendResponse(400, exc.what(), "text/plain");
        return RestRequestRouter::MR_YES;
    }
}

void
ServicePeer::
initServer(std::shared_ptr<PeerServer> server)
{
    this->peerServer = server;
}

void
ServicePeer::
onNewConnection(std::shared_ptr<PeerConnection> conn)
{
    DEBUG_MSG(logger) << peerInfo.peerName << " accepted new connection";
    auto entry = std::make_shared<RemotePeer>(this);
    PeerInfo remoteInfo = entry->initAfterAccept(conn);

    DEBUG_MSG(logger) << peerInfo.peerName << " got new connection from "
         << remoteInfo.peerName;
    peers.addEntry(remoteInfo.peerName, entry);

    // Start it running
    entry->start();
}

void
ServicePeer::
initDiscovery(std::shared_ptr<PeerDiscovery> discovery)
{
    this->discovery = discovery;
    this->discoveryWatch = discovery->knownPeers.watchElements("*", true, nullptr);
    this->discoveryWatch.bind(std::bind(&ServicePeer::onNewPeer,
                                        this,
                                        std::placeholders::_1));
}

void
ServicePeer::
onNewPeer(const RestCollection<std::string, PeerInfo>::ChildEvent & newPeer)
{
    DEBUG_MSG(logger) << "peer " << peerInfo.peerName << " found a new peer " << newPeer.key;
    DEBUG_MSG(logger) << "newPeer.event = " << newPeer.event;

    switch (newPeer.event) {
    case CE_NEW:
        if (newPeer.key == peerInfo.peerName) {
            if (jsonEncode(*newPeer.value) != jsonEncode(peerInfo))
                throw MLDB::Exception("our peer was discovered with different info");
            DEBUG_MSG(logger) << "new entry for me " << peerInfo.peerName << " "
                 << newPeer.key << " " << jsonEncode(peers.getKeys());
            ExcAssert(peers.tryGetExistingEntry(peerInfo.peerName));
        }
        else if (newPeer.key < peerInfo.peerName) {
            auto entry = peers.tryGetExistingEntry(newPeer.key);
            if (entry) {
                logger->error() << "onNewPeer: peer " << newPeer.key << " already exists";
                abort();  // logic error
            }

            // We connect
            // TODO: async connect (for when we have lots of peers)

            try {
                DEBUG_MSG(logger) << "peer " << peerInfo.peerName 
                     << " is connecting to new peer "
                     << newPeer.key;
                auto conn = peerServer->connect(*newPeer.value);
                auto entry = std::make_shared<RemotePeer>(this);
                entry->initAfterConnect(conn, *newPeer.value);

                DEBUG_MSG(logger) << "peer " << peerInfo.peerName 
                     << " being added after connecting to "
                     << newPeer.key;

                peers.addEntry(newPeer.key, entry);
                
                // Now it's added we can make it live
                entry->start();
            } catch (const std::exception & exc) {
                logger->error() << "error connecting to peer " << peerInfo.peerName
                     << ": " << exc.what();
            }
        }
        else {
            // We accept.  Once that's done we add ourselves to the peers.
            DEBUG_MSG(logger) << "waiting for accept from peer " << newPeer.key;
        }
        break;
    case CE_DELETED:
        DEBUG_MSG(logger) << "deleted peer " << newPeer.key;
        if (newPeer.key == peerInfo.peerName) {
            // We lost our own entry.  We need to restart.
            logger->warn() << "WARNING: peer " << peerInfo.peerName
                 << " lost its own entry in discovery.  Letting it come back";
            return;
        }
        peers.deleteEntry(newPeer.key);
        break;
    case CE_UPDATED:
        DEBUG_MSG(logger) << "updated peer " << newPeer.key;
        DEBUG_MSG(logger) << "TODO: implement updates";
    default:
        throw MLDB::Exception("unexpected discovery event type");
    }
}

void
ServicePeer::
onPeerStateChange(RemotePeer * peer)
{
    if (this->shutdown_)
        return;

    if (peer->state != PS_ERROR && peer->state != PS_SHUTDOWN)
        return;
    
    PeerInfo info = peer->remotePeerInfo;
    
    //DEBUG_MSG(logger) << "peer state change for " << jsonEncode(info);

    // post this so that we can let it get out of its callback
    peerServer->postWork([=] () { peers.deleteEntry(info.peerName); });
}

std::string
ServicePeer::
bindTcp(const PortRange & portRange, const std::string & host)
{
    return HttpRestService::bindTcp(portRange, host);
}

namespace {
// G++ 4.7 and later
std::future_status getStatus(std::future_status st)
{
    return st;
}

// G++ 4.6 returns a bool, against the standard
MLDB_UNUSED std::future_status getStatus(bool st)
{
    return (st ? std::future_status::ready : std::future_status::timeout);
}

} // file scope        

void
ServicePeer::
start()
{
    shutdown_ = false;

    // 1.  Start listening for connections
    auto newPeerInfo = peerServer->listen(peerInfo);
    peerInfo = newPeerInfo;

    //DEBUG_MSG(logger) << "after: " << jsonEncode(peerInfo);

    // 2.  Check that we can establish a connection with our published
    // address (externally).  To do this we go through the entire connection
    // protocol from both sides.
    if (peerServer->supportsExternalConnections()) {
        std::promise<PeerInfo> acceptedPeerInfo;
        auto selfAcceptEntry = std::make_shared<RemotePeer>(this);

        peerServer->setNewConnectionHandler
            ([&] (std::shared_ptr<PeerConnection> accepted)
             {
                selfAcceptEntry->initAfterAccept(accepted);
                //DEBUG_MSG(logger) << "**** ONNEWCONNECTION";
                acceptedPeerInfo.set_value(selfAcceptEntry->remotePeerInfo);
                //DEBUG_MSG(logger) << "accept done";
            });

        auto selfConnection = peerServer->connect(newPeerInfo);
        auto selfConnectEntry = std::make_shared<RemotePeer>(this);
        selfConnectEntry->initAfterConnect(selfConnection, newPeerInfo);
        //DEBUG_MSG(logger) << "got selfConnection and connect done";

        std::future<PeerInfo> future = acceptedPeerInfo.get_future();

        // Give it one second.  getStatus() call is G++ 4.7 workaround
        std::future_status status
            = getStatus(future.wait_for(std::chrono::seconds(1)));
        if (status != std::future_status::ready)
            throw MLDB::Exception("Self connection not accepted after "
                                "one second");
        
        PeerInfo info = future.get();

        ExcAssertEqual(jsonEncode(newPeerInfo), jsonEncode(info));

        peerServer->setNewConnectionHandler(nullptr);
    }

    // 2.  Connect to ourself
    DEBUG_MSG(logger) << "Checking we can connect to ourself...";
    auto selfConnection = peerServer->connectToSelf();
    auto selfEntry = std::make_shared<RemotePeer>(this);
    selfEntry->initAfterConnect(selfConnection, newPeerInfo);

    // ... and check that we could
    if (selfEntry->state != PS_OK) {
        throw MLDB::Exception("Can't start service peer: unable to talk to itself (error is '" + selfEntry->error + "').  Check that etcd is OK and that the published port and hostname is really the exterally accessible port and hostname.");
    }
    //DEBUG_MSG(logger) << "done checking";

    peerServer->setNewConnectionHandler(std::bind(&ServicePeer::onNewConnection,
                                                  this,
                                                  std::placeholders::_1));
    

    // 3.  Add our entry
    peers.addEntry(peerInfo.peerName, selfEntry);

    //DEBUG_MSG(logger) << "publishing";

    // 4.  Announce ourselves to the world
    discovery->publish(peerInfo);

    //DEBUG_MSG(logger) << "starting";

    // 5.  Start our self entry running
    selfEntry->start();
}

void
ServicePeer::
shutdown()
{
    httpEndpoint->closePeer();

    this->shutdown_ = true;
    if (discovery)
        discovery->shutdown();
    if (peerServer)
        peerServer->shutdown();

    auto cleanupPeer = [&] (string peerName, RemotePeer & entry)
        {
            entry.shutdown();
            return true;
        };
    peers.forEachEntry(cleanupPeer);
    peers.shutdown();

    httpEndpoint->shutdown();
}

std::vector<WatchStatus>
ServicePeer::
getLocalWatchesForPeer(const std::string & peer) const
{
    auto entry = peers.getExistingEntry(peer);
    return entry->getLocalWatches();
}

std::vector<WatchStatus>
ServicePeer::
getRemoteWatchesForPeer(const std::string & peer) const
{
    auto entry = peers.getExistingEntry(peer);
    return entry->getRemoteWatches();
}

std::vector<LinkStatus>
ServicePeer::
getLocalLinksForPeer(const std::string & peer) const
{
    auto entry = peers.getExistingEntry(peer);
    return entry->getLocalLinks();
}

std::vector<LinkStatus>
ServicePeer::
getRemoteLinksForPeer(const std::string & peer) const
{
    auto entry = peers.getExistingEntry(peer);
    return entry->getRemoteLinks();
}

Utf8String
ServicePeer::
getUriForPath(ResourcePath path)
{
    Utf8String result = "/v1";
    for (auto & e: path)
        result += "/" + e;
    return result;
}

std::shared_ptr<EntityLinkToken>
ServicePeer::
doCreateLink(RestEntity * forWho,
             const std::vector<Utf8String> & remotePath,
             const std::string & linkType,
             Any linkParams)
{
    //DEBUG_MSG(logger) << "creating link ";
    //DEBUG_MSG(logger) << "from " << jsonEncode(forWho->getPath());
    //DEBUG_MSG(logger) << "to   " << jsonEncode(remotePath);
    //DEBUG_MSG(logger) << "linkType " << linkType;
    //DEBUG_MSG(logger) << "linkParams " << jsonEncode(linkParams);

    return this->acceptLink(forWho->getPath(),
                            remotePath,
                            linkType,
                            linkParams);
}

std::vector<std::string>
ServicePeer::
knownPeers() const
{
    return peers.getKeys();
}

PeerStatus
ServicePeer::
getPeerStatus(const std::string & peer) const
{
    return peers.getExistingEntry(peer)->status();
}


std::vector<PeerStatus>
ServicePeer::
getPeerStatuses() const
{
    vector<PeerStatus> result;

    auto doPeer = [&] (string peerName, const RemotePeer & entry)
        {
            result.emplace_back(entry.status());
            return true;
        };
    peers.forEachEntry(doPeer);
    
    return result;
}

int64_t
ServicePeer::
sendPeerMessage(const std::string & peer,
                MessagePriority priority,
                Date deadline,
                int layer,
                int type,
                std::vector<std::string> && message,
                OnResponse onResponse,
                OnError onError)
{
    auto p = peers.tryGetExistingEntry(peer);
    if (!p) {
        if (onError) {
            PeerMessage peerMessage;
            peerMessage.messageId = -1;
            peerMessage.priority = priority;
            peerMessage.state = PeerMessage::SEND_ERROR;
            peerMessage.error = "Unknown peer";
            peerMessage.deadline = deadline;
            peerMessage.layer = layer;
            peerMessage.type = type;
            peerMessage.payload = std::move(message);

            try {
                onError(std::move(peerMessage));
            } MLDB_CATCH_ALL {
                logger->error() << "error on onError handler of message";
                abort();
            }
        }
        return -1;
    }
    
    RemotePeer & entry = *p;

    return entry.sendMessage(priority, deadline, layer, type,
                             std::move(message),
                             std::move(onResponse),
                             std::move(onError));
}

void
ServicePeer::
handlePeerMessage(RemotePeer * peer, PeerMessage && msg)
{
    throw MLDB::Exception("Service peer of type %s doesn't support peer to peer "
                        "messages", MLDB::type_name(*this).c_str());
}

WatchT<Date>
ServicePeer::
getTimer(Date nextExpiry, double period,
         std::function<void (Date)> toBind)
{
    return peerServer->getTimer(nextExpiry, period, toBind);
}


/*****************************************************************************/
/* PEER COLLECTION                                                           */
/*****************************************************************************/

void
ServicePeer::PeerCollection::
initRoutes(RouteManager & manager)
{
    RestCollection<std::string, RemotePeer>::initRoutes(manager);

    auto getServicePeer = [=] (const RestRequestParsingContext & cxt)
        {
            return service;
        };

    // See MLDBFB-236
    #if 0
    RestRequestRouter::OnProcessRequest getKnownPeersRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                auto service = getServicePeer(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto res = service->knownPeers();
                connection.sendHttpResponse(200, jsonEncodeStr(res),
                                            "application/json", {});
                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                connection.sendResponse(400, exc.what(), "text/plain");
                return RestRequestRouter::MR_YES;
            }
        };
    #endif   
    Json::Value help;
    help["result"] = "List of " + manager.nounPlural;
    
    //manager.collectionNode->addRoute("", { "GET" },
    //                               "Get information about all peers",
    //                               getKnownPeersRoute, help);

    RestRequestRouter::OnProcessRequest getPeerStatusRoute
        = [=] (RestConnection & connection,

               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                auto service = getServicePeer(cxt);
                auto key = manager.getKey(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto res = service->getPeerStatus(key);
                connection.sendHttpResponse(200, jsonEncodeStr(res),
                                            "application/json", {});
                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                connection.sendResponse(400, exc.what(), "text/plain");
                return RestRequestRouter::MR_YES;
            }
        };

    help["result"] = "Status of given peer";

    manager.valueNode->addRoute("/status", { "GET" },
                                "Get information about a single peer",
                                getPeerStatusRoute, help);
}

std::pair<const std::type_info *,
          std::shared_ptr<const ValueDescription> >
ServicePeer::PeerCollection::
getWatchBoundType(const ResourceSpec & spec)
{
    if (spec.empty())
        throw MLDB::Exception("no type for empty spec");

    if (spec.size() == 1) {
        if (spec[0].channel == "names")
            return make_pair(&typeid(tuple<std::string>), nullptr);
        else if (spec[0].channel == "children")
            return make_pair(&typeid(tuple<RestEntityChildEvent>), nullptr);
        else throw MLDB::Exception("PeerCollection has no channel named '%s'",
                                 spec[0].channel.rawData());
    }
    
    if (spec[0].channel != "children")
        throw MLDB::Exception("PeerCollection has only a children channel, "
                            "not '%s'", spec[0].channel.rawData());

    return service->getWatchBoundType(ResourceSpec(spec.begin() + 1, spec.end()));
}

Watch
ServicePeer::PeerCollection::
watchChannel(const Utf8String & channel,
             const Utf8String & filter,
             bool catchUp,
             Any info)
{
    //DEBUG_MSG(logger) << "PeerCollection::watchChannel " << channel << " " << filter
    //     << " " << catchUp << " " << jsonEncodeStr(info);

    if (channel == "children")
        return watchChildren(filter, catchUp, info);
    else if (channel == "elements")
        return watchElements(filter, catchUp, info);
    else if (channel == "names") {
        return watchNames(filter, catchUp, info);
#if 0
        return transform<std::string>(watchChildren(filter, catchUp, info),
                                     [] (const RestEntityChildEvent & ev)
                                      { DEBUG_MSG(logger) << "channel event " << ev.name << " " << ev.event;
                                         return (ev.event == CE_NEW
                                               ? "+" : "-") + ev.name; });
#endif
    }
    else throw MLDB::Exception("No channel named '%s' on PeerCollection",
                             channel.rawData());
}

template class RestCollection<std::string, RemotePeer>;

} // namespace MLDB
