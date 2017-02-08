// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer.h                                                  -*- C++ -*-
   Jeremy Barnes, 3 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "rest_collection.h"
#include "remote_peer.h"
#include "peer_discovery.h"
#include "mldb/watch/watch.h"
#include "mldb/rest/http_rest_service.h"
#include "mldb/utils/log_fwd.h"

namespace MLDB {


struct RestRequestRouter;



/*****************************************************************************/
/* SERVICE PEER                                                              */
/*****************************************************************************/

/** This is a base class for a service that operates as a set of cooperating
    peers.

    It provides functionality for the peers to:
    * Discover each other
    * Send messages to each other
    * Broadcast messages
    * Have a leader/follower election

    Each peer also knows about a hierarchical set of entities that are
    available under it.  This allows
    * A consistent naming scheme across entities
    * A REST interface for objects that are made available
    * Watches to be set up over both local and remote objects, which can
      also be used for publish/subscribe semantics
    * Sending messages directly to a given object, either on the same or a
      different peer
*/

struct ServicePeer
    : public RestDirectory,
      public HttpRestService {

    ServicePeer(const std::string & peerName,
                const std::string & serviceType, 
                const std::string & serviceLocation,
                bool enableLogging = false);

    ~ServicePeer()
    {
        shutdown();
    }

    /** Set up the routes. */
    void addRoutes();

    /** Set up the given server to be used to accept connections from and
        connect to other peers.
    */
    void initServer(std::shared_ptr<PeerServer> server);

    /** Set up the given discovery service to publish our peer and
        get information from others.
    */
    void initDiscovery(std::shared_ptr<PeerDiscovery> discovery);

    /** Bind the REST interface to an HTTP TCP port, returning the
        URI at which it can be accessed.
    */
    std::string bindTcp(const PortRange & portRange = PortRange(),
                        const std::string & host = "localhost");

    /** Start the HTTP server running. */
    void start();

    /** Shut everything down. */
    void shutdown();

public:
    const std::string & getLocalPeerName() const
    {
        return peerInfo.peerName;
    }

    const std::string & getFromPeersAddress() const
    {
        return peerInfo.uri;
    }

    /*************************************************************************/
    /* PEER TO PEER MESSAGING                                                */
    /*************************************************************************/

    /** Send a message to the given peer server.  If given, then call
        the given callback on a response.

        Returns a message ID that can be used to match up a response or
        cancel the message.

        This function is thread-safe.
    */
    int64_t sendPeerMessage(const std::string & peer,
                            MessagePriority priority,
                            Date deadline,
                            int layer,
                            int type,
                            std::vector<std::string> && message,
                            OnResponse onResponse = nullptr,
                            OnError onError = nullptr);
    
    /** Handle a message from the peer.  Default will crash saying that this
        peer does not support peer to peer messaging.

        Note that watches and links are still supported even if this
        message isn't supported, and this is the best way to send messages
        between peers.  This method exists only for rare cases when you
        want two peers to talk to each other directly, not in the context of
        a connection through a link or watch.
    */
    virtual void handlePeerMessage(RemotePeer * peer, PeerMessage && msg);
    

    /*************************************************************************/
    /* PEER TO PEER CONNECTIONS                                              */
    /*************************************************************************/

    /** Return the list of currently known peers. */
    std::vector<std::string> knownPeers() const;

    /** Return the current status of a single peer. */
    PeerStatus getPeerStatus(const std::string & peers) const;

    /** Return the status of all known peers. */
    std::vector<PeerStatus> getPeerStatuses() const;


    /*************************************************************************/
    /* WATCHES                                                               */
    /*************************************************************************/

    /** Watch the given object or class of objects, either on this peer or
        a remote one.  Returns the watch, which will be pre-populated with
        the existing state of the system.
        
        The first return value of the watch is always the path of the element
        that is being watched.
    */

    std::vector<Any> getPeerWatches() const
    {
        return peers.getChildWatches();
    }

    /** Return the status of local watches for the given peer. */
    std::vector<WatchStatus>
    getLocalWatchesForPeer(const std::string & peer) const;

    /** Return the status of remote watches from the given peer. */
    std::vector<WatchStatus>
    getRemoteWatchesForPeer(const std::string & peer) const;

    /** Return the status of local links for the given peer. */
    std::vector<LinkStatus>
    getLocalLinksForPeer(const std::string & peer) const;

    /** Return the status of remote links from the given peer. */
    std::vector<LinkStatus>
    getRemoteLinksForPeer(const std::string & peer) const;

    /** Get the URI for the given path. */
    virtual Utf8String getUriForPath(ResourcePath path);


    /*************************************************************************/
    /* TIMERS                                                                */
    /*************************************************************************/

    virtual WatchT<Date> getTimer(Date nextExpiry, double period = -0.0,
                                  std::function<void (Date)> toBind = nullptr);
    

    /// Router to deal with REST requests
    RestRequestRouter router;

private:
    friend class RemotePeer;

    PeerInfo peerInfo;

    /** Server that accepts incoming peer connections. */
    std::shared_ptr<PeerServer> peerServer;

    /** Discovery service. */
    std::shared_ptr<PeerDiscovery> discovery;

    /** Watch on the discovery service's collection of peers. */
    WatchT<RestCollection<std::string, PeerInfo>::ChildEvent> discoveryWatch;

    void onNewConnection(std::shared_ptr<PeerConnection> connection);
    void onNewPeer(const RestCollection<std::string, PeerInfo>::ChildEvent & newPeer);
    /** Called by the RemotePeer when its state has changed. */
    void onPeerStateChange(RemotePeer * peer);

    std::atomic<bool> shutdown_;
    //volatile int shutdown_;

    /** This provides the collection of peers. */
    struct PeerCollection
        : public RestCollection<std::string, RemotePeer> {
        PeerCollection(ServicePeer * service)
            : RestCollection<std::string, RemotePeer>(L"peer", L"peers", service),
              service(service)
        {
        }

        ServicePeer * service;

        void initRoutes(RouteManager & routeManager);

        virtual std::pair<const std::type_info *,
                          std::shared_ptr<const ValueDescription> >
        getWatchBoundType(const ResourceSpec & spec);

        virtual Watch watchChannel(const Utf8String & channel,
                                   const Utf8String & filter,
                                   bool catchUp,
                                   Any info);

    };

    PeerCollection peers;

    std::shared_ptr<EntityLinkToken>
    doCreateLink(RestEntity * forWho,
                 const std::vector<Utf8String> & remotePath,
                 const std::string & linkType,
                 Any linkParams);

    /** Route used to create a watch from HTTP. */
    RestRequestMatchResult
    createHttpWatch(RestConnection & connection,
                    const RestRequest & req,
                    const RestRequestParsingContext & context);

    std::shared_ptr<spdlog::logger> logger;
};

extern template class RestCollection<std::string, RemotePeer>;

} // namespace MLDB
