// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* remote_peer.h                                                   -*- C++ -*-
   Jeremy Barnes, 12 May 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "rest_collection.h"
#include "mldb/watch/watch.h"
#include "peer_message.h"
#include "peer_connection.h"
#include <atomic>

namespace MLDB {

struct ServicePeer;

/// Statistics for a given type of ping
struct PingStats {
    PingStats()
        : latestTimeMs(-0.0)
    {
    }

    double latestTimeMs;
    Date latestReceived;
        
    void record(double pingTimeMs, Date when)
    {
        latestTimeMs = pingTimeMs;
        latestReceived = when;
    }
};

/// Statistics for a peer
struct PeerStats {
    PingStats ping;
};

enum PeerState {
    PS_UNKNOWN,         ///< State of peer is unknown
    PS_CONNECTING,      ///< Peer is currently connecting
    PS_OK,              ///< Peer is currently connected and OK
    PS_ERROR,           ///< Peer currently has an error
    PS_SHUTDOWN         ///< Peer has been shut down
};

std::ostream & operator << (std::ostream & stream, PeerState s);

/// Status of a peer
struct PeerStatus {
    std::string peerName;
    PeerInfo peerInfo;
    PeerState state;
    std::string error;
    PeerStats stats;
    Date lastMessageReceived;
    double timeSinceLastMessageReceivedMs;
    int64_t messagesSent;
    int64_t messagesReceived;
    int64_t messagesReceivedAfterDeadline;
    int64_t messagesQueued0;
    int64_t messagesQueued1;
    int64_t messagesAwaitingResponse;
    int64_t messagesWithDeadline;
    int64_t messagesTimedOut;
    int64_t responsesSent;
    PeerConnectionStatus connection;
};

/** Structure containing the status of a peer watch (local or remote). */
struct WatchStatus {
    int64_t watchId;
    Any info;
    int attached;
    ResourceSpec spec;
    int triggers;
    int errors;
};

/** Structure containing the status of a link (local or remote). */
struct LinkStatus {
    int64_t linkId;
    LinkState state;
    int messagesSend;
    int messagesReceived;
    int stateChanges;
};

    
/*****************************************************************************/
/* REMOTE PEER                                                               */
/*****************************************************************************/

/// Entry for a peer real-time behaviour service
struct RemotePeer: public RestEntity {
    RemotePeer(ServicePeer * owner);

    ~RemotePeer();

    void initAfterConnect(std::shared_ptr<PeerConnection> connection,
                          PeerInfo remotePeerInfo);

    PeerInfo initAfterAccept(std::shared_ptr<PeerConnection> connection);

    virtual bool isCollection() const
    {
        return false;
    }

    virtual Utf8String getDescription() const
    {
        return L"Peer running on another machine";
    }

    virtual RestRequestRouter & initRoutes(RestRequestRouter & parent)
    {
        return parent;
        // ...
    }

    /** Get our path in the REST hierarchy. */
    virtual std::vector<Utf8String> getPath() const
    {
        return { L"peers", Utf8String(remotePeerInfo.peerName) };
    }

    virtual Utf8String getName() const
    {
        return Utf8String(remotePeerInfo.peerName);
    }
        
    /** Get our parent. */
    virtual RestEntity * getParent() const;

    virtual std::pair<const std::type_info *,
                      std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec);

    void shutdown();

    // These two are protected by statsMutex
    PeerState state;
    std::string error;

    ServicePeer * owner;

    PeerInfo remotePeerInfo;
    
    std::shared_ptr<PeerConnection> connection;

    /** Construct an object with the status of this peer. */
    PeerStatus status() const;

    double totalBlockedTime;
    double totalSleepTime;
    double totalAwakeTime;
    double lastBlockedSeconds;
    double lastSleepSeconds;
    double lastAwakeSeconds;
    Date lastCheckTime;

    mutable std::mutex statsMutex;
    PeerStats stats;

    std::multiset<std::pair<Date, int64_t> > deadlines;
    std::map<int64_t, PeerMessage> awaitingResponse;

    int64_t sendMessage(MessagePriority priority,
                        Date deadline,
                        int layer,
                        int type,
                        std::vector<std::string> && message,
                        OnResponse onResponse = nullptr,
                        OnError onError = nullptr);

    void sendResponse(PeerMessage && msg);

    enum LayerOneMessageType {
        PING,           ///< Check connection is working

        WATCH,          ///< Create a new watch
        WATCHRELEASED,  ///< A watch was released
        WATCHFIRED,     ///< Signal that a watch has fired
        WATCHSTATUS,    ///< Signal that a watch has disappeared

        LINKCREATE,     ///< A new link was created   
        LINKSTATEYOURS, ///< Someone else's link changed state
        LINKSTATEOURS,  ///< Our link changed state
        LINKOUTBOUND,   ///< Outbound link message (source to dest)
        LINKINBOUND,    ///< Inbound link message (dest to source)
    };

    void checkMessageQueue();
    Date checkDeadlines();
    void handleMessageIn(PeerMessage && message);
    void handleResponse(PeerMessage && message);
    bool send(PeerMessage && msg, MessageDirection dir);

    void recordHit(const char * msg)
    {
    }

    void recordHit(const std::string & str)
    {
    }

    std::atomic<int64_t> messagesEnqueued, messagesSent, messagesAcknowledged;
    std::atomic<int64_t> messagesReceived, messagesReceivedAfterDeadline, messagesTimedOut;
    int64_t responsesSent;
    mutable std::mutex messagesMutex;
    std::deque<PeerMessage> messages;
    std::deque<PeerMessage> responses;
    int64_t currentMessageId;
    int shutdown_;
    Date lastMessageReceived;

    /** Put the given fully formed message on the queue to be sent. */
    void enqueueMessage(PeerMessage && msg);

    bool gotMessage(std::string && message);
    bool getMessage(std::string & msg);

    /** Get a watch on the given channel on the remote system. */
    virtual WatchT<std::vector<Utf8String>, Any>
    watchWithPath(const ResourceSpec & spec, bool catchUp, Any info,
                  const std::vector<Utf8String> & currentPath
                  = std::vector<Utf8String>());

    /** Overridden acceptLink implementation that pushes it over
        to the other side via watches.
    */
    std::shared_ptr<EntityLinkToken>
    acceptLink(const std::vector<Utf8String> & sourcePath,
               const std::vector<Utf8String> & targetPath,
               const std::string & linkType,
               Any linkParams);
        
    /// Mutex on remote watches
    mutable std::mutex remoteWatchMutex;

    /// Watches the remote peer has set on us
    std::map<int64_t, Watch> remoteWatches;

    /// Current ID for local watches
    int64_t localWatchNumber;

    struct LocalWatch {
        WatchesT<std::vector<Utf8String>, Any> watches;
        Any info;
        std::shared_ptr<const ValueDescription> desc;
    };

    /// Watches we've set on a remote host
    std::map<int64_t, std::shared_ptr<LocalWatch> > localWatches;

    /** Return the status of all local watches. */
    std::vector<WatchStatus> getLocalWatches() const;

    /** Return the status of all remote watches. */
    std::vector<WatchStatus> getRemoteWatches() const;

    /** Return the status of all local links. */
    std::vector<LinkStatus> getLocalLinks() const;

    /** Return the status of all remote links. */
    std::vector<LinkStatus> getRemoteLinks() const;


    int localLinkNumber;

    struct LocalLink {
        std::shared_ptr<EntityLinkToken> token;
        WatchT<Any> dataWatch;
        WatchT<LinkState> stateWatch;
    };

    /// Links we've set on a remote host
    std::map<int64_t, std::shared_ptr<LocalLink> > localLinks;

    struct RemoteLink {
        std::shared_ptr<EntityLinkToken> token;
        WatchT<Any> dataWatch;
        WatchT<LinkState> stateWatch;
    };

    std::map<int64_t, std::shared_ptr<RemoteLink> > remoteLinks;

    WatchT<Date> heartbeat;
    WatchT<Date> earliestMessageExpiry;
    WatchT<PeerConnectionState> stateWatch;

    void handleLayerOneMessage(PeerMessage & message);
    void handleRemotePing(std::vector<std::string> & message);
    void handleRemoteCreateWatch(std::vector<std::string> & message);
    void handleRemoteReleaseWatch(std::vector<std::string> & message);
    void handleRemoteWatchFired(std::vector<std::string> & message);
    void handleRemoteWatchStatus(std::vector<std::string> & message);
    void handleRemoteCreateLink(std::vector<std::string> & message);
    void handleRemoteLinkStateYours(std::vector<std::string> & message);
    void handleRemoteLinkStateOurs(std::vector<std::string> & message);
    void handleRemoteLinkInbound(std::vector<std::string> & message);
    void handleRemoteLinkOutbound(std::vector<std::string> & message);

    void deletePeerWatch(int64_t externalWatchId);
    void sendPeerWatchFired(int64_t externalWatchId, const Any & ev);
    void startWriting();

    /** Common part of the initialization code; called after both
        initAfterAccept and initAfterConnect.
    */
    void finishInit();

    /** Start the remote peer running. */
    void start();

    /** Check how healthy the connection, etc is. */
    void checkConnectionState();
};


DECLARE_ENUM_DESCRIPTION_NAMED(ServicePeerStateDescription,
                              PeerState);

DECLARE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerPingStatsDescription,
                                   PingStats);

DECLARE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerPeerStatsDescription,
                                   PeerStats);

DECLARE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerPeerStatusDescription,
                                   PeerStatus);

DECLARE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerWatchStatusDescription,
                                   WatchStatus);

DECLARE_STRUCTURE_DESCRIPTION(LinkStatus);


} // file scope
