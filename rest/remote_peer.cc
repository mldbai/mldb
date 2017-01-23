// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* remote_peer.cc                                                  -*- C++ -*-
   Jeremy Barnes, 12 May 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "remote_peer.h"
#include "service_peer.h"
#include <sys/timerfd.h>
#include "mldb/watch/watch_impl.h"
#include "call_me_back.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/jml/utils/vector_utils.h"


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* REMOTE PEER                                                               */
/*****************************************************************************/

RemotePeer::
RemotePeer(ServicePeer * owner)
    : state(PS_UNKNOWN),
      owner(owner),
      messagesEnqueued(0), messagesSent(0),
      messagesAcknowledged(0), messagesReceived(0),
      messagesReceivedAfterDeadline(0),
      messagesTimedOut(0),
      responsesSent(0),
      currentMessageId(0),
      shutdown_(false),
      localWatchNumber(0), localLinkNumber(0)
{
    //cerr << "construction: " << this << endl;
}

RemotePeer::
~RemotePeer()
{
    //cerr << "destruction: " << this << endl;
    shutdown();
    //cerr << "destruction done: " << this << endl;
}

RestEntity *
RemotePeer::
getParent() const
{
    return owner;
}

void
RemotePeer::
initAfterConnect(std::shared_ptr<PeerConnection> connection,
                 PeerInfo remotePeerInfo)
{
    //cerr << "initAfterConnect: " << this << endl;

    this->remotePeerInfo = remotePeerInfo;
    this->connection = connection;

    connection->send(jsonEncodeStr(owner->peerInfo));

    finishInit();
}

PeerInfo
RemotePeer::
initAfterAccept(std::shared_ptr<PeerConnection> connection)
{
    //cerr << "initAfterAccept: " << this << endl;
    this->connection = connection;

    // We just accepted a connection.  Wait for the other end to send through
    // the handshake.

    std::string payload;

    std::timed_mutex mutex;
    mutex.lock();



    auto onRecv = [&] (std::string && data)
        {
            payload = std::move(data);
            mutex.unlock();
            return false;  // don't read anymore
        };
    
    connection->startReading(onRecv);

    if (!mutex.try_lock_for(std::chrono::milliseconds(1000))) {
        connection->shutdown();
        throw MLDB::Exception("didn't receive handshake after one second");
    }

    remotePeerInfo = jsonDecodeStr<PeerInfo>(payload);

    finishInit();

    return remotePeerInfo;
}

void
RemotePeer::
finishInit()
{
    lastMessageReceived = Date::now();
    this->state = PS_OK;
    this->stateWatch = connection->stateWatches.add();

    auto onStateChange = [&] (PeerConnectionState state)
        {
            cerr << "peer " << owner->peerInfo.peerName
                 << " connection to " << remotePeerInfo.peerName
                 << " changed state to " << state << endl;
            if (state == ST_CLOSED) {
                this->state = PS_ERROR;
                owner->onPeerStateChange(this);
            }
        };

    this->stateWatch.bind(onStateChange);
}

void
RemotePeer::
start()
{
    this->heartbeat = connection->getTimer(Date::now(), 1.0);

    auto gotPingResponse = [=] (const PeerMessage & msg,
                                std::vector<std::string> && payload)
        {
            //Date received = jsonDecodeStr<Date>(payload.at(0));

            Date now = Date::now();
            //double queueTimeMs = msg.timeSent.secondsSince(msg.timeEnqueued) * 1000;
            //double outboundTimeMs = received.secondsSince(msg.timeSent) * 1000;
            double pingTimeMs = now.secondsSince(msg.timeSent) * 1000;
            std::unique_lock<std::mutex> guard(statsMutex);
            stats.ping.record(pingTimeMs, now);
            //cerr << owner->peerInfo.peerName << " got ping from "
            //<< remotePeerInfo.peerName << " q " << queueTimeMs
            //<< " out " << outboundTimeMs << " ping "
            //<< pingTimeMs << endl;
        };

    auto gotPingError = [=] (const PeerMessage & msg)
        {
            cerr << owner->peerInfo.peerName
            << " got nothing back from ping to "
            << remotePeerInfo.peerName << endl;
        };
    
    connection->startReading(std::bind(&RemotePeer::gotMessage,
                                       this,
                                       std::placeholders::_1));

    // NOTE: This must come after startReading, as there will be a heartbeat queued,
    // and if we do this before startReading for our connection to ourself, then
    // we have a race condition and my miss the message.
    this->heartbeat.bind([=] (Date date)
                         {
                             //cerr << "heartbeat: " << this
                             //     << " this->connection = " << this->connection
                             //     << endl;
                             if (!this->connection)
                                 return;

                             //cerr << remotePeerInfo.peerName << " heartbeat" << endl;

                             //cerr << owner->peerInfo.peerName << " pinging "
                             //     << remotePeerInfo.peerName << endl;
                             
                             vector<std::string> message;
                             message.push_back("PING-" + owner->peerInfo.peerName + "-"
                                               + remotePeerInfo.peerName);

                             sendMessage(PRI_NORMAL,
                                         Date::now().plusSeconds(1.0),
                                         1,
                                         PING,
                                         std::move(message),
                                         gotPingResponse,
                                         gotPingError);
                                         

                             this->checkConnectionState();
                         });

    startWriting();
}

void
RemotePeer::
checkConnectionState()
{
    Date now = Date::now();

    double timeSinceLast = now.secondsSince(lastMessageReceived);
    double timeSincePing = now.secondsSince(stats.ping.latestReceived);

    if (timeSinceLast > 1.5) {
        if (state == PS_OK) {
            state = PS_ERROR;
            error = "> 1.5 seconds since any message received";
        }
    }
    else if (stats.ping.latestReceived != Date() && timeSincePing > 1.5) {
        if (state == PS_OK) {
            state = PS_ERROR;
            error = "> 1.5 seconds since ping received";
        }
    }
}

int64_t
RemotePeer::
sendMessage(MessagePriority priority,
            Date deadline,
            int layer,
            int type,
            std::vector<std::string> && message,
            OnResponse onResponse,
            OnError onError)
{
    PeerMessage peerMessage;
    peerMessage.direction = DIR_REQUEST;
    peerMessage.messageId = __sync_fetch_and_add(&currentMessageId, 1);
    peerMessage.priority = priority;
    peerMessage.deadline = deadline;
    peerMessage.payload = message;
    peerMessage.onResponse = onResponse;
    peerMessage.onError = onError;
    peerMessage.state = PeerMessage::QUEUED;
    peerMessage.layer = layer;
    peerMessage.type = type;
    peerMessage.timeEnqueued = Date::now();

#if 0
    cerr << "trying to send message from " << owner->peerInfo.peerName
         << " to " << remotePeerInfo.peerName << endl;
    cerr << "id " << peerMessage.messageId << " layer " << peerMessage.layer
         << " type " << peerMessage.type << " length " << peerMessage.payload.size()
         << endl;
    for (auto & m: peerMessage.payload) {
        string s = m;
        bool allAscii = true;
        for (char c: s)
            if (c < ' ' || c >= 127)
                allAscii = false;
        if (allAscii)
            cerr << s << endl;
        else cerr << "(binary)" << endl;
    }
#endif

    int64_t messageId = peerMessage.messageId;
    enqueueMessage(std::move(peerMessage));
    return messageId;
}

void
RemotePeer::
enqueueMessage(PeerMessage && msg)
{
    //cerr << "enqueueMessage: " << this << " connection " << connection
    //     << endl;
    //ExcAssert(connection);

    bool startWriting = false;

#if 0
        cerr << "enqueuing message from " << owner->peerInfo.peerName
             << " to " << remotePeerInfo.peerName << endl;
        cerr << "id " << msg.messageId << " layer " << msg.layer
             << " type " << msg.type << " length " << msg.payload.size()
             << " dir " << msg.direction << endl;
        for (auto & m: msg.payload) {
            string s = m;
            bool allAscii = true;
            for (char c: s)
                if (c < ' ' || c >= 127)
                    allAscii = false;
            if (allAscii)
                cerr << s << endl;
            else cerr << "(binary)" << endl;
        }
#endif

    {
        std::unique_lock<std::mutex> qGuard(this->messagesMutex);
        startWriting = messages.empty();
        messages.emplace_back(std::move(msg));
    }
    
    startWriting = true;

    if (startWriting && connection)
        this->startWriting();
}

bool
RemotePeer::
getMessage(std::string & payload)
{
    //cerr << "getMessage: " << this << " connection " << connection
    //     << endl;
    std::unique_lock<std::mutex> guard(this->messagesMutex);

    ExcAssert(connection);

    //cerr << "getMessage: messages.size() = " << messages.size() << endl;

    for (;;) {
        if (messages.empty())
            return false;

        auto & msg = messages.front();

        Date now = Date::now();

#if 0
        cerr << "getting to send message from " << owner->peerInfo.peerName
             << " to " << remotePeerInfo.peerName << endl;
        cerr << "id " << msg.messageId << " layer " << msg.layer
             << " type " << msg.type << " length " << msg.payload.size()
             << " dir " << msg.direction << endl;
        for (auto & m: msg.payload) {
            string s = m;
            bool allAscii = true;
            for (char c: s)
                if (c < ' ' || c >= 127)
                    allAscii = false;
            if (allAscii)
                cerr << s << endl;
            else cerr << "(binary)" << endl;
        }
#endif

        auto encoded = msg.encode();
        payload = std::string(encoded->begin(), encoded->end());

        //cerr << "payload.size() = " << payload.size() << endl;

        if (msg.direction == DIR_REQUEST) {
        
            if (msg.deadline <= now) {
                // deadline already expired
                msg.state = PeerMessage::TIMEOUT_SEND;
                msg.error = "Timeout before sending";
                messagesTimedOut += 1;
                if (msg.onError) {
                    try {
                        msg.onError(std::move(msg));
                    } MLDB_CATCH_ALL {
                        cerr << "warning: onError handler threw"
                             << endl;
                    }
                }
                messages.pop_front();
                continue;
            }

            auto it_added
                = this->awaitingResponse.insert(make_pair(msg.messageId,
                                                          PeerMessage()));
            if (!it_added.second) {
                cerr << "attempt to re-use message ID" << endl;
                abort();
            }

            bool newTimer
                = deadlines.empty()
                || msg.deadline < deadlines.begin()->first;
            deadlines.insert(make_pair(msg.deadline, msg.messageId));
            
            if (newTimer) {
                earliestMessageExpiry = connection->getTimer(this->deadlines.begin()->first);
                earliestMessageExpiry.bind(std::bind(&RemotePeer::checkDeadlines, this));
            }
        
            it_added.first->second = std::move(msg);
            it_added.first->second.timeSent = Date::now();
            messagesSent += 1;
        }
        else {
            //throw MLDB::Exception("how do we send a response?");
            responsesSent += 1;
        }

        messages.pop_front();
    
        return true;
    }
}

bool
RemotePeer::
gotMessage(std::string && message)
{
    //cerr << "peer " << owner->peerInfo.peerName
    //     << " got message from " << remotePeerInfo.peerName << ": "
    //     << message << endl;
    
    lastMessageReceived = Date::now();

    PeerMessage msg = PeerMessage::decode(message);

    handleMessageIn(std::move(msg));

    return true;  // yes, we want more messages
}

void
RemotePeer::
shutdown()
{
    //cerr << "shutdown: " << this << endl;

    shutdown_ = true;
    
    heartbeat = WatchT<Date>();
    earliestMessageExpiry = WatchT<Date>();

    if (connection) {
        connection->shutdown();
        connection.reset();
    }
}

void
RemotePeer::
sendResponse(PeerMessage && msg)
{
    //cerr << "************ sending response back for message " << msg.messageId
    //     << endl;

    // Mark it as a response
    msg.direction = DIR_RESPONSE;
    enqueueMessage(std::move(msg));
}

void
RemotePeer::
handleMessageIn(PeerMessage && message)
{
    //cerr << "handleMessageIn" << endl;

    if (message.header.dir == DIR_RESPONSE) {
        //++responsesReceived;
        
        auto messagePtr = std::make_shared<PeerMessage>(std::move(message));
        
        auto work = [messagePtr,this] ()
            {
                PeerMessage & message = *messagePtr;
                
                handleResponse(std::move(message));
            };
        
        connection->postWorkAsync(work);
        //work();
    }
    else {
        ++messagesReceived;

        if (message.layer == 0) {
            throw MLDB::Exception("layer 0 message not understood");
        }
        else if (message.layer == 1) {

            auto messagePtr = std::make_shared<PeerMessage>(std::move(message));

            auto work = [messagePtr,this] ()
                {
                    PeerMessage & message = *messagePtr;
                    
                    handleLayerOneMessage(message);
                    
                    if (!message.payload.empty()) {
                        sendResponse(std::move(message));
                    }
                };
            
            connection->postWorkAsync(work);
            //work();
        }
        else {
            owner->handlePeerMessage(this, std::move(message));
        }
    }
}

void
RemotePeer::
handleResponse(PeerMessage && response)
{
    MessageHeader & header = response.header;

    std::unique_lock<std::mutex> guard(this->messagesMutex);

    int64_t messageId = header.id;

#if 0
    cerr << "got peer response with id " << messageId
         << " layer " << header.layer << " type "
         << header.messageType << " size " << response.payload.size()
         << endl;

    for (auto & m: response.payload) {
        string s = m;
        bool allAscii = true;
        for (char c: s)
            if (c < ' ' || c >= 127)
                allAscii = false;
        if (allAscii)
            cerr << s << endl;
        else cerr << "(binary)" << endl;
    }

#endif

    auto it = this->awaitingResponse.find(messageId);
    if (it == this->awaitingResponse.end()) {
        //thithis->recordHit("unknownPeerMessage");
#if 0
        cerr << "got unknown peer response with id " << messageId
             << " layer " << header.layer << " type "
             << header.messageType << " size " << response.size()
             << " awaitingResponse.size() = " << this->awaitingResponse.size()
             << endl;
#endif
        return;  // unknown message
    }

    PeerMessage peerMessage = std::move(it->second);
    peerMessage.state = PeerMessage::RESPONDED;
    Date earliestDeadline = deadlines.begin()->first;
    this->awaitingResponse.erase(it);
    this->deadlines.erase(std::make_pair(peerMessage.deadline,
                                         peerMessage.messageId));

    bool needToCheckDeadlines = false;

    if (!this->deadlines.empty() && earliestDeadline != deadlines.begin()->first) {
        earliestMessageExpiry = connection->getTimer(this->deadlines.begin()->first);

        // Since we have the messages lock, which also may need to be acquired by the
        // checkDeadlines function, we need to bind our timer in such a way that no
        // recursive callbacks are called.  If there is work to be done (which means,
        // concretely, that the earliest deadline has expired) we will handle it below
        // when we no longer have the lock.

        auto onAlreadyExpired = [&] (Date) { needToCheckDeadlines = true; };
        earliestMessageExpiry.bindNonRecursive(std::bind(&RemotePeer::checkDeadlines, this),
                                               onAlreadyExpired);
    }
    
    guard.unlock();
    
    if (peerMessage.onResponse) {
        try {
            peerMessage.onResponse(std::move(peerMessage),
                                   std::move(response.payload));

        } catch (const std::exception & exc) {
            cerr << "error: exception thrown in onResponse: "
                 << exc.what() << endl;
        } MLDB_CATCH_ALL {
            cerr << "error: exception thrown in onResponse"
                 << endl;
        }
    }

    if (needToCheckDeadlines)
        checkDeadlines();
}

Date
RemotePeer::
checkDeadlines()
{
    std::vector<int64_t> toExpire;
    Date now = Date::now();

    Date newExpiry = now.plusSeconds(1.0);

    {
        std::unique_lock<std::mutex> guard(messagesMutex);

        while (!this->deadlines.empty()
               && this->deadlines.begin()->first <= now) {
            toExpire.push_back(this->deadlines.begin()->second);
            this->deadlines.erase(this->deadlines.begin());
        }

        //cerr << "Check deadlines: there are now " << deadlines.size()
        //     << " deadlines" << endl;

        if (!deadlines.empty())
            newExpiry = deadlines.begin()->first;

        //cerr << "setting deadline to " << now.secondsUntil(newExpiry)
        //     << " seconds from now at " << Date::now().print(6) << endl;
    }

    //cerr << "expiring " << toExpire.size() << " deadlines" << endl;
    
    if (toExpire.empty())
        return newExpiry;

    std::unique_lock<std::mutex> guard(this->messagesMutex);

    for (auto mid: toExpire) {
        auto it = this->awaitingResponse.find(mid);
        if (it == this->awaitingResponse.end()) {
            continue;  // must be already done
        }
        
        PeerMessage & msg = it->second;

        messagesTimedOut += 1;

        if (msg.onError) {
            msg.state = PeerMessage::TIMEOUT_RESPONSE;
            msg.error = "Timeout waiting for response";
            try {
                msg.onError(std::move(msg));
            } MLDB_CATCH_ALL {
                cerr << "Exception in onError handler" << endl;
                abort();
            }
        }

        this->awaitingResponse.erase(it);
    }

    return newExpiry;
}

WatchT<std::vector<Utf8String>, Any>
RemotePeer::
watchWithPath(const ResourceSpec & spec, bool catchUp, Any info,
              const std::vector<Utf8String> & currentPath)
{
    cerr << "peer " << remotePeerInfo.peerName << " on " << owner->peerInfo.peerName
         << " asked to watch " << jsonEncodeStr(spec) << endl;

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    std::vector<std::string> msg;
    msg.emplace_back(jsonEncodeStr(spec));
    msg.emplace_back(jsonEncodeStr(catchUp));
    int64_t externalWatchId = ++localWatchNumber;
    msg.emplace_back(jsonEncodeStr(externalWatchId));
    msg.emplace_back(Any::jsonEncodeStrTyped(info));

    //cerr << "creating local watch " << externalWatchId << endl;
    
    auto & wp = localWatches[externalWatchId];
    ExcAssert(!wp);

    auto data = wp = std::make_shared<LocalWatch>();

    // Called when the local watch is released.  We need to tell the remote
    // end that it's no longer needed.
    auto onWatchReleased = [=] (Watch & watch, Any && info)
        {
            // Don't deal with it when we're shutting down
            if (this->shutdown_)
                return;

            //cerr << "local watch " << externalWatchId << " was released"
            //     << endl;

            // Tell the other side that it can get rid of the watch
            std::vector<std::string> msg;
            msg.emplace_back(jsonEncodeStr(externalWatchId));
            
            this->sendMessage(PRI_NORMAL, Date::now().plusSeconds(1),
                              1, WATCHRELEASED, std::move(msg),
                              nullptr /* ignore response */,
                              nullptr /* ignore errors */);

            // We're being called by the watches, so we can't just
            // delete it now.  Wait for the function to exit before
            // we do it.

            auto doDelete = [=] ()
            {
                std::unique_lock<std::mutex> guard(remoteWatchMutex);
                cerr << "erasing local watch data for " << externalWatchId << endl;
                if (!this->localWatches.erase(externalWatchId)) {
                    throw MLDB::Exception("logic error: "
                                        "released watch with unknown ID");
                }
                //cerr << "done erasing local watch data" << endl;
            };

            callMeBackLater(doDelete);
        }; 

    data->watches.onRelease = onWatchReleased;
    data->info = info;

    auto onResponse = [=] (const PeerMessage & msg,
                           std::vector<std::string> && payload)
        {
            // Our watch was either successfully created or had an error
            ExcAssertEqual(payload.size(), 2);
            string error = payload[0];
            string returnedTypeName = payload[1];

            //cerr << "onResponse with error = " << error << endl;

            if (error.empty()) {
                // Get the value description for the returned type
                data->desc = ValueDescription::get(returnedTypeName);

                if (data->desc) {
                    //data->watches.bindType(data->desc);
                    return;
                }
                else error = "No ValueDescription found for type '"
                         + returnedTypeName + "'";
            }

            //cerr << "data->watches.size() = " << data->watches.size()
            //     << endl;

            //cerr << "error = " << error << endl;

            // Error creating the watch.  Pass on the error...
            if (error != "Already deleted") {
                cerr << "error creating watch for " << jsonEncode(spec) << " "
                << jsonEncode(info) << endl;
                data->watches.error(WatchError(WATCH_ERR_CREATE, error));
            }
                
            // ... and remove it to make sure the other end gets
            // notified.
            std::unique_lock<std::mutex> guard(remoteWatchMutex);
            localWatches.erase(externalWatchId);
        };

    auto onError = [=] (const PeerMessage & msg)
        {
            cerr << "error setting watch on remote channel" << endl;
            cerr << msg.error << endl;

            data->watches.error(WatchError(WATCH_ERR_CREATE, msg.error));

            std::unique_lock<std::mutex> guard(remoteWatchMutex);
            localWatches.erase(externalWatchId);
        };

    // Get the watch before we send the message to avoid races
    auto res = data->watches.add();

    sendMessage(PRI_NORMAL, Date::now().plusSeconds(1),
                1, WATCH, std::move(msg),
                onResponse, onError);

    //cerr << "returning local watch " << externalWatchId << endl;
    
    // Return our watch
    return res;
}

std::pair<const std::type_info *,
          std::shared_ptr<const ValueDescription> >
RemotePeer::
getWatchBoundType(const ResourceSpec & spec)
{
    // For now, assume that it's the same peer on the other end
    return owner->getWatchBoundType(spec);
}

PeerStatus
RemotePeer::
status() const
{
    PeerStatus result;
    result.peerInfo = remotePeerInfo;
    result.peerName = remotePeerInfo.peerName;
    result.state = state;
    result.error = error;
    Date lmr = lastMessageReceived;
    result.lastMessageReceived = lmr;
    result.timeSinceLastMessageReceivedMs
        = 1000.0 * Date::now().secondsSince(lmr);
    {
        std::unique_lock<std::mutex> guard(statsMutex);
        result.stats = stats;
    }
    result.messagesSent = messagesSent;
    result.messagesReceived = messagesReceived;
    result.messagesReceivedAfterDeadline = messagesReceivedAfterDeadline;
    result.messagesTimedOut = messagesTimedOut;
    result.responsesSent = responsesSent;

    {
        std::unique_lock<std::mutex> guard(messagesMutex);
        result.messagesQueued0 = messages.size();
        result.messagesQueued1 = 0;
        result.messagesAwaitingResponse = awaitingResponse.size();
#if 0
        for (auto & m: awaitingResponse) {
            cerr << "message " << m.first << " layer " << m.second.layer
                 << " type " << m.second.type << " state "
                 << m.second.state << " dl " << m.second.deadline
                 << " still awaiting response" << endl;
        }
#endif
        result.messagesWithDeadline = deadlines.size();
    }

    result.connection = connection->getStatus();

    return result;
}

std::vector<WatchStatus>
RemotePeer::
getLocalWatches() const
{
    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    std::vector<WatchStatus> result;

    for (auto & id_w: localWatches) {
        WatchStatus status;
        auto & w = *id_w.second;
        status.watchId = id_w.first;
        status.attached = w.watches.size();
        status.triggers = w.watches.getTriggerCount();
        status.errors = w.watches.getErrorCount();
        status.info = w.info;
        result.push_back(status);
    }

    return result;
}

std::vector<WatchStatus>
RemotePeer::
getRemoteWatches() const
{
    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    std::vector<WatchStatus> result;

    for (auto & id_w: remoteWatches) {
        WatchStatus status;
        auto & w = id_w.second;
        status.watchId = id_w.first;
        status.attached = w.attached();
        status.triggers = w.getTriggerCount();
        status.errors = w.getErrorCount();
        status.info = w.info();
        result.push_back(status);
    }

    return result;
}

std::vector<LinkStatus>
RemotePeer::
getLocalLinks() const
{
    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    std::vector<LinkStatus> result;

    for (auto & id_l: localLinks) {
        LinkStatus status;
        auto & l = *id_l.second;
        status.linkId = id_l.first;
        status.state = l.token->getState();
        result.push_back(status);
    }

    return result;
}

std::vector<LinkStatus>
RemotePeer::
getRemoteLinks() const
{
    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    std::vector<LinkStatus> result;

    for (auto & id_l: remoteLinks) {
        LinkStatus status;
        auto & l = id_l.second;
        status.linkId = id_l.first;
        status.state = l->token->getState();
        result.push_back(status);
    }

    return result;
}

std::shared_ptr<EntityLinkToken>
RemotePeer::
acceptLink(const std::vector<Utf8String> & sourcePath,
           const std::vector<Utf8String> & targetPath,
           const std::string & linkType,
           Any linkParams)
{
    cerr << "RemotePeer acceptLink: sourcePath = " << sourcePath
         << " targetPath = " << jsonEncode(targetPath) << endl;

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    std::vector<std::string> msg;
    int64_t externalLinkId = ++localLinkNumber;
    msg.emplace_back(jsonEncodeStr(externalLinkId));
    msg.emplace_back(jsonEncodeStr(sourcePath));
    msg.emplace_back(jsonEncodeStr(targetPath));
    msg.emplace_back(jsonEncodeStr(linkType));
    msg.emplace_back(Any::jsonEncodeStrTyped(linkParams));

    auto entry = std::make_shared<LocalLink>();
    std::shared_ptr<EntityLinkToken> res;
    
    vector<Utf8String> targetPath2{ "peers", remotePeerInfo.peerName };
    targetPath2.insert(targetPath2.end(), targetPath.begin(), targetPath.end());

    std::unique_ptr<EntityLinkToken> connectEnd(new EntityLinkToken(sourcePath));
    std::unique_ptr<EntityLinkToken> acceptEnd(new EntityLinkToken(targetPath2));

    std::tie(res, entry->token)
        = MLDB::createLinkT<EntityLinkToken>(std::move(connectEnd),
                                                   std::move(acceptEnd),
                                                   LS_CONNECTING, linkParams, nullptr);
    
    ExcAssertEqual(res->getLocalAddress(), sourcePath);
    ExcAssertEqual(res->getRemoteAddress(), targetPath2);

    auto onResponse = [=] (const PeerMessage & msg,
                           std::vector<std::string> && payload)
        {
            //cerr << "got response to acceptLink message" << endl;
            //cerr << "payload.size() = " << payload.size() << endl;
            //cerr << "payload[0] = " << payload[0] << endl;

            ExcAssertEqual(payload.size(), 2);
            string error = payload[0];
            Any ev = Any::jsonDecodeStrTyped(payload[1]);

            // Successful connection.  Update the state
            if (error.empty()) {
                entry->token->setAcceptParams(ev);
                entry->token->updateState(LS_CONNECTED);
                return;
            }

            // Error creating the link.  Pass on the error...
            if (error != "Already deleted") {
                cerr << "error creating link: " << error << endl;
                entry->token->updateState(LS_ERROR);
            }

#if 0                
            // ... and remove it to make sure the other end gets
            // notified.
            std::unique_lock<std::mutex> guard(remoteWatchMutex);
            localLinks.erase(externalLinkId);
#endif
        };

    auto onError = [=] (const PeerMessage & msg)
        {
            cerr << "error setting remote link" << endl;
            cerr << msg.error << endl;

#if 0
            data->links.error(LINK_ERR_CREATE, msg.error);
#endif
            std::unique_lock<std::mutex> guard(remoteWatchMutex);
            localLinks.erase(externalLinkId);
        };

    //cerr << "111111111111 link sent response" << endl;
    sendMessage(PRI_NORMAL, Date::now().plusSeconds(1),
                1, LINKCREATE, std::move(msg),
                onResponse, onError);


    localLinks[externalLinkId] = entry;

    auto onStateChanged = [=] (LinkState newState)
        {
            ExcAssertNotEqual(newState, LS_CONNECTING);

            // We don't need to send back that we're connected; it's implicit
            if (newState == LS_CONNECTED)
                return;

            //cerr << "Link " << externalLinkId << " on peer "
            //<< peerName << " state changed to " << newState << endl;
            std::vector<std::string> msg;
            msg.emplace_back(jsonEncodeStr(externalLinkId));
            msg.emplace_back(jsonEncodeStr(newState));

            sendMessage(PRI_NORMAL, Date::now().plusSeconds(1),
                        1, LINKSTATEOURS, std::move(msg),
                        nullptr, nullptr);
        };

    //cerr << "22222222222 link binding state changes" << endl;

    // We don't need to catch up.  We're in CONNECTING state.
    entry->stateWatch = entry->token->onState(false /* catchUp */);
    entry->stateWatch.bind(onStateChanged);

    //cerr << "33333333333 local end of link is set up" << endl;

    auto onMessage = [=] (const Any & payload)
        {
            //cerr << "Link received message to pass through" << endl;

            std::vector<std::string> msg;
            msg.emplace_back(jsonEncodeStr(externalLinkId));
            msg.emplace_back(Any::jsonEncodeStrTyped(payload));

            sendMessage(PRI_NORMAL, Date::now().plusSeconds(1),
                        1, LINKOUTBOUND, std::move(msg),
                        nullptr, nullptr);
        };

    entry->dataWatch = entry->token->onRecv();
    entry->dataWatch.bind(onMessage);

    return res;
}

void
RemotePeer::
handleLayerOneMessage(PeerMessage & message)
{
    MessageHeader header = message.header;

    LayerOneMessageType type
        = static_cast<LayerOneMessageType>(header.messageType);

    switch (type) {
    case PING: {
        // Checking we're still connected
        handleRemotePing(message.payload);
        return;
    }
        
    case WATCH: {
        // The other peer wants a remote watch on our service
        handleRemoteCreateWatch(message.payload);
        return;
    }
    case WATCHRELEASED: {
        // The other peer wants to cancel a remote watch on our service
        handleRemoteReleaseWatch(message.payload);
        return;
    }
    case WATCHFIRED: {
        // One of our watches on the remote peer fired
        handleRemoteWatchFired(message.payload);
        return;
    }
    case WATCHSTATUS: {
        // One of our watches on the remote peer was deleted
        handleRemoteWatchStatus(message.payload);
        return;
    }

    case LINKCREATE: {
        // We want to create a new link
        handleRemoteCreateLink(message.payload);
        return;
    }
    case LINKSTATEYOURS: {
        handleRemoteLinkStateYours(message.payload);
        return;
    }
    case LINKSTATEOURS: {
        handleRemoteLinkStateOurs(message.payload);
        return;
    }

    case LINKOUTBOUND: {
        handleRemoteLinkOutbound(message.payload);
        return;
    }
    case LINKINBOUND:  {
        handleRemoteLinkInbound(message.payload);
        return;
    }
    }
    
    //throw MLDB::Exception("got unknown layer 0 message type %d", type);
    // For forward compatibility, we simply won't respond to those
    cerr << "got unknown message type " << type << endl;
}

struct RemoteWatchInfo {
    std::string peer;
    Any info;
};

DECLARE_STRUCTURE_DESCRIPTION(RemoteWatchInfo);
DEFINE_STRUCTURE_DESCRIPTION(RemoteWatchInfo);

RemoteWatchInfoDescription::
RemoteWatchInfoDescription()
{
    addField("peer", &RemoteWatchInfo::peer, "Remote peer who is watching");
    addField("info", &RemoteWatchInfo::info, "Info from remote peer");
}

void
RemotePeer::
handleRemotePing(std::vector<std::string> & message)
{
    message.clear();
    message.push_back(jsonEncodeStr(Date::now()));
    message.push_back("RESPONSEPING-" + remotePeerInfo.peerName + "-"
                      + owner->peerInfo.peerName);
}

void
RemotePeer::
handleRemoteCreateWatch(std::vector<std::string> & message)
{
    ExcAssertEqual(message.size(), 4);

    auto spec = jsonDecodeStr<ResourceSpec>(message[0]);
    bool catchUp = jsonDecodeStr<bool>(message[1]);
    int64_t externalWatchId = jsonDecodeStr<int64_t>(message[2]);

    Any info = Any::jsonDecodeStrTyped(message[3]);

    //cerr << owner->peerInfo.peerName << " " << remotePeerInfo.peerName
    //     << " handleRemoteCreateWatch "
    //     << externalWatchId << " " << message[4] << endl;

    //cerr << "getting local watch with " << spec << ", " << catchUp
    //     << endl;

    auto bt = this->getWatchBoundType(spec).first;

    // Get the local watch
    WatchT<std::vector<std::string>, Any> watch;

    watch = getParent()->watchWithPath(spec, catchUp,
                                       RemoteWatchInfo{remotePeerInfo.peerName, info},
                                       {});
    
    //auto bt = watch.boundType();

    string error;

    // Find the types it returns
    auto onWatchFire = [=] (const Any & ev)
        {
            //cerr << "onWatchFire " << jsonEncode(ev) << endl;
            this->sendPeerWatchFired(externalWatchId, ev);
        };

    {
        std::unique_lock<std::mutex> guard(remoteWatchMutex);
        //cerr << "watch.any() = " << watch.any() << endl;

        if (remoteWatches.count(externalWatchId)) {
            // Already deleted
            //cerr << "remote watch " << externalWatchId
            //     << " was destroyed before being created"
            //     << endl;
            remoteWatches.erase(externalWatchId);
            error = "Already deleted";
        }
        else {
            auto & entry = remoteWatches[externalWatchId];
            entry = std::move(watch);

            //cerr << "remoteWatches.size() = " << remoteWatches.size()
            //<< endl;


            //cerr << "entry.any() = " << entry.any() << endl;

            //Any res = entry.waitGeneric(0.1);
            //cerr << "watch got " << jsonEncode(res) << endl;


            entry.bindGeneric(onWatchFire);

            ExcAssert(!entry.any());

            //cerr << "entry.any() = " << entry.any() << endl;
        }
    }

    message.resize(0);
    message.emplace_back(error);
    message.emplace_back(bt->name());  // bound type
}

void
RemotePeer::
handleRemoteReleaseWatch(std::vector<std::string> & message)
{
    //cerr << owner->peerInfo.peerName << " " << remotePeerInfo.peerName
    //     << " handleRemoteReleaseWatch " << message[1] << endl;

    ExcAssertEqual(message.size(), 1);
    int64_t externalWatchId = jsonDecodeStr<int64_t>(message[0]);
    this->deletePeerWatch(externalWatchId);

    message.clear();
}

void
RemotePeer::
handleRemoteWatchFired(std::vector<std::string> & message)
{
    //cerr << owner->peerInfo.peerName << " " << remotePeerInfo.peerName
    //     << " handleRemoteWatchFired " << message[1] << " "
    //     << message[2] << endl;

    ExcAssertEqual(message.size(), 2);

    int64_t externalWatchId = jsonDecodeStr<int64_t>(message[0]);

    std::vector<std::string> path;
    Any ev;

    std::tie(path, ev)
        = Any::jsonDecodeStrTyped(message[1])
        .as<std::tuple<std::vector<std::string>, Any> >();

    string extra[2] = { "peers", remotePeerInfo.peerName };

    path.insert(path.begin(), extra, extra + 2);

    message.resize(0);

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    auto it = localWatches.find(externalWatchId);
    if (it == localWatches.end()) {
        // Watch not found
        message.emplace_back(jsonEncodeStr(false));  // peer not found
        guard.unlock();
        return;
    }

    auto w = it->second;
    guard.unlock();

    // Trigger it
    w->watches.triggerGeneric(std::tuple<std::vector<std::string>, Any>(path, ev));

    message.emplace_back(jsonEncodeStr(true));  // peer found
}

void
RemotePeer::
deletePeerWatch(int64_t externalWatchId)
{
    std::unique_lock<std::mutex> guard(remoteWatchMutex);
    
    if (!remoteWatches.erase(externalWatchId)) {
        // Put in an empty watch to signify that it's already been
        // deleted.
        remoteWatches[externalWatchId] = Watch();
    }
}

void
RemotePeer::
handleRemoteWatchStatus(std::vector<std::string> & message)
{
    throw MLDB::Exception("handleRemoteWatchStatus");
}

void
RemotePeer::
sendPeerWatchFired(int64_t externalWatchId,
                   const Any & ev)
{
    //cerr << "peer watch fired with " << jsonEncode(ev) << endl;

    std::vector<std::string> message;
    message.push_back(jsonEncodeStr(externalWatchId));
    message.push_back(jsonEncodeStr(ev));

    auto onError = [=] (const PeerMessage & msg)
        {
            // Delete the watch, since we can no longer guarantee what the
            // state was.

            // ...

            cerr << "peer watch was deleted as its message with id "
                 << msg.messageId
                 << jsonEncodeStr(ev) << " didn't get through"
                 << endl;
            
            this->deletePeerWatch(externalWatchId);

            //throw MLDB::Exception("peer watch fired error not done yet");
        };

    sendMessage(PRI_NORMAL,
                Date::now().plusSeconds(1),
                1,
                WATCHFIRED,
                std::move(message),
                nullptr,
                onError);
}

void
RemotePeer::
handleRemoteCreateLink(std::vector<std::string> & message)
{
    string error;
    Any linkOutput;

    try {
        ExcAssertEqual(message.size(), 5);

        auto externalLinkId = jsonDecodeStr<uint64_t>(message[0]);
        auto sourcePath = jsonDecodeStr<std::vector<Utf8String> >(message[1]);
        auto targetPath = jsonDecodeStr<std::vector<Utf8String> >(message[2]);
        string linkType = jsonDecodeStr<std::string>(message[3]);
        Any linkParams = Any::jsonDecodeStrTyped(message[4]);

        //cerr << "4444444444 handleRemoteCreateLink" << endl;
        //cerr << "**** handleRemoteCreateLink " << remotePeerInfo.peerName << " "
        //     << externalLinkId << " " << sourcePath << " " << targetPath
        //     << " " << linkType << " " << jsonEncodeStr(linkParams) << endl;

        vector<Utf8String> fullSourcePath = { "peers", remotePeerInfo.peerName };
        fullSourcePath.insert(fullSourcePath.end(),
                              sourcePath.begin(), sourcePath.end());
        
        auto link = getParent()->acceptLink(fullSourcePath, targetPath, linkType,
                                            linkParams);
        
        if (link) {
            std::unique_lock<std::mutex> guard(remoteWatchMutex);

            linkOutput = link->acceptParams();

            //cerr << "watch.any() = " << watch.any() << endl;
            
            if (remoteLinks.count(externalLinkId)) {
                remoteLinks.erase(externalLinkId);
                error = "Already deleted";
            }
            else {
                auto & entry = remoteLinks[externalLinkId];
                entry.reset(new RemotePeer::RemoteLink);
                //linkOutput = link->remoteArgs;
                entry->token = link;
                // TODO: make it do something

                // Called when we get a message over the link to send back
                // to the peer.
                auto onRecv = [=] (const Any & ev)
                    {
                        //cerr << "sending watch event " << jsonEncode(ev)
                        //<< endl;

                        std::vector<std::string> message;
                        message.push_back(jsonEncodeStr(externalLinkId));
                        message.push_back(jsonEncodeStr(ev));

                        auto onError = [=] (const PeerMessage & msg)
                        {
                            // Delete the watch, since we can no longer guarantee what the
                            // state was.

                            // ...

                            cerr << "peer link was deleted as its message "
                                 << jsonEncodeStr(ev) << " didn't get through"
                                 << endl;
                            
                            //this->deletePeerWatch(peer, externalWatchId);

                            //throw MLDB::Exception("peer watch fired error not done yet");
                        };

                        sendMessage(PRI_NORMAL,
                                    Date::now().plusSeconds(1),
                                    1,
                                    LINKINBOUND,
                                    std::move(message),
                                    nullptr,
                                    onError);
                    };

                entry->dataWatch = link->onRecv();
                entry->dataWatch.bind(onRecv);

                auto onState = [=] (LinkState state)
                    {
                        ExcAssertNotEqual(state, LS_CONNECTING);
                        ExcAssertNotEqual(state, LS_CONNECTED);

                        //cerr << "remote link " << externalLinkId
                        //<< " on peer " << remotePeerInfo.peerName << " state change to " << state
                        //<< endl;
                        
                        std::vector<std::string> message;
                        message.push_back(jsonEncodeStr(externalLinkId));
                        message.push_back(jsonEncodeStr(state));

                        auto onError = [=] (const PeerMessage & msg)
                        {
                            cerr << "peer link state change message  didn't get through"
                                 << endl;
                        };

                        sendMessage(PRI_NORMAL,
                                    Date::now().plusSeconds(1),
                                    1,
                                    LINKSTATEYOURS,
                                    std::move(message),
                                    nullptr,
                                    onError);
                        
                    };

                //cerr << "5555555555 handleRemoteCreateLink state watches" << endl;

                // We don't need to catch up.  It started off in the CONNECTED state
                // and only a change to ERROR or DISCONNECTED will need to be
                // propagated.
                entry->stateWatch = link->onState(false /* catchup */);
                entry->stateWatch.bind(onState);
                //cerr << "6666666666 handleRemoteCreateLink state watches" << endl;
                
            }
        }
        else error = "Link was not accepted";
    } catch (const std::exception & exc) {
        error = exc.what();
    }

    message.resize(0);
    message.emplace_back(error);
    message.emplace_back(jsonEncodeStr(linkOutput));
}

void
RemotePeer::
handleRemoteLinkStateYours(std::vector<std::string> & message)
{
    ExcAssertEqual(message.size(), 2);
    int64_t externalLinkId = jsonDecodeStr<int64_t>(message[0]);
    LinkState state = jsonDecodeStr<LinkState>(message[1]);

    message.resize(0);

    cerr << "remote link " << externalLinkId << " changed to state "
         << state << " on peer " << owner->peerInfo.peerName << endl;

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    auto it = localLinks.find(externalLinkId);
    if (it == localLinks.end()) {
        guard.unlock();
        return;
    }
   
    auto w = it->second;
    w->token->updateState(state);

    // Error or disconnect?  Move on
    if (state == LS_ERROR || state == LS_DISCONNECTED)
        localLinks.erase(it);
    
    guard.unlock();

    message.push_back(jsonEncodeStr(true));
}

void
RemotePeer::
handleRemoteLinkStateOurs(std::vector<std::string> & message)
{
    ExcAssertEqual(message.size(), 2);
    int64_t externalLinkId = jsonDecodeStr<int64_t>(message[0]);
    LinkState state = jsonDecodeStr<LinkState>(message[1]);

    message.resize(0);

    cerr << "our link " << externalLinkId << " changed to state "
         << state << " on peer " << owner->peerInfo.peerName << endl;

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    auto it = remoteLinks.find(externalLinkId);
    if (it == remoteLinks.end()) {
        guard.unlock();
        return;
    }
   
    auto w = it->second;
    w->token->updateState(state);

    // Error or disconnect?  Move on
    if (state == LS_ERROR || state == LS_DISCONNECTED)
        remoteLinks.erase(it);
    
    guard.unlock();
}

void
RemotePeer::
handleRemoteLinkInbound(std::vector<std::string> & message)
{
    ExcAssertEqual(message.size(), 2);

    auto externalLinkId = jsonDecodeStr<uint64_t>(message[0]);
    Any event = Any::jsonDecodeStrTyped(message[1]);

    //cerr << "got inbound link event " << jsonEncode(event) << endl;

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    auto it = localLinks.find(externalLinkId);
    if (it == localLinks.end()) {
        // Link not found
        message.emplace_back(jsonEncodeStr(false));  // peer not found
        guard.unlock();
        return;
    }

   
    auto w = it->second;
    guard.unlock();

    //cerr << "accepted inbound link event " << jsonEncode(event) << endl;
    w->token->send(event);

    message.emplace_back(jsonEncodeStr(true));  // peer found
}

void
RemotePeer::
handleRemoteLinkOutbound(std::vector<std::string> & message)
{
    ExcAssertEqual(message.size(), 2);

    auto externalLinkId = jsonDecodeStr<uint64_t>(message[0]);
    Any event = Any::jsonDecodeStrTyped(message[1]);

    //cerr << "got inbound link event " << jsonEncode(event) << endl;

    std::unique_lock<std::mutex> guard(remoteWatchMutex);

    auto it = remoteLinks.find(externalLinkId);
    if (it == remoteLinks.end()) {
        // Link not found
        message.emplace_back(jsonEncodeStr(false));  // peer not found
        guard.unlock();
        return;
    }

   
    auto w = it->second;
    guard.unlock();

    //cerr << "accepted inbound link event " << jsonEncode(event) << endl;
    w->token->send(event);

    message.emplace_back(jsonEncodeStr(true));  // peer found

}

void
RemotePeer::
startWriting()
{
    ExcAssert(connection);
    connection->startWriting(std::bind(&RemotePeer::getMessage,
                                       this,
                                       std::placeholders::_1));
}


/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION_NAMED(ServicePeerStateDescription,
                              PeerState);

DEFINE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerPingStatsDescription,
                                   PingStats);

DEFINE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerPeerStatsDescription,
                                   PeerStats);

DEFINE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerPeerStatusDescription,
                                   PeerStatus);

DEFINE_STRUCTURE_DESCRIPTION_NAMED(ServicePeerWatchStatusDescription,
                                   WatchStatus);

DEFINE_STRUCTURE_DESCRIPTION(LinkStatus);


ServicePeerStateDescription::
ServicePeerStateDescription()
{
    addValue("UNKNOWN", PS_UNKNOWN);
    addValue("CONNECTING", PS_CONNECTING);
    addValue("OK", PS_OK);
    addValue("ERROR", PS_ERROR);
}

std::ostream & operator << (std::ostream & stream, PeerState s)
{
    return stream << jsonEncode(s).asString();
}

ServicePeerPingStatsDescription::
ServicePeerPingStatsDescription()
{
    addField("latestTimeMs", &PingStats::latestTimeMs,
             "Latest ping time");
    addField("latestReceived", &PingStats::latestReceived,
             "Latest time received");
}

ServicePeerPeerStatsDescription::
ServicePeerPeerStatsDescription()
{
    addField("ping", &PeerStats::ping,
             "Stats for ping 0");
}

ServicePeerPeerStatusDescription::
ServicePeerPeerStatusDescription()
{
    addField("peerName", &PeerStatus::peerName,
             "Name of peer");
    addField("peerInfo", &PeerStatus::peerInfo,
             "Information about peer");
    addField("state", &PeerStatus::state,
             "State of peer");
    addField("lastMessageReceived", &PeerStatus::lastMessageReceived,
             "Timestamp of last received message");
    addField("timeSinceLastMessageReceivedMs",
             &PeerStatus::timeSinceLastMessageReceivedMs,
             "Time in milliseconds since last message received");
    addField("error", &PeerStatus::error,
             "Error message for when in ERROR state");
    addField("stats", &PeerStatus::stats,
             "Statistics");
    addField("messagesSent", &PeerStatus::messagesSent,
             "Number of messages sent");
    addField("messagesReceived", &PeerStatus::messagesReceived,
             "Number of messages received");
    addField("messagesReceivedAfterDeadline",
             &PeerStatus::messagesReceivedAfterDeadline,
             "Number of messages received after their deadline had expired");
    addField("messagesQueued0", &PeerStatus::messagesQueued0,
             "Number of messages in queue 0");
    addField("messagesQueued1", &PeerStatus::messagesQueued1,
             "Number of messages in queue 1");
    addField("responsesSent", &PeerStatus::responsesSent,
             "Number of message responses sent");
    addField("messagesAwaitingResponse",
             &PeerStatus::messagesAwaitingResponse,
             "Number of messages waiting for a response");
    addField("messagesWithDeadline",
             &PeerStatus::messagesWithDeadline,
             "Number of messages with a deadline");
    addField("messagesTimedOut",
             &PeerStatus::messagesTimedOut,
             "Number of messages timed out");
    addField("connection", &PeerStatus::connection,
             "Status of the peer connection");
}

ServicePeerWatchStatusDescription::
ServicePeerWatchStatusDescription()
{
    addField("watchId", &WatchStatus::watchId,
             "Id of the watch in the owning peer's space");
    addField("info", &WatchStatus::info,
             "Information added when creating the watch");
    addField("attached", &WatchStatus::attached,
             "Is the watch attached?");
    addField("spec", &WatchStatus::spec,
             "Specification with which the watch was created");
    addField("triggers", &WatchStatus::triggers,
             "Number of times that the watch was triggered");
    addField("errors", &WatchStatus::errors,
             "Number of times that the watch had an error");
}

LinkStatusDescription::
LinkStatusDescription()
{
    addField("linkId", &LinkStatus::linkId,
             "Id of the link in the owning peer's space");
    addField("state", &LinkStatus::state,
             "State of link");
}


} // namespace MLDB
