// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_message.h                                                 -*- C++ -*-
    Jeremy Barnes, 30 May 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Message from one peer to another.
*/

#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>
#include "mldb/types/date.h"


namespace MLDB {

/** Controls what priority the message gets.  This allows different
    tradeoffs between latency and throughput to be selected and for
    some basic QOS to be enforced.

    Note that for a given priority, messages will be sent in order,
    but there is no guarantee of ordering between priority bands.
*/
enum MessagePriority {
    PRI_IMMEDIATE,  ///< Optimized for latency (not queued)
    PRI_HIGH,       ///< Optimized for throughput, high priority
    PRI_NORMAL      ///< Optimized for throughput, normal priority
};

enum MessageDirection {
    DIR_REQUEST,    ///< Message is a new request
    DIR_RESPONSE    ///< Message is a response to an existing request
};

/** Binary structure that goes on the header of messages send back
    and forth.
*/
struct MessageHeader {
    int32_t version;    ///< Version of the protocol
    int layer;          ///< Layer this message is for (0, 1 or 2)
    Date ts;            ///< Timestamp message was enqueued on host
    Date deadline;      ///< When the message must be done by
    int64_t id;         ///< Host's ID of message
    int64_t size;       ///< Message payload length
    int messageType;    ///< Type of message
    int numParts;       ///< Number of message parts
    MessageDirection dir;///< Direction of message
};

/** Encoded version of a message. */
struct EncodedMessage : public MessageHeader {
    char payload[0];

    const char * begin() const
    {
        return (const char *)this;
    }

    const char * end() const
    {
        return begin() + size;
    }

    struct Deleter {
        void operator () (void * mem)
        {
            delete[] (char *)mem;
        }
    };
};

struct PeerDatagram : public MessageHeader {
    std::vector<std::string> payload;
};

struct PeerMessage;

typedef std::function<void (PeerMessage && message,
                            std::vector<std::string> &&)> OnResponse;
typedef std::function<void (PeerMessage && msg) > OnError;

/** Structure holding a message from a peer. */
struct PeerMessage {
    enum State {
        NONE,
        QUEUED,
        SENT,
        RECEIVED,
        RESPONDED,
        TIMEOUT_SEND,
        TIMEOUT_RESPONSE,
        SEND_ERROR
    };

    MessageHeader header;

    MessageDirection direction;
    int layer;
    int type;
    int64_t messageId;
    MessagePriority priority;
    Date timeEnqueued;
    Date timeSent;
    Date deadline;
    State state;
    std::string error;
    boost::system::error_code errorCode;
    std::vector<std::string> payload;
        
    OnResponse onResponse;
    OnError onError;

    std::unique_ptr<EncodedMessage, EncodedMessage::Deleter>
    encode() const;

    // Synchronously receive it from the socket
    static PeerMessage recv(boost::asio::ip::tcp::socket & socket);

    static PeerMessage decode(const std::string & str);

    // Synchronously receive the entire message from the socket
    void send(boost::asio::ip::tcp::socket & socket,
              MessageDirection dir);
};

} // namespace MLDB
