// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** peer_message.cc
    Jeremy Barnes, 30 May 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "peer_message.h"

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* PEER MESSAGE                                                              */
/*****************************************************************************/

std::unique_ptr<EncodedMessage, EncodedMessage::Deleter>
PeerMessage::
encode() const
{
    size_t totalSize
        = sizeof(MessageHeader)
        + payload.size() * 4;   // Size headers per chunk
    
    for (auto & m: payload) {
        totalSize += m.size();
    }

    // Raw memory goes here
    char * mem = new char[totalSize];

    std::unique_ptr<EncodedMessage, EncodedMessage::Deleter>
        msg(new (mem) EncodedMessage());

    MessageHeader & header = *msg;
    header.version = 1;
    header.ts = timeSent;
    header.id = messageId;
    header.layer = layer;
    header.messageType = type;
    header.deadline = deadline;
    header.dir = direction;
    header.numParts = payload.size();
    header.size = totalSize;

    char * p = msg->payload;

    //cerr << "writing " << message.size() << " chunks at " << p - mem << endl;

    //int i = 0;
    for (auto & m: payload) {
        //cerr << "chunk " << i++ << " at " << (p - mem) << endl;

        // 4 bytes of part size
        uint32_t * p2 = (uint32_t *)p;
        *p2 = m.size();
        p += 4;

        // payload of part
        std::copy(m.data(), m.data() + m.size(), p);
        p += m.size();
    }

    //ExcAssertEqual(p - mem, totalSize);

    return msg;
}

PeerMessage
PeerMessage::
decode(const std::string & msg)
{
    PeerMessage result;

    const char * p = msg.c_str();

    // First thing we do is get the handshake out
    MessageHeader & header = result.header;
    header = *(const MessageHeader *)p;

    result.layer = header.layer;
    result.type = header.messageType;
    result.messageId = header.id;
    result.timeSent = header.ts;
    result.deadline = header.deadline;
    result.header = header;
    result.direction = header.dir;

    p += sizeof(header) - 4;

    for (unsigned i = 0;  i < header.numParts;  ++i) {
        const uint32_t * sz = (const uint32_t *)p;
        uint32_t partSize = *sz;
        p += 4;

        //cerr << "part " << i << " is size " << partSize << endl;

        result.payload.push_back(string(p, p + partSize));
        
        p += partSize;
    } 

    return result;
}

} // namespace MLDB
