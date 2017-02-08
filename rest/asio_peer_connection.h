/* asio_peer_connection.h                                          -*- C++ -*-
   Jeremy Barnes, 1 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "peer_connection.h"
#include <boost/asio.hpp>
#include <atomic>

namespace MLDB {


/*****************************************************************************/
/* ASIO PEER CONNECTION                                                      */
/*****************************************************************************/

struct AsioPeerConnection: public PeerConnection {

    AsioPeerConnection(std::shared_ptr<boost::asio::ip::tcp::socket> sock);
    ~AsioPeerConnection();

    virtual PeerConnectionStatus getStatus() const;

    virtual void shutdown();
    virtual void send(std::string && data);

    virtual void startReading(std::function<bool (std::string && data)> onRecv);
    virtual void stopReading();

    virtual void startWriting(std::function<bool (std::string & data)> onSend);
    virtual void stopWriting();

    /** Return a timer that triggers at the given expiry and optionally
        resets to fire periodically.
    */
    virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                  std::function<void (Date)> toBind = nullptr);

    virtual void postWorkSync(std::function<void ()> work);
    virtual void postWorkAsync(std::function<void ()> work);

private:

    struct Itl {

        Itl(std::shared_ptr<boost::asio::ip::tcp::socket> sock,
            AsioPeerConnection * connection);


        PeerConnectionStatus getStatus() const;

        std::atomic<bool> shutdown_;

        std::shared_ptr<boost::asio::ip::tcp::socket> sock;
        boost::asio::strand strand;
        AsioPeerConnection * connection;
        PeerConnectionState currentState;
        std::string currentError;
        std::function<bool (std::string & data)> onSend;

        // This mutex only protects changes in internal object state.  The
        // asio strand protects the actual reading and writing.
        mutable std::recursive_mutex mutex;  // TODO: should be able to get rid of it

        uint64_t currentPacketLength;
        std::string currentPacket;
        std::unique_ptr<std::string> bufferedRead;

        std::atomic<bool> currentlyReading;
        std::atomic<bool> currentlyWriting;
    };

    std::shared_ptr<Itl> itl;

    static void doStartReading(std::shared_ptr<Itl> itl);
    static void onReadLengthDone(boost::system::error_code error,
                                 size_t bytesDone,
                                 std::shared_ptr<Itl> itl);
    static void onReadDataDone(boost::system::error_code error,
                               size_t bytesDone,
                               std::shared_ptr<Itl> itl);
    static void notifyError(boost::system::error_code error,
                            std::shared_ptr<Itl> itl);
    static void doStartWriting(std::shared_ptr<Itl> itl);
    static void onWriteDone(boost::system::error_code err,
                            size_t bytesDone,
                            std::shared_ptr<Itl> itl);
    void setState(PeerConnectionState newState);

};




} // namespace MLDB

