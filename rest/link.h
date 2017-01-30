// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* link.h                                                          -*- C++ -*-
   Jeremy Barnes, 12 May 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Data structures to deal with links between different REST collections.
   These can be used to model things like subscriptions.
*/

#pragma once

#include "mldb/watch/watch.h"

namespace MLDB {


struct LinkToken;

enum LinkState {
    LS_DISCONNECTED,
    LS_CONNECTING,
    LS_CONNECTED,
    LS_ERROR
};

DECLARE_ENUM_DESCRIPTION(LinkState);

std::ostream & operator << (std::ostream & stream, LinkState state);

enum LinkEnd {
    LE_CONNECT,   ///< The end of the link that initiated the connection
    LE_ACCEPT     ///< The end of the link that accepted the connection
};

DECLARE_ENUM_DESCRIPTION(LinkEnd);

std::ostream & operator << (std::ostream & stream, LinkEnd end);


/*****************************************************************************/
/* LINK DATA                                                                */
/*****************************************************************************/

struct LinkData {
    std::mutex mutex;
    std::weak_ptr<LinkToken> end1;
    std::weak_ptr<LinkToken> end2;
    LinkState state;
    Any connectParams, acceptParams;
    Any connectAddr, acceptAddr;

    WatchesT<LinkState> stateWatches;   ///< State changes go here

    /** Update the state, triggering watches if not the same as the previous
        state.
    */
    void setState(LinkState newState);

    /** Wait until the link state gets to LS_CONNECTED.  If it hits LS_ERROR
        then an exception will be thrown.  If the maximum wait time is exceeded
        an exception will be thrown.
    */
    void waitUntilState(double waitTime, LinkState stateRequired);

    /** Wait until the link state gets to LS_CONNECTED.  If it hits LS_ERROR
        then an exception will be thrown.  If the maximum wait time is exceeded
        an exception will be thrown.
    */
    void waitUntilConnected(double waitTime);

    void waitUntilDisconnected(double waitTime);

    WatchT<LinkState> onState(bool catchUp = true);
};


/*****************************************************************************/
/* LINK TOKEN                                                                */
/*****************************************************************************/

/** This is a token that is used to represent a link from one object to
    another.  The link is broken once the token is destroyed.
*/

struct LinkToken {
    LinkToken();

    LinkState getState() const;

    void send(Any val);

    WatchT<Any> onRecv();

    WatchT<LinkState> onState(bool catchUp = true);

    /** Wait until the link state gets to LS_CONNECTED.  If it hits LS_ERROR
        then an exception will be thrown.  If the maximum wait time is exceeded
        an exception will be thrown.
    */
    void waitUntilConnected(double waitTime = INFINITY);

    void waitUntilDisconnected(double waitTime = INFINITY);

    void updateState(LinkState newState);

    const Any & connectParams() const;

    const Any & acceptParams() const;

    void setAcceptParams(Any newParams) const;

    /** Which end of the link are we attached to? */
    LinkEnd getEnd() const
    {
        return end_;
    }

    int64_t messagesSent() const;

    int64_t messagesReceived() const;

protected:
    std::shared_ptr<const LinkToken> otherEnd() const;

private:
    friend class LinkData;

    friend 
    std::pair<std::shared_ptr<LinkToken>,
              std::shared_ptr<LinkToken> >
    createLink(std::unique_ptr<LinkToken> && tok1,
               std::unique_ptr<LinkToken> && tok2,
               LinkState initialState,
               Any connectParams,
               Any acceptParams,
               Any connectAddr,
               Any acceptAddr);

    WatchesT<Any> * sendData;             ///< Data sent to here
    std::shared_ptr<LinkData> data;       ///< Data holding link together
    WatchesT<Any> dataWatches;            ///< Data received to here
    LinkEnd end_;
};


/*****************************************************************************/
/* CREATE LINK                                                               */
/*****************************************************************************/

std::pair<std::shared_ptr<LinkToken>,
          std::shared_ptr<LinkToken> >
createLink(std::unique_ptr<LinkToken> && tok1,
           std::unique_ptr<LinkToken> && tok2,
           LinkState initialState = LS_CONNECTED,
           Any connectParams = nullptr, Any acceptParams = nullptr,
           Any connectAddr = nullptr, Any acceptAddr = nullptr);

template<typename Token>
std::pair<std::shared_ptr<Token>,
          std::shared_ptr<Token> >
createLinkT(std::unique_ptr<Token> && tok1,
            std::unique_ptr<Token> && tok2,
            LinkState initialState = LS_CONNECTED,
            Any connectParams = nullptr,
            Any acceptParams = nullptr)
{
    auto res = createLink(std::move(tok1), std::move(tok2), initialState,
                          connectParams, acceptParams);

    return { std::static_pointer_cast<Token>(std::move(res.first)),
             std::static_pointer_cast<Token>(std::move(res.second)) };
}

template<typename Token>
std::pair<std::shared_ptr<Token>,
          std::shared_ptr<Token> >
createLinkT(LinkState initialState = LS_CONNECTED,
            Any connectParams = nullptr,
            Any acceptParams = nullptr)
{
    std::unique_ptr<Token> tok1(new Token());
    std::unique_ptr<Token> tok2(new Token());

    auto res = createLink(std::move(tok1), std::move(tok2), initialState,
                          connectParams, acceptParams);

    return { std::static_pointer_cast<Token>(std::move(res.first)),
             std::static_pointer_cast<Token>(std::move(res.second)) };
}

/*****************************************************************************/
/* LINKS                                                                     */
/*****************************************************************************/

struct Links {

    Links();

    ~Links();

    struct Entry {
        std::shared_ptr<LinkToken> link;
        WatchT<LinkState> stateWatch;
    };

    typedef std::map<int, std::shared_ptr<Entry> > Data;

    std::shared_ptr<LinkToken>
    accept(Any params);

    void add(std::shared_ptr<LinkToken> token);

    void forEach(const std::function<void (LinkToken & token)> & fn) const;

    size_t size() const;

    void clear();

private:
    struct Impl;

    std::unique_ptr<Impl> impl;

    /** Release the subscription with the given ID. */
    void release(int id);
};




} // namespace MLDB
