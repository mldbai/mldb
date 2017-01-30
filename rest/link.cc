// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** link.cc
    Jeremy Barnes, 14 May 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "link.h"
#include "call_me_back.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/types/enum_description.h"
#include "mldb/watch/watch_impl.h"


using namespace std;


namespace MLDB {

DEFINE_ENUM_DESCRIPTION(LinkState);

LinkStateDescription::
LinkStateDescription()
{
    addValue("DISCONNECTED", LS_DISCONNECTED, "Link is disconnected");
    addValue("CONNECTING",   LS_CONNECTING,   "Link is connecting");
    addValue("CONNECTED",    LS_CONNECTED,    "Link is connected");
    addValue("ERROR",        LS_ERROR,        "Link had an error");
}

DEFINE_ENUM_DESCRIPTION(LinkEnd);

LinkEndDescription::
LinkEndDescription()
{
    addValue("CONNECT",  LE_CONNECT,  "This is the active (connect) end");
    addValue("ACCEPT",   LE_ACCEPT,   "This is the passive (accept) end");
}

std::ostream & operator << (std::ostream & stream, LinkEnd end)
{
    return stream << jsonEncodeStr(end);
}

std::ostream & operator << (std::ostream & stream, LinkState state)
{
    return stream << jsonEncodeStr(state);
}

/*****************************************************************************/
/* LINK DATA                                                                */
/*****************************************************************************/

void
LinkData::
setState(LinkState newState)
{
    std::unique_lock<std::mutex> guard(mutex);
    if (state == newState)
        return;
    state = newState;
    stateWatches.trigger(newState);
}

void
LinkData::
waitUntilState(double waitTime, LinkState stateRequired)
{
    if (state == stateRequired)
        return;

    Date limit = Date::now().plusSeconds(waitTime);
    auto w = onState(true /* catchup */);
    for (;;) {
        if (state == stateRequired)
            return;
        Date now = Date::now();
        if (now > limit)
            throw MLDB::Exception("timed out waiting for link connection");

        LinkState state;
        bool found;

        std::tie(found, state) = w.tryWait(Date::now().secondsUntil(limit));

        if (found) {
            if (state == stateRequired)
                return;
            if (state == LS_ERROR)
                throw MLDB::Exception("link in error state");
        }
    }
}

void
LinkData::
waitUntilConnected(double waitTime)
{
    waitUntilState(waitTime, LS_CONNECTED);
}

void
LinkData::
waitUntilDisconnected(double waitTime)
{
    waitUntilState(waitTime, LS_DISCONNECTED);
}

WatchT<LinkState>
LinkData::
onState(bool catchUp)
{
    std::unique_lock<std::mutex> guard(mutex);
    auto w = stateWatches.add();
    if (catchUp)
        w.trigger(state);
    return w;
}


/*****************************************************************************/
/* LINK TOKEN                                                                */
/*****************************************************************************/

LinkToken::
LinkToken()
{
}

LinkState
LinkToken::
getState() const
{
    return data->state;
}

void
LinkToken::
send(Any val)
{
    std::unique_lock<std::mutex> guard(data->mutex);
    sendData->trigger(val);
}

WatchT<Any>
LinkToken::
onRecv()
{
    std::unique_lock<std::mutex> guard(data->mutex);
    return dataWatches.add();
}

WatchT<LinkState>
LinkToken::
onState(bool catchUp)
{
    return data->onState(catchUp);
}

void
LinkToken::
waitUntilConnected(double waitTime)
{
    data->waitUntilConnected(waitTime);
}

void
LinkToken::
waitUntilDisconnected(double waitTime)
{
    data->waitUntilDisconnected(waitTime);
}

void
LinkToken::
updateState(LinkState newState)
{
    data->setState(newState);
}

const Any &
LinkToken::
connectParams() const
{
    return data->connectParams;
}

const Any &
LinkToken::
acceptParams() const
{
    std::unique_lock<std::mutex> guard(data->mutex);
    return data->acceptParams;
}

void
LinkToken::
setAcceptParams(Any newParams) const
{
    std::unique_lock<std::mutex> guard(data->mutex);
    data->acceptParams = newParams;
}

int64_t
LinkToken::
messagesSent() const
{
    if (!sendData)
        return 0;
    return sendData->getTriggerCount();
}

int64_t
LinkToken::
messagesReceived() const
{
    return dataWatches.getTriggerCount();
}

std::shared_ptr<const LinkToken>
LinkToken::
otherEnd() const
{
    if (end_ == LE_ACCEPT)
        return data->end1.lock();
    else return data->end2.lock();
}


/*****************************************************************************/
/* CREATE LINK                                                               */
/*****************************************************************************/

std::pair<std::shared_ptr<LinkToken>,
          std::shared_ptr<LinkToken> >
createLink(std::unique_ptr<LinkToken> && tok1,
           std::unique_ptr<LinkToken> && tok2,
           LinkState initialState,
           Any connectParams,
           Any acceptParams,
           Any connectAddr,
           Any acceptAddr)
{
    auto data = std::make_shared<LinkData>();
    data->state = initialState;
    tok1->data = tok2->data = data;
    data->connectParams = std::move(connectParams);
    data->acceptParams = std::move(acceptParams);
    data->connectAddr = std::move(connectAddr);
    data->acceptAddr = std::move(acceptAddr);

    // Link the send and receive channels together
    tok1->sendData = &tok2->dataWatches;
    tok2->sendData = &tok1->dataWatches;

    tok1->end_ = LE_CONNECT;
    tok2->end_ = LE_ACCEPT;

    auto onDelete1 = [=] (LinkToken * token)
        {
            //cerr << "deleting end 1" << endl;
            token->data->setState(LS_DISCONNECTED);
            delete token;
        };

    auto onDelete2 = [=] (LinkToken * token)
        {
            //cerr << "deleting end 2" << endl;
            token->data->setState(LS_DISCONNECTED);
            delete token;
        };
    
    std::shared_ptr<LinkToken> res1(tok1.release(), onDelete1);
    std::shared_ptr<LinkToken> res2(tok2.release(), onDelete2);

    data->end1 = res1;
    data->end2 = res2;

    return { res1, res2 };
}


/*****************************************************************************/
/* LINKS                                                                     */
/*****************************************************************************/

struct Links::Impl {
    Impl()
        : readOnlyData(readOnlyLock), subscriptionId(0), shuttingDown(false)
    {
    }
    
    Data data;
    
    GcLock readOnlyLock;
    RcuProtected<Data> readOnlyData;
    
    mutable std::mutex mutex;
    int subscriptionId;
    bool shuttingDown;
};

Links::
Links()
    : impl(new Impl())
{
}

Links::
~Links()
{
    impl->shuttingDown = true;
}

std::shared_ptr<LinkToken>
Links::
accept(Any params)
{
    std::shared_ptr<LinkToken> tok1, tok2;
    std::tie(tok1, tok2)
        = createLinkT<LinkToken>(LS_CONNECTED, params, params);
    add(tok2);
    return tok1;
}

void
Links::
add(std::shared_ptr<LinkToken> token)
{
    std::unique_lock<std::mutex> guard(impl->mutex);

    int id = impl->subscriptionId++;

    impl->data[id].reset(new Entry());

    impl->data[id]->link = std::move(token);

    auto doStateChange = [=] (LinkState state)
        {
            using namespace std;
            //cerr << "doStateChange " << state << endl;
            if (state == LS_DISCONNECTED) {
                callMeBackLater.add([=] () { this->release(id); });
            }
        };

    impl->data[id]->stateWatch = impl->data[id]->link->onState();
    impl->data[id]->stateWatch.bind(doStateChange);

    // Publish it read-only
    impl->readOnlyData.replace(new Data(impl->data));
}

void
Links::
forEach(const std::function<void (LinkToken & token)> & fn) const
{
    for (auto & s: *impl->readOnlyData())
        fn(*s.second->link);
    return;
}

size_t
Links::
size() const
{
    return impl->readOnlyData()->size();
    //return data.size();
}

void
Links::
clear()
{
    std::unique_lock<std::mutex> guard(impl->mutex);
    impl->data.clear();
    // Publish it read-only
    impl->readOnlyData.replace(new Data(impl->data));
}

void
Links::
release(int id)
{
    if (impl->shuttingDown)
        return;
    std::unique_lock<std::mutex> guard(impl->mutex);
    impl->data.erase(id);
    impl->readOnlyData.replace(new Data(impl->data));
}


} // namespace MLDB
