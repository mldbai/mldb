// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** watch.cc
    Jeremy Barnes, 9 April 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Implementation of watches.
*/

#include "watch_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/map_description.h"
#include "mldb/ext/jsoncpp/json.h"
#include "watch.h"

namespace MLDB {

void crashTheProgram(const WatchError & error)
{
    using namespace std;
    cerr << "ABORT: watch failed with error " << jsonEncode(error) << endl;
    abort();
}

void crashIfNotDisconnected(const WatchError & error)
{
    if (error.kind == WATCH_DISCONNECTED)
        return;  // ignore
    crashTheProgram(error);
}

void ignoreErrors(const WatchError & error)
{
}

const WatchErrorHandler CRASH_IF_NOT_DISCONNECTED(crashIfNotDisconnected);
const WatchErrorHandler CRASH_THE_PROGRAM(crashTheProgram);
const WatchErrorHandler IGNORE_ERRORS(ignoreErrors);

/*****************************************************************************/
/* WATCH EXCEPTION                                                           */
/*****************************************************************************/

WatchException::
~WatchException() throw()
{
}

WatchException::
WatchException(WatchError && err)
    : err(std::move(err)),
      what_(jsonEncodeStr(this->err))
{
}

const char *
WatchException::
what() const throw()
{
    return what_.c_str();
}


/*****************************************************************************/
/* ALL OUTPUT                                                                */
/*****************************************************************************/


struct AllOutputDescription: public ValueDescriptionT<AllOutput> {
    virtual void parseJsonTyped(AllOutput * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const AllOutput * val,
                                JsonPrintingContext & context) const;
};

void
AllOutputDescription::
parseJsonTyped(AllOutput * val, JsonParsingContext & context) const
{
    throw MLDB::Exception("AllOutputDescription::parseJsonTyped(): not impl");
}

void
AllOutputDescription::
printJsonTyped(const AllOutput * val,
               JsonPrintingContext & context) const
{
    throw MLDB::Exception("AllOutputDescription::printJsonTyped(): not impl");
}

ValueDescriptionT<AllOutput> *
getDefaultDescription(AllOutput *)
{
    return new AllOutputDescription();
}

DEFINE_ENUM_DESCRIPTION(WatchErrorKind);

WatchErrorKindDescription::
WatchErrorKindDescription()
{
    addValue("ok", WATCH_OK, "No error");
    addValue("disconnected", WATCH_DISCONNECTED, "Watch was disconnected");
    addValue("create", WATCH_ERR_CREATE, "Error creating watch");
    addValue("type", WATCH_ERR_TYPE, "Type mismatch");

}

DEFINE_STRUCTURE_DESCRIPTION(WatchError);

WatchErrorDescription::
WatchErrorDescription()
{
    addField("kind", &WatchError::kind, "Kind of error");
    addField("message", &WatchError::message, "Error message");
    addField("sender", &WatchError::sender, "Sender of message");
    addField("details", &WatchError::details, "Error details");
}

/*****************************************************************************/
/* WATCH DATA                                                                */
/*****************************************************************************/

WatchData::
WatchData(Watches * owner,
          const std::type_info * boundType,
          Any info)
    : holder(nullptr), owner(owner), boundType_(boundType),
      info(std::move(info)), triggers(0), errors(0),
      lockInfo(new LockInfo())
{
}

WatchData::
~WatchData()
{
    if (holder)
        holder->data = nullptr;
}

void
WatchData::
setHolder(Watch & newHolder)
{
    ExcAssertEqual(newHolder.data, 0);
    ExcAssertEqual(holder, 0);

    auto guard = lock();
    newHolder.data = this;
    holder = &newHolder;
}

void
WatchData::
transferHolder(Watch && oldHolder, Watch & newHolder)
{
    auto guard = lock();
        
    ExcAssertEqual(oldHolder.data, this);

    if (newHolder.data)
        newHolder.data->releaseHolder(newHolder);
        
    newHolder.data = oldHolder.data;
    oldHolder.data = nullptr;
    holder = &newHolder;
}

void
WatchData::
releaseHolder(Watch & holder)
{
    auto guard = lock();

    ExcAssertEqual(this->holder, &holder);
    this->holder = nullptr;
    holder.data = nullptr;
    if (this->owner) {
        // Release the mutex, since this object will be destroyed
        // by the owner and we don't want our guard to read the
        // freed mutex after we're done.
        guard.unlock();
        this->owner->releaseWatchWithData(this);
    }
}

void
WatchData::
releaseHolderForDestroy(Watch & holder)
{
    releaseHolder(holder);
}

Any
WatchData::
waitGeneric(double timeToWait)
{
    bool found;
    Any res;
        
    std::tie(found, res) = tryWaitGeneric(timeToWait);

    if (!found)
        throw MLDB::Exception("No event found before timeout");
        
    return res;
}

std::shared_ptr<const ValueDescription>
WatchData::
boundValueDescription() const
{
    auto guard = lock();
    ExcAssert(owner);
    return owner->boundValueDescription();
}

void
WatchData::
waitForInProgressEvents() const
{
    // Just grab and release the lock
    while (any()) ;
}


/*****************************************************************************/
/* UNKNOWN TYPE WATCH DATA                                                   */
/*****************************************************************************/

UnknownTypeWatchData::
UnknownTypeWatchData(Watches * owner,
                     std::function<bool (const Any &)> filterFn,
                     Any info)
    : WatchData(owner, owner->boundType(), std::move(info)),
      numSaved(0),
      filterFn(std::move(filterFn))
{
}
    
void
UnknownTypeWatchData::
triggerGeneric(const Any & vals)
{
    auto guard = this->lock();
    triggerGenericLocked(vals);
}

void
UnknownTypeWatchData::
triggerGenericLocked(const Any & vals)
{
    ++this->triggers;

    if (filterFn && !filterFn(vals))
        return;

    if (boundFn && saved.empty())
        boundFn(vals);
    else {
        saved.emplace_back(vals);
        ++numSaved;
        ML::futex_wake(numSaved);
    }
}

void
UnknownTypeWatchData::
error(const WatchError & error)
{
    auto guard = this->lock();
    errorLocked(error);
}

void
UnknownTypeWatchData::
errorLocked(const WatchError & error)
{
    ++this->errors;
    
    if (errorFn) {
        ExcAssert(saved.empty());
        ExcAssertEqual(numSaved, 0);
        errorFn(error);
    }
    else {
        saved.emplace_back(error);
        ++numSaved;
        ML::futex_wake(numSaved);
    }
}

std::pair<bool, Any>
UnknownTypeWatchData::
tryWaitGeneric(double timeToWait)
{
    auto res = tryWaitMaybeGeneric(timeToWait);

    if (res.isNull()) // timeout
        return std::make_pair(false, Any());
    else if (!res) // error
        throw WatchException(std::move(res.err()));
    else
        return std::make_pair(true, std::move(res.val()));
}

MaybeT<Any, WatchError>
UnknownTypeWatchData::
tryWaitMaybeGeneric(double timeToWait)
{
    if (boundFn)
        throw MLDB::Exception("cannot wait on a bound watch");

    Date limit = Date::now().plusSeconds(timeToWait);
        
    for (;;) {
        double secondsToWait
            = std::max(0.0, Date::now().secondsUntil(limit));

        if (secondsToWait > 0) {
            ML::futex_wait(numSaved, 0, secondsToWait);
            if (!numSaved)
                continue;
        }
        else {
            if (!numSaved)
                return MaybeT<Any, WatchError>();
        }

        auto guard = this->lock();
            
        if (numSaved == 0) {
            if (secondsToWait > 0.0)
                continue;
            else
                return MaybeT<Any, WatchError>();
        }

        ExcAssert(!saved.empty());
            
        auto el = std::move(saved.front());
        saved.pop_front();
        --numSaved;

        return el;
    }
}

void
UnknownTypeWatchData::
bindGeneric(const std::function<void (const Any &) > & fn,
            WatchErrorHandler errorFn)
{
    auto guard = this->lock();

    // TODO: use errorFn

    ExcAssert(!boundFn);
    this->boundFn = fn;
    this->errorFn = errorFn;

    for (auto & s: saved) {
        if (s)
            this->boundFn(s.val());
        else {
            this->errorFn(s.err());
        }
    }

    saved.clear();
    numSaved = 0;
}

bool
UnknownTypeWatchData::
tryBindNonRecursiveGeneric(const std::function<void (const Any &) > & fn,
                           WatchErrorHandler errorFn)
{
    auto guard = this->lock();
    ExcAssert(!boundFn);

    if (!saved.empty())
        return false;
    
    this->boundFn = fn;
    this->errorFn = errorFn;
    
    for (auto & s: saved) {
        if (s)
            boundFn(s.val());
        else this->errorFn(s.err());
    }
    
    saved.clear();
    numSaved = 0;

    return true;
}

bool
UnknownTypeWatchData::
bound() const
{
    auto guard = this->lock();
    return this->boundFn.operator bool ();
}

void
UnknownTypeWatchData::
unbind()
{
    auto guard = this->lock();
    this->boundFn = nullptr;
    this->errorFn = nullptr;
}

void
UnknownTypeWatchData::
multiplex(Watch && other)
{
    auto guard = this->lock();
    multiplexed.emplace_back(std::move(other));

    // Bind it in so that we get the events
    auto recursiveTrigger = std::make_shared<bool>(true);
        
    auto onEvent = [=] (const Any & val)
        {
            if (*recursiveTrigger)
                this->triggerGenericLocked(val);
            else this->triggerGeneric(val);
        };

    multiplexed.back().bindGeneric(onEvent, errorFn);

    *recursiveTrigger = false;
}


/*****************************************************************************/
/* WATCH                                                                     */
/*****************************************************************************/

Watch::
Watch(WatchData * data)
    : data(nullptr)
{
    if (data)
        data->setHolder(*this);
}

inline Watch &
Watch::
operator = (Watch && other)
{
    bool anyBefore = other.any();

    if (this->data)
        data->releaseHolder(*this);
    if (other.data)
        other.data->transferHolder(std::move(other), *this);
    this->piggyback = std::move(other.piggyback);

    ExcAssertEqual(this->any(), anyBefore);

    return *this;
}

Watch::
~Watch()
{
    this->detach();
}

void
Watch::
detach()
{
    if (!data)
        return;
    data->releaseHolder(*this);
    ExcAssert(!data);

    //piggyback.reset();
    // NOTE: we don't reset piggyback here, as it contains data whose lifecycle
    // is tied to the watch itself, not its binding.
}

void
Watch::
release(WatchData * data)
{
    ExcAssertEqual(data, this->data);
    if (this->data)
        data->releaseHolder(*this);
}

std::pair<bool, Any>
Watch::
tryWaitGeneric(double timeToWait)
{
    ExcAssert(data);
    return data->tryWaitGeneric(timeToWait);
}

void
Watch::
multiplex(Watch && w)
{
    if (!data) {
        *this = std::move(w);
    }
    else {
        data->multiplex(std::move(w));
    }
}

void
Watch::
bindGeneric(const std::function<void (const Any &) > & fn,
            WatchErrorHandler errorFn)
{
    ExcAssert(data);
    data->bindGeneric(fn, std::move(errorFn));
}

bool
Watch::
bound() const
{
    if (!data)
        return false;
    return data->bound();
}

void
Watch::
unbind()
{
    if (!data)
        return;
    data->unbind();
}

const std::type_info *
Watch::
boundType() const
{
    if (!data)
        return nullptr;
    return data->boundType();
}

std::shared_ptr<const ValueDescription>
Watch::
boundValueDescription() const
{
    if (!data)
        return nullptr;
    return data->boundValueDescription();
}

bool
Watch::
any() const
{
    if (!data) return false;
    return data->any();
}

int
Watch::
count() const
{
    if (!data) return 0;
    return data->count();
}

Any
Watch::
info() const
{
    if (!data) return nullptr;
    return data->info;
}

void
Watch::
waitForInProgressEvents() const
{
    ExcAssert(data);
    return data->waitForInProgressEvents();
}

int
Watch::
getTriggerCount() const
{
    if (!data)
        return -1;
    return data->triggers;
}

int
Watch::
getErrorCount() const
{
    if (!data)
        return -1;
    return data->errors;
}

void
Watch::
throwException(WatchErrorKind kind, const char * msg, ...) const
{
    va_list ap;
    va_start(ap, msg);
    try {
        std::string message = vformat(msg, ap);
        va_end(ap);
        WatchError error(kind, message);
        throw WatchException(std::move(error));
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}

/*****************************************************************************/
/* WATCHES                                                                   */
/*****************************************************************************/

Watches::
Watches(const std::type_info * boundType,
        std::shared_ptr<const ValueDescription> valueDescription)
    : boundType_(boundType),
      boundValueDescription_(valueDescription),
      shuttingDown(false),
      triggers(0),
      errors(0)
{
}

Watches::
~Watches()
{
    shuttingDown = true;
}

void
Watches::
release(Watch & watch)
{
    releaseWatchWithData(watch.data);
}


Watch
Watches::
add(Any info,
    std::function<bool (const Any &)> filterFn)
{
    std::unique_lock<std::mutex> guard(mutex);
    watches.emplace_back(new UnknownTypeWatchData(this, filterFn,
                                                  std::move(info)));
    return Watch(watches.back().get());
}

void
Watches::
triggerGeneric(const Any & val)
{
    std::unique_lock<std::mutex> guard(mutex);
    triggerThread = std::this_thread::get_id();
    ++triggers;
    for (auto & w: watches)
        w->triggerGeneric(val);
    triggerThread = std::thread::id();
}

void
Watches::
error(const WatchError & error)
{
    using namespace std;
    cerr << "watch got error " << jsonEncode(error) << endl;

    std::unique_lock<std::mutex> guard(mutex);
    triggerThread = std::this_thread::get_id();
    ++errors;
    for (auto & w: watches)
        w->error(error);
    triggerThread = std::thread::id();
}

void
Watches::
bindType(std::shared_ptr<const ValueDescription> boundDesc,
         const std::type_info * boundType)
{
    std::unique_lock<std::mutex> guard(mutex);

    if (this->boundType_ || this->boundValueDescription_)
        throw MLDB::Exception("Watches already have a bound type");

    this->boundValueDescription_ = std::move(boundDesc);
    this->boundType_ = boundType;
    if (!boundType_ && boundValueDescription_)
        boundType_ = boundValueDescription_->type;

    std::string typeName = demangle(*boundType_);
    if (typeName.find("std::tuple<") != 0)
        throw MLDB::Exception("Watches must always be bound to a tuple type: '%s'",
                            typeName.c_str());
}

std::vector<std::pair<Watch *, Any> >
Watches::
active() const
{
    std::vector<std::pair<Watch *, Any> > result;

    std::unique_lock<std::mutex> guard(mutex);

    for (auto & w: watches)
        result.emplace_back(std::make_pair(w->holder, w->info));

    return result;
}

void
Watches::
releaseWatchWithData(WatchData * data)
{
    if (!data)
        throw MLDB::Exception("can't release null data");

    if (data->holder)
        data->holder->data = nullptr;

    if (shuttingDown)
        return;

    std::unique_lock<std::mutex> guard(mutex, std::try_to_lock);
    if (!guard && triggerThread != std::this_thread::get_id()) {
        guard.lock();
    }
    for (unsigned i = 0;  i < watches.size();  ++i) {
        if (watches[i].get() == data) {
            if (onRelease)
                onRelease(*watches[i]->holder, std::move(watches[i]->info));
            watches.erase(watches.begin() + i);
            return;
        }
    }
        
    throw MLDB::Exception("Watch %p not found in watch set when releasing",
                        data);
}

void
Watches::
clear()
{
    std::unique_lock<std::mutex> guard(mutex);
    for (unsigned i = 0;  i < watches.size();  ++i) {
        if (onRelease)
            onRelease(*watches[i]->holder, std::move(watches[i]->info));
    }
    watches.clear();
}

WatchErrorHandler
Watches::
passError() const
{
    return std::bind(&Watches::error, const_cast<Watches *>(this),
                     std::placeholders::_1);
}

void
Watches::
throwException(WatchErrorKind kind, const char * msg, ...) const
{
    va_list ap;
    va_start(ap, msg);
    try {
        std::string message = vformat(msg, ap);
        va_end(ap);
        WatchError error(kind, message);
        throw WatchException(std::move(error));
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}

} // namespace MLDB
