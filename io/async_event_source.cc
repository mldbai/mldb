// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* async_event_source.cc
   Jeremy Barnes, 9 November 2012
*/

#include "async_event_source.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/wait_on_address.h"
#include "mldb/base/exc_assert.h"
#include <iostream>
#include "message_loop.h"
#include <cstring>


using namespace std;

namespace MLDB {

/*****************************************************************************/
/* ASYNC EVENT SOURCE                                                        */
/*****************************************************************************/

void
AsyncEventSource::
disconnect()
{
    if (!parent_)
        return;
    parent_->removeSource(this);
    parent_ = nullptr;
}

void
AsyncEventSource::
waitConnectionState(int state) const
{
    for (;;) {
        int current = connectionState_.load();
        if (current == state) return;
        ExcAssertGreaterEqual(current, DISCONNECTED);
        ExcAssertLessEqual(current, CONNECTED);
        MLDB::wait_on_address(connectionState_, current);
    }
}


/*****************************************************************************/
/* PERIODIC EVENT SOURCE                                                     */
/*****************************************************************************/

PeriodicEventSource::
PeriodicEventSource()
    : timePeriodSeconds(0),
      singleThreaded_(true)
      
{
}

PeriodicEventSource::
PeriodicEventSource(double timePeriodSeconds,
                    std::function<void (uint64_t)> onTimeout,
                    bool singleThreaded)
    : timePeriodSeconds(timePeriodSeconds),
      onTimeout(onTimeout),
      singleThreaded_(singleThreaded)
{
    init(timePeriodSeconds, onTimeout, singleThreaded);
}

void
PeriodicEventSource::
init(double timePeriodSeconds,
     std::function<void (uint64_t)> onTimeout,
     bool singleThreaded)
{
    if (timerFd.initialized())
        throw MLDB::Exception("double initialization of periodic event source");

    this->timePeriodSeconds = timePeriodSeconds;
    this->onTimeout = onTimeout;
    this->singleThreaded_ = singleThreaded;

    timerFd.init(TIMER_MONOTONIC, TIMER_NON_BLOCKING);
    timerFd.setTimeout(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::duration<double>(timePeriodSeconds)));
}

PeriodicEventSource::
~PeriodicEventSource()
{
}

int
PeriodicEventSource::
selectFd() const
{
    return timerFd.fd();
}

bool
PeriodicEventSource::
processOne()
{
    uint64_t numWakeups = timerFd.read();
    onTimeout(numWakeups);
    return false;
}


} // namespace MLDB
