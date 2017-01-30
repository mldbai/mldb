/* legacy_event_loop.cc
   This file is part of MLDB.

   Wolfgang Sourdeau, July 2016
   Copyright (c) 2016 mldb.ai inc.  All rights reserved.
*/

#include "mldb/base/exc_assert.h"
#include "mldb/io/message_loop.h"
#include "mldb/io/legacy_event_loop.h"

using namespace MLDB;


LegacyEventLoop::
LegacyEventLoop()
    : loop_(new MessageLoop(1, 0, -1))
{
}

LegacyEventLoop::
~LegacyEventLoop()
{
    shutdown();
}

MessageLoop &
LegacyEventLoop::
loop()
    const
{
    ExcAssert(loop_.get() != nullptr);

    return *loop_;
}

void
LegacyEventLoop::
start()
{
    loop_->start();
}

void
LegacyEventLoop::
shutdown()
{
    loop_->shutdown();
}
