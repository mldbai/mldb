/* legacy_event_loop.h                                               -*-C++-*-
   This file is part of MLDB.

   Wolfgang Sourdeau, July 2016
   Copyright (c) 2016 Datacratic.  All rights reserved.
*/

#pragma once

#include <memory>


namespace Datacratic {

/* Forward declarations */
struct MessageLoop;


/****************************************************************************/
/* LEGACY EVENT LOOP                                                        */
/****************************************************************************/

/* A wrapper around MessageLoop that provides an API similar to the EventLoop
 * class */

struct LegacyEventLoop {
    LegacyEventLoop();
    ~LegacyEventLoop();

    /** Return the associated MessageLoop instance */
    MessageLoop & loop() const;

    /** Start the loop thread */
    void start();

    /** Shutdown the loop thread */
    void shutdown();

private:
    std::unique_ptr<MessageLoop> loop_;
};

} // namespace Datacratic
