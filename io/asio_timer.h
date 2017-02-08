// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_timer.h                                                    -*- C++ -*-
   Jeremy Barnes, 1 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "mldb/watch/watch.h"
#include "mldb/types/date.h"

namespace boost {
namespace asio {
struct io_service;

// This is a typedef, and is embedded within io_context, and so can't be
// forward declared.  See the StrandHolder workaround below.
//struct strand;
} // namespace asio
} // namespace boost


namespace MLDB {

struct EventLoop;


// This class can be constructed from a boost::asio::strand without including
// the ASIO headers.  Usage is StrandHolder(strand).  Will fail at run-time
// if it's passed the wrong type.
struct StrandHolder {
    template<typename T>
    explicit StrandHolder(T & strand)
    {
        init(&strand, &typeid(strand));
    }

private:
    friend class AsioTimer;
    void init(void * strand, const std::type_info * type);
        
    void * strand;
};

/*****************************************************************************/
/* ASIO TIMER                                                                */
/*****************************************************************************/

/** Watch based timer based on boost::asio. 

    This is designed to be used as in the getTimer functions below.  Note that
    the bound function called when the watch is triggered has some
    restrictions.  It many NOT call arm() on the timer (this restriction
    may be lifted in the future).  However, it is OK if the function called
    by the watch causes the watch itself to be deallocated.
 */

struct AsioTimer {
    /** Create a timer that will be servived by the given IO service. */
    AsioTimer(boost::asio::io_service & ioService);

    /** Create a timer that will be servived by the given strand.  Note
        that this means that its callback will not be called concurrently
        with other callbacks in the same strand, ie there is mutual
        exclusion.
    */
    AsioTimer(const StrandHolder & strand);

    // Auto-arm
    AsioTimer(Date nextExpiry, double period,
              boost::asio::io_service & ioService);
    // Auto-arm
    AsioTimer(Date nextExpiry, double period,
              const StrandHolder & strand);

    ~AsioTimer();

    void arm(Date nextExpiry, double period);

    WatchT<Date> watch();

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
    
    // Private as currently disarming only works properly from the
    // destructor
    void disarm();
};

WatchT<Date> getTimer(Date nextExpiry,
                      double period,
                      EventLoop & eventLoop,
                      std::function<void (Date)> toBind = nullptr);

WatchT<Date> getTimer(Date nextExpiry,
                      double period,
                      boost::asio::io_service & ioService,
                      std::function<void (Date)> toBind = nullptr);

WatchT<Date> getTimer(Date nextExpiry,
                      double period,
                      const StrandHolder & strand,
                      std::function<void (Date)> toBind = nullptr);

extern template class WatchT<Date>;
extern template class WatchesT<Date>;

} // namespace MLDB
