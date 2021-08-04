// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rt.h                                                            -*- C++ -*-
   Jeremy Barnes, 11 January 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   Real-time utilities.
*/

#ifndef __jml__arch__rt_h__
#define __jml__arch__rt_h__

#include <thread>

namespace MLDB {


bool makeThreadRealTime(std::thread::native_handle_type handle, int priority);

/** Make the given boost::thread into a realtime thread with the given
    priority (from zero upwards).  This will put it into the round-robin
    real time scheduling class for the given priority level.

    Note that either a root process or extra capabilities are required to
    enable this functionality.

    Returns whether or not the call succeeded.
*/

inline bool makeThreadRealTime(std::thread & thread, int priority)
{
    return makeThreadRealTime(thread.native_handle(), priority);
}

} // namespace MLDB

#endif /* __jml__arch__rt_h__ */

