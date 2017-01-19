// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* stats_events.h                                                  -*- C++ -*-
   Jeremy Barnes, 1 March 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Different types of stats events.
*/

#ifndef __logger__stats_events_h__
#define __logger__stats_events_h__

#include <utility>

namespace MLDB {

enum EventType {
    ET_HIT,          ///< Represents an extra count on a counter
    ET_COUNT,        ///< Represents an extra value accumulated
    ET_STABLE_LEVEL, ///< Represents the current level of a stable something
    ET_LEVEL,        ///< Represents the current level of something
    ET_OUTCOME       ///< Represents the outcome of an experiment
};

static constexpr std::initializer_list<int> DefaultOutcomePercentiles =
    { 90, 95, 98 };

} // namespace MLDB



#endif /* __logger__stats_events_h__ */
