// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* event_service.h                                                 -*- C++ -*-
   Jeremy Barnes, 12 December 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Service for high frequency logging of events.
*/

#pragma once

#include "mldb/soa/service/stats_events.h"
#include "mldb/arch/format.h"
#include <map>
#include <string>
#include <memory>
#include <vector>


namespace MLDB {

struct MultiAggregator;


/*****************************************************************************/
/* EVENT SERVICE                                                             */
/*****************************************************************************/

struct EventService {
    virtual ~EventService()
    {
    }
    
    virtual void onEvent(const std::string & name,
                         const char * event,
                         EventType type,
                         float value,
                         std::initializer_list<int> extra = DefaultOutcomePercentiles) = 0;

    virtual void dump(std::ostream & stream) const
    {
    }

    /** Dump the content
    */
    std::map<std::string, double> get(std::ostream & output) const;
};

/*****************************************************************************/
/* NULL EVENT SERVICE                                                        */
/*****************************************************************************/

struct NullEventService : public EventService {

    NullEventService();
    ~NullEventService();
    
    virtual void onEvent(const std::string & name,
                         const char * event,
                         EventType type,
                         float value,
                         std::initializer_list<int> extra = DefaultOutcomePercentiles);

    virtual void dump(std::ostream & stream) const;

    std::unique_ptr<MultiAggregator> stats;
};


/*****************************************************************************/
/* EVENT RECORDER                                                            */
/*****************************************************************************/

/** Bridge class to an event recorder. */

struct EventRecorder {

    EventRecorder(const std::string & eventPrefix,
                  const std::shared_ptr<EventService> & events);

    /*************************************************************************/
    /* EVENT RECORDING                                                       */
    /*************************************************************************/

    /** Notify that an event has happened.  Fields are:
        eventNum:  an ID for the event;
        eventName: the name of the event;
        eventType: the type of the event.  Default is ET_COUNT;
        value:     the value of the event (quantity being measured).
                   Default is 1.0;
        units:     the units of the event (eg, ms).  Default is unitless.
    */
    void recordEvent(const char * eventName,
                     EventType type = ET_COUNT,
                     float value = 1.0,
                     std::initializer_list<int> extra = DefaultOutcomePercentiles) const;

    void recordEventFmt(EventType type,
                        float value,
                        std::initializer_list<int> extra,
                        const char * fmt, ...) const MLDB_FORMAT_STRING(5, 6);

    template<typename... Args>
    void recordHit(const std::string & event, Args... args) const
    {
        return recordEventFmt(ET_HIT, 1.0, {}, event.c_str(),
                              forwardForPrintf(args)...);
    }

    template<typename... Args>
    MLDB_ALWAYS_INLINE
    void recordHit(const char * event, Args... args) const
    {
        return recordEventFmt(ET_HIT, 1.0, {}, event,
                              forwardForPrintf(args)...);
    }

    void recordHit(const char * event) const
    {
        recordEvent(event, ET_HIT);
    }

    void recordHit(const std::string & event) const
    {
        recordEvent(event.c_str(), ET_HIT);
    }

    template<typename... Args>
    void recordCount(float count, const std::string & event, Args... args) const
    {
        return recordEventFmt(ET_COUNT, count, {}, event.c_str(),
                              forwardForPrintf(args)...);
    }
    
    template<typename... Args>
    MLDB_ALWAYS_INLINE
    void recordCount(float count, const char * event, Args... args) const
    {
        return recordEventFmt(ET_COUNT, count, {}, event,
                              forwardForPrintf(args)...);
    }

    void recordCount(float count, const char * event) const
    {
        recordEvent(event, ET_COUNT, count);
    }

    void recordCount(float count, const std::string & event) const
    {
        recordEvent(event.c_str(), ET_COUNT, count);
    }

    template<typename... Args>
    void recordOutcome(float outcome, const std::string & event, Args... args) const
    {
        return recordEventFmt(ET_OUTCOME, outcome, DefaultOutcomePercentiles, event.c_str(),
                             forwardForPrintf(args)...);
    }
    
    template<typename... Args>
    void recordOutcome(float outcome, const char * event, Args... args) const
    {
        return recordEventFmt(ET_OUTCOME, outcome, DefaultOutcomePercentiles, event,
                              forwardForPrintf(args)...);
    }

    void recordOutcome(float outcome, const char * event) const
    {
        recordEvent(event, ET_OUTCOME, outcome, DefaultOutcomePercentiles);
    }

    void recordOutcome(float outcome, const std::string & event) const
    {
        recordEvent(event.c_str(), ET_OUTCOME, outcome, DefaultOutcomePercentiles);
    }

    template<typename... Args>
    void recordOutcomeCustom(float outcome, std::initializer_list<int> percentiles,
                               const std::string& event, Args... args) const
    {
        return recordEventFmt(ET_OUTCOME, outcome, percentiles, event.c_str(),
                             forwardForPrintf(args)...);
    }

    template<typename... Args>
    void recordOutcomeCustom(float outcome, std::initializer_list<int> percentiles,
                               const char * event, Args... args) const
    {
        return recordEventFmt(ET_OUTCOME, outcome, percentiles, event,
                              forwardForPrintf(args)...);
    }

    void recordOutcomeCustom(float outcome, std::initializer_list<int> percentiles,
                               const char * event) const
    {
        recordEvent(event, ET_OUTCOME, outcome, percentiles);
    }

    void recordOutcomeCustom(float outcome, std::initializer_list<int> percentiles,
                               const std::string & event) const
    {
        recordEvent(event.c_str(), ET_OUTCOME, outcome, percentiles);
    }
    
    template<typename... Args>
    void recordLevel(float level, const std::string & event, Args... args) const
    {
        return recordEventmt(ET_LEVEL, level, {}, event.c_str(),
                             forwardForPrintf(args)...);
    }
    
    template<typename... Args>
    void recordLevel(float level, const char * event, Args... args) const
    {
        return recordEventFmt(ET_LEVEL, level, {}, event,
                              forwardForPrintf(args)...);
    }

    void recordLevel(float level, const char * event) const
    {
        recordEvent(event, ET_LEVEL, level);
    }

    void recordLevel(float level, const std::string & event) const
    {
        recordEvent(event.c_str(), ET_LEVEL, level);
    }

    template<typename... Args>
    void recordStableLevel(float level, const std::string & event, Args... args) const
    {
        return recordEventmt(ET_STABLE_LEVEL, level, {}, event.c_str(),
                             forwardForPrintf(args)...);
    }

    template<typename... Args>
    void recordStableLevel(float level, const char * event, Args... args) const
    {
        return recordEventFmt(ET_STABLE_LEVEL, level, {}, event,
                              forwardForPrintf(args)...);
    }

    void recordStableLevel(float level, const char * event) const
    {
        recordEvent(event, ET_STABLE_LEVEL, level);
    }

    void recordStableLevel(float level, const std::string & event) const
    {
        recordEvent(event.c_str(), ET_STABLE_LEVEL, level);
    }

protected:
    std::string eventPrefix_;
    std::shared_ptr<EventService> events_;
};


} // namespace MLDB
