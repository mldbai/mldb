/* graphite_connector.h                                            -*- C++ -*-
   Jeremy Barnes, 3 August 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Class to accumulate operational stats and connect to graphite (directly).
*/

#pragma once

#include "mldb/jml/stats/distribution.h"
#include "mldb/types/date.h"
#include "stats_events.h"
#include <unordered_map>
#include <map>
#include <deque>
#include <atomic>


namespace MLDB {


struct StatReading {
    StatReading(const std::string & name = "",
                float value = 0.0,
                Date timestamp = Date())
        : name(name), value(value), timestamp(timestamp)
    {
    }

    std::string name;
    float value;
    Date timestamp;
};

/*****************************************************************************/
/* STAT AGGREGATOR                                                           */
/*****************************************************************************/

/** Generic class that aggregates statistics. */

struct StatAggregator {

    virtual ~StatAggregator()
    {
    }

    /** Record a value. */
    virtual void record(float value) = 0;

    /** Read and reset the counter, providing output in Graphite's preferred
        format. */
    virtual std::vector<StatReading> read(const std::string & prefix) = 0;
};


/*****************************************************************************/
/* COUNTER AGGREGATOR                                                        */
/*****************************************************************************/

/** Class that aggregates counts over a period of time. */

struct CounterAggregator : public StatAggregator {
    CounterAggregator();

    virtual ~CounterAggregator();

    virtual void record(float value);

    std::pair<double, Date> reset();

    /** Read and reset the counter, providing output in Graphite's preferred
        format. */
    virtual std::vector<StatReading> read(const std::string & prefix);

private:
    Date start;    //< Date at which we last cleared the counter
    std::atomic<double> total;  //< total since we last added it up
    std::deque<double> totalsBuffer; //< Totals for the last n reads.

};


/*****************************************************************************/
/* GAUGE AGGREGATOR                                                          */
/*****************************************************************************/

/** Class that aggregates a gauge over a period of time. */

struct GaugeAggregator : public StatAggregator {

    enum Verbosity
    {
        StableLevel, ///< mean
        Level,       ///< mean, min, max
        Outcome      ///< mean, min, max, percentiles, count
    };

    GaugeAggregator(Verbosity  verbosity = Outcome,
            const std::vector<int>& extra = DefaultOutcomePercentiles);

    virtual ~GaugeAggregator();

    /** Record a new value of the stat.  Lock-free but may spin briefly. */
    virtual void record(float value);

    /** Obtain a the current statistics and replace with a new version. */
    std::pair<distribution<float> *, Date> reset();

    /** Read and reset the counter, providing output in Graphite's preferred
        format.
    */
    virtual std::vector<StatReading> read(const std::string & prefix);

private:
    Verbosity verbosity;
    Date start;  //< Date at which we last cleared the counter
    std::atomic<distribution<float> *> values;  //< List of added values
    std::vector<int> extra;
};


} // namespace MLDB
