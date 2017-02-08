/* multi_aggregator.cc
   Jeremy Barnes, 3 August 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#include "stat_aggregator.h"
#include "multi_aggregator.h"
#include <iostream>

using namespace std;
using namespace MLDB;


namespace {

StatAggregator * createNewCounter()
{
    return new CounterAggregator();
}

StatAggregator * createNewStableLevel()
{
    return new GaugeAggregator(GaugeAggregator::StableLevel);
}

StatAggregator * createNewLevel()
{
    return new GaugeAggregator(GaugeAggregator::Level);
}

StatAggregator * createNewOutcome(const std::vector<int>& percentiles)
{
    return new GaugeAggregator(GaugeAggregator::Outcome, percentiles);
}

}

/*****************************************************************************/
/* MULTI AGGREGATOR                                                          */
/*****************************************************************************/

MultiAggregator::
MultiAggregator()
    : doShutdown(false), doDump(false), dumpInterval(0.0)
{
}

MultiAggregator::
MultiAggregator(const std::string & path,
                const OutputFn & output,
                double dumpInterval,
                std::function<void ()> onStop)
    : doShutdown(false), doDump(false)
{
    open(path, output, dumpInterval, onStop);
}

MultiAggregator::
~MultiAggregator()
{
    shutdown();
}

void
MultiAggregator::
open(const std::string & path,
     const OutputFn & output,
     double dumpInterval,
     std::function<void ()> onStop)
{
    shutdown();

    doShutdown = doDump = false;
    if (dumpInterval < 1.0) {
        dumpInterval = 1.0;
    }
    this->dumpInterval = dumpInterval;
    this->onStop = onStop;

    if (path == "") prefix = "";
    else prefix = path + ".";

    if (output)
        outputFn = output;
    else
        outputFn = [&] (const std::vector<StatReading> & values)
            {
                this->doStat(values);
            };

    dumpingThread.reset
        (new std::thread(std::bind(&MultiAggregator::runDumpingThread,
                                       this)));
}

void
MultiAggregator::
stop()
{
    shutdown();
}

void
MultiAggregator::
doStat(const std::vector<StatReading> & values) const
{
    outputFn(values);
}

void
MultiAggregator::
record(const std::string & stat,
       EventType type,
       float value,
       std::initializer_list<int> extra)
{
    switch (type) {
    case ET_HIT:
        recordHit(stat);
        break;
    case ET_COUNT:
        recordCount(stat, value);
        break;
    case ET_STABLE_LEVEL:
        recordStableLevel(stat, value);
        break;
    case ET_LEVEL:
        recordLevel(stat, value);
        break;
    case ET_OUTCOME:
        recordOutcome(stat, value, extra);
        break;
    default:
        cerr << "warning: unknown stat type" << endl;
    }
}

void
MultiAggregator::
recordHit(const std::string & stat)
{
    getAggregator(stat, createNewCounter).record(1.0);
}

void
MultiAggregator::
recordCount(const std::string & stat, float quantity)
{
    getAggregator(stat, createNewCounter).record(quantity);
}

void
MultiAggregator::
recordStableLevel(const std::string & stat, float value)
{
    getAggregator(stat, createNewStableLevel).record(value);
}

void
MultiAggregator::
recordLevel(const std::string & stat, float value)
{
    getAggregator(stat, createNewLevel).record(value);
}
    
void
MultiAggregator::
recordOutcome(const std::string & stat, float value,
              const std::vector<int>& percentiles)
{
    getAggregator(stat, createNewOutcome, percentiles).record(value);
}

void
MultiAggregator::
dump()
{
    {
        std::lock_guard<std::mutex> lock(m);
        doDump = true;
    }
    
    cond.notify_all();
}

void
MultiAggregator::
dumpSync(std::ostream & stream) const
{
    std::unique_lock<Lock> guard(this->lock);

    for (auto & s: stats) {
        auto vals = s.second->read(s.first);
        for (auto v: vals) {
            stream << v.name << ":\t" << v.value << endl;
        }
    }
}

void
MultiAggregator::
shutdown()
{
    if (dumpingThread) {
        if (onPreShutdown) onPreShutdown();
        
        {
            std::lock_guard<std::mutex> lock(m);
            doShutdown = true;
        }

        cond.notify_all();
        
        dumpingThread->join();
        dumpingThread.reset();

        if (onPostShutdown) onPostShutdown();

        if (onStop) onStop();
    }
}

void
MultiAggregator::
runDumpingThread()
{
    size_t current = 0;
    Date nextWakeup = Date::now();

    for (;;) {
        std::unique_lock<std::mutex> lock(m);

        nextWakeup.addSeconds(1.0);
        if (cond.wait_until(lock, nextWakeup.toStd(), [&] { return doShutdown.load(); }))
            break;

        // Get the read lock to extract a list of stats to dump
        vector<Stats::iterator> toDump;
        {
            std::unique_lock<Lock> guard(this->lock);
            toDump.reserve(stats.size());
            for (auto it = stats.begin(), end = stats.end(); it != end;  ++it)
                toDump.push_back(it);
        }

        ++current;
        bool dumpNow = doDump.exchange(false) ||
                       (current % static_cast<size_t>(dumpInterval)) == 0;

        // Now dump them without the lock held. Note that we still need to call
        // read every second even if we're not flushing to carbon.
        for (auto it = toDump.begin(), end = toDump.end(); it != end;  ++it) {

            try {
                auto stat = (*it)->second->read((*it)->first);

                // Hack: ensures that all timestamps are consistent and that we
                // will not have any gaps within carbon.
                for (auto& s : stat) s.timestamp = nextWakeup;

                if (dumpNow) doStat(std::move(stat));
            } catch (const std::exception & exc) {
                cerr << "error writing stat: " << exc.what() << endl;
            }
        }
    }
}
