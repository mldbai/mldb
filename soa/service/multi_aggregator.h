/* multi_aggregator.h                                              -*- C++ -*-
   Jeremy Barnes, 3 August 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <condition_variable>
#include "mldb/soa/service/stat_aggregator.h"
#include "mldb/arch/thread_specific.h"
#include <iostream>

namespace MLDB {

/*****************************************************************************/
/* MULTI AGGREGATOR                                                          */
/*****************************************************************************/

/** Aggregates multiple stats together. */

struct MultiAggregator {
    MultiAggregator();

    typedef std::function<void (const std::vector<StatReading> &)>
        OutputFn;

    MultiAggregator(const std::string & path,
                    const OutputFn & output = OutputFn(),
                    double dumpInterval = 1.0,
                    std::function<void ()> onStop
                        = std::function<void ()>());
    
    ~MultiAggregator();

    void open(const std::string & path,
              const OutputFn & output = OutputFn(),
              double dumpInterval = 1.0,
              std::function<void ()> onStop
                  = std::function<void ()>());

    OutputFn outputFn;

    /** Function to be called when the stat is to be done.  Default will
        call the OutputFn.
    */
    virtual void doStat(const std::vector<StatReading> & value) const;

    /** Record, generic version. */
    void record(const std::string & stat,
                EventType type = ET_COUNT,
                float value = 1.0,
                std::initializer_list<int> extra = DefaultOutcomePercentiles);

    /** Simplest interface: record that a particular event occurred.  The
        stat will record the total count for each second.  Lock-free and
        thread safe.
    */
    void recordHit(const std::string & stat);

    /** Record that something happened.  The stat will record the total amount
        in each second.
    */
    void recordCount(const std::string & stat, float quantity);

    /** Record the value of a something. THe stat will record the mean of that
        value over a second.

        Lock-free (except the first time it's called for each name) and thread
        safe.
     */
    void recordStableLevel(const std::string & stat, float value);

    /** Record the level of a something.  The stat will record the mean, minimum
        and maximum level over the second.

        Lock-free (except the first time it's called for each name) and
        thread safe.
    */
    void recordLevel(const std::string & stat, float value);
    
    /** Record that a given value from an independent process. The stat will
        record the mean, mininum, maxixmum outcome over the second as well as
        the percentiles specified by the last argument, defaulting to the
        90th, 95th and 98th percentiles and the number of outcomes.

        Lock-free (except the first time it's called for each name) and thread
        safe.
    */
    void recordOutcome(const std::string & stat, float value,
            const std::vector<int>& percentiles = DefaultOutcomePercentiles);

    /** Dump synchronously (taking the lock).  This should only be used in
        testing or debugging, not when connected to Carbon.
    */
    void dumpSync(std::ostream & stream) const;

    /** Wake up and dump the data.  Asynchronous; this signals the dumping
        thread to do the actual dump.
    */
    void dump();

    /** Stop the dumping. */
    void stop();
    
protected:
    // Prefix to add to each stat to put it in its namespace
    std::string prefix;

    // Function to call when it's stopped/shutdown
    std::function<void ()> onStop;

    // Functions to implement the shutdown
    std::function<void ()> onPreShutdown, onPostShutdown;

private:
    // This map can only have things removed from it, never added to
    typedef std::map<std::string, std::shared_ptr<StatAggregator> > Stats;
    Stats stats;

    // R/W mutex for reading/writing stats.  Read to look up, write to
    // add a new stat.
    typedef std::mutex Lock;
    mutable Lock lock;

    typedef std::unordered_map<std::string, Stats::iterator> LookupCache;

    /** Thread that's started up to start dumping. */
    void runDumpingThread();

    /** Shutdown everything. */
    void shutdown();

    // Cache of lookups for each thread to avoid needing to acquire a lock
    // very much.
    ThreadSpecificInstanceInfo<LookupCache, void> lookupCache;

    /** Look for the aggregator for this given stat.  If it doesn't exist,
        then initialize it from the given function.
    */
    template<typename... Args>
    StatAggregator & getAggregator(const std::string & stat,
                                   StatAggregator * (*createFn) (Args...),
                                   Args&&... args)
    {
        using namespace std;

        auto & threadCache = *lookupCache.get();

        auto found = threadCache.find(stat);
        if (found != threadCache.end()) {
            return *found->second->second;
        }

        // Get the read lock to look for the aggregator
        std::unique_lock<Lock> guard(lock);

        auto found2 = stats.find(stat);

        if (found2 != stats.end()) {
            guard.unlock();

            threadCache[stat] = found2;

            return *found2->second;
        }

        guard.unlock();

        // Get the write lock to add it to the aggregator
        std::unique_lock<Lock> guard2(lock);

        // Add it in
        found2 = stats.insert(
                make_pair(stat, std::shared_ptr<StatAggregator>(createFn(std::forward<Args>(args)...)))).first;

        guard2.unlock();
        threadCache[stat] = found2;
        return *found2->second;
    }
    
    std::unique_ptr<std::thread> dumpingThread;

    std::condition_variable cond;  // to wake up dumping thread
    std::mutex m;
    std::atomic<bool> doShutdown;                 // thread woken up to shutdown
    std::atomic<bool> doDump;                     // thread woken up to dump

    /** How many seconds to wait before we dump.  If set to zero, dumping
        is only done on demand.
    */
    double dumpInterval;
};

} //namespace MLDB
