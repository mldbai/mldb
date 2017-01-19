// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_collection_stress_test.cc
   Jeremy Barnes, 25 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/utils/runner.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/rest/rest_collection.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/arch/timers.h"
#include <chrono>
#include <thread>
#include <functional>
#include <numeric>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;


/******************************************************************************/
/* FAIR RW LOCK                                                               */
/******************************************************************************/

/** Comes from here:
    
    https://github.com/RAttab/lockless/blob/master/src/rwlock.h

    With the following copyright and license:
    
    Rémi Attab (remi.attab@gmail.com), 24 Aug 2013
    FreeBSD-style copyright and disclaimer apply
*/

struct FairRWLock
{
    static constexpr uint64_t Mask = 0xFFFF;

    FairRWLock() { d.all = 0; }

    void lock()
    {
        uint16_t ticket = d.split.tickets.fetch_add(1);
        while (ticket != d.split.writes);
    }

    bool tryLock()
    {
        uint64_t val;
        uint64_t old = d.all;

        do {
            uint16_t writes = old >> 16;
            uint16_t tickets = old >> 32;
            if (writes != tickets) return false;

            tickets++;
            val = (old & ~(Mask << 32)) | (uint64_t(tickets) << 32);

        } while (!d.all.compare_exchange_weak(old, val));

        return true;
    }

    void unlock()
    {
        /* This function is implemented this way to avoid an atomic increment
           and go for a simple load store operation instead.
         */

        uint16_t reads = uint16_t(d.split.reads) + 1;
        uint16_t writes = uint16_t(d.split.writes) + 1;

        // Note that tickets can still be modified so we can't update d.all.
        d.rw = uint32_t(reads) | uint32_t(writes) << 16;
    }

    void readLock()
    {
        uint16_t ticket = d.split.tickets.fetch_add(1);
        while (ticket != d.split.reads);

        // Since we've entered a read section, allow other reads to continue.
        d.split.reads++;
    }

    bool tryReadLock()
    {
        uint64_t val;
        uint64_t old = d.all;

        do {
            uint16_t reads = old;
            uint16_t tickets = old >> 32;

            if (reads != tickets) return false;

            reads++;
            tickets++;
            val = (old & (Mask << 16)) | uint64_t(tickets) << 32 | reads;
        } while (!d.all.compare_exchange_weak(old, val));

        return true;
    }

    void readUnlock()
    {
        d.split.writes++;
    }

private:

    union {
        struct {
            std::atomic<uint16_t> reads;
            std::atomic<uint16_t> writes;
            std::atomic<uint16_t> tickets;
        } split;
        std::atomic<uint32_t> rw;
        std::atomic<uint64_t> all;
    } d;
};


/** This is a stress test of the RestCollection, that tests the ability of
    a watch to maintain an accurate view of the state of a container at
    all times.

    It performs mutations of the container in one or more threads at the
    same time as creating watches from another thread.  Occasionally,
    the mutator threads will be stopped and the state according to the
    watches compared against the real state of the container.
*/

BOOST_AUTO_TEST_CASE( stress_test_watch_coherency )
{
    typedef RestCollection<std::string, std::string> Coll;
    Coll collection("item", "items", &collection);
    
    std::atomic<bool> shutdown(false);

    for (unsigned i = 0;  i < 20;  ++i)
        collection.addEntry("item" + to_string(i),
                            std::make_shared<std::string>("hello"),
                            false /* mustAdd */);

    int numMutatorThreads = 2;

    // Mutex to allow the mutate threads (which are "reading" the collection)
    // to be stopped by the listener so that it can ascertain if it's in a
    // coherent state.
    std::atomic<int> numMutatorThreadsRunning(0);

    FairRWLock lock;

    auto mutateThread = [&] ()
        {
            int index MLDB_UNUSED = 0;
            while (!shutdown) {
                lock.readLock();

                ++numMutatorThreadsRunning;

                std::string key = "item" + to_string(random() % 20);
                std::string value = to_string(index++);
                
                if (collection.addEntry(key ,
                                        std::make_shared<std::string>(value),
                                        false /* mustAdd */)) {
                    //cerr << MLDB::format("mut: added entry %s %s\n",
                    //                   key.c_str(), value.c_str());
                }
                    
                
                key = "item" + to_string(random() % 20);
                
                if (collection.deleteEntry(key)) {
                    //cerr << MLDB::format("mut: deleted entry %s\n",
                    //                   key.c_str());
                }

                --numMutatorThreadsRunning;

                lock.readUnlock();
            };
            
            cerr << "finished work thread with " << index << " mutations"
                 << endl;
        };
    
    auto listenThread = [&] (int threadNum)
        {
            ExcAssertGreater(threadNum, 0);

            while (!shutdown) {
                WatchT<Coll::ChildEvent> watch
                    = collection.watchElements("*", true /* catchUp */,
                                               string("watchAll"));

                std::map<std::string, std::string> currentState;

                std::atomic<bool> eventsAreFinished(false);

                // When we get an event, we update our representation of
                // the current state of the universe.
                auto onEvent = [&] (const Coll::ChildEvent & ev)
                {
                    if (eventsAreFinished) {
                        cerr << "got event after: threadNum " << threadNum
                        << " key " << ev.key << " value " << *ev.value
                        << " event " << ev.event
                        << " shutdown " << shutdown << endl;
                        ExcAssert(!eventsAreFinished);
                    }

                    switch (ev.event) {
                    case CE_NEW: {
                        string key = ev.key;
                        string value = *ev.value;
                        //cerr << "new element " << key << " " << value << endl;
                        
                        ExcAssert(!currentState.count(key));
                        currentState[key] = value;
                        break;
                    }
                    case CE_DELETED: {
                        string key = ev.key;
                        string value = *ev.value;
                        //cerr << "deleted element " << key << " " << value << endl;

                        ExcAssert(currentState.count(key));
                        currentState.erase(key);
                        break;
                    }
                    default:
                    throw MLDB::Exception("unexpected watch event");
                    }
                };
                // Bind in our event handler
                watch.bind(onEvent);

                // Wait for some events to accumulate
                std::this_thread::sleep_for(std::chrono::milliseconds(10));

                lock.lock();

                // Wait for everything that's happening to finish.
                watch.waitForInProgressEvents();

                // There should be no events from now on
                eventsAreFinished = true;

                std::atomic_thread_fence(std::memory_order_release);
                
                int numDone = 0;
                
                auto onEntry = [&] (const std::string & key,
                                    const std::string & value)
                {
                    ExcAssert(currentState.count(key));
                    ExcAssertEqual(currentState[key], value);
                    ++numDone;
                    return true;
                };

                collection.forEachEntry(onEntry);
                
                ExcAssertEqual(numDone, currentState.size());

                eventsAreFinished = false;

                watch.unbind();

                lock.unlock();
            }
            
            cerr << "finished listen thread" << endl;
        };

    std::vector<std::thread> threads;
    for (unsigned i = 0;  i < numMutatorThreads;  ++i) {
        threads.emplace_back(mutateThread);
    }

    int numListenThreads = 2;

    for (unsigned i = 0;  i < numListenThreads;  ++i) {
        threads.emplace_back(std::bind(listenThread, i + 1));
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    shutdown = true;
    
    for (auto & t: threads)
        t.join();
}

/** This is a stress test of the RestCollection, that tests the coherence and integrity 
    of the collection against repetive updates and reads.
*/
BOOST_AUTO_TEST_CASE( stress_test_collection_integrity )
{
    typedef RestCollection<std::string, double> Coll;
    Coll collection("item", "items", &collection);
    
    std::atomic<bool> shutdown(false);

    collection.addEntry("item", std::make_shared<double>(0.0), false /* mustBeNewEntry */);

    auto overwriteThread = [&] ()
        {
            Timer timer;
            while (!shutdown) {

                std::string key = "item";
                
                if (!collection.replaceEntry(key ,
                                             std::make_shared<double>(timer.elapsed_ticks()),
                                             false /* mustBeNewEntry */)) {
                    cerr << "failed to add the entry" << endl;
                    BOOST_ERROR("failed to replace an entry");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            };
        };
    
    std::vector<double> delays;
    
    auto listenThread = [&] ()
        {
            double previous_value = 0;
            while (!shutdown) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                auto entry = collection.getEntry("item");
                delays.emplace_back(*entry.first - previous_value);
                /* value should be increasing */
                BOOST_CHECK_LE(previous_value, *entry.first);
                previous_value = *entry.first;
            }
        };


    std::thread updater(overwriteThread);
    std::thread reader(listenThread);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    shutdown = true;
    
    updater.join();
    reader.join();
    double average = std::accumulate(delays.begin(), delays.end(), 0.0) / delays.size();
    double max = *std::max_element(delays.begin(), delays.end());

    // check that the updates have been happening timely
    BOOST_CHECK(average != max); // nah it can't be so deterministic!
    // more difference between the max time between update and the average time would be fishy
    BOOST_CHECK_LE(max / average, 10);
}

struct Counter {
    static std::atomic<unsigned int> count;
    Counter()
    {
        ++count;
        //cerr << "creating at " << this << endl;
    }
    ~Counter()
    {
        --count;
        //cerr << "deleting at " << this << endl;
    }
};

std::atomic<unsigned int> Counter::count(0);
        
/** This is a stress test of the RestCollection, to ensure that replacing entries does
    not leave object behind.
*/
BOOST_AUTO_TEST_CASE( stress_test_collection_overwrite )
{
    typedef RestCollection<std::string, Counter> Coll;
    Coll collection("counted", "counted", &collection);
    
    std::atomic<bool> shutdown(false);

    collection.addEntry("counter", std::make_shared<Counter>(), false /* mustBeNewEntry */);

    auto overwriteThread = [&] ()
        {
            Timer timer;
            while (!shutdown) {

                std::string key = "counter";
                
                if (!collection.replaceEntry(key ,
                                             std::make_shared<Counter>(),
                                             false /* mustBeNewEntry */)) {
                    cerr << "failed to add the entry" << endl;
                    BOOST_ERROR("failed to replace an entry");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            };
        };
    
    auto listenThread = [&] ()
        {
            while (!shutdown) {
                auto entry = collection.getEntry("counter");
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                //cerr << entry.first->count << endl;
            }
        };


    std::vector<std::thread> threads;
    unsigned int numMutatorThreads = 10;
    for (unsigned i = 0;  i < numMutatorThreads;  ++i) {
        threads.emplace_back(overwriteThread);
    }

    std::thread reader(listenThread);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    shutdown = true;
    
    for (auto & t: threads)
        t.join();
     
    reader.join();
    // all object overwritten should be gone by now
    BOOST_CHECK_EQUAL(Counter::count, 1);
}
