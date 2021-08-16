/** thread_specific.cc
    Jeremy Barnes, 13 November 2011
    Copyright (c) 2011 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Placed under the BSD license.

    Contains code to deal with thread specific data per instance of an object.
*/

#include "thread_specific.h"
#include <map>
#include <mutex>


namespace MLDB {

// Race handling
//
// We want deterministic behavior for the T destructors: the T that belongs to a specific
// ThreadSpecificInstanceInfo I for thread t should be run on the EARLIEST of either a)
// I's destructor being run or b) thread t disappearing.  However, this causes a potential
// race if thread t finishes at the same time as instance I is being destroyed.  In
// particular, the logic for destroying thread t assumes that the instance I is still
// there, and the logic for destroying instance I assumes that the values (including for
// thread t) are all still there).  So destroying both simultaneously requires
// synchronization.
//
// Since the condition is very rare, we don't synchronize in general.  Instead, we can detect
// when it happens because on the thread side, we're no longer in the free set of the
// instance, and on the instance side, the Value's object pointer is null.  So when we
// detect a race, we call these functions which synchronize everything.  In particular, they
// ensure that neither the instance nor the thread are destroyed until the other one has
// finished accessing what it needs to.
//
// The dance is:
// - The thread side unlocks the free set lock of the instance.  The instance side needs to
//   ensure that its memory isn't freed.
// - The instance side puts a false entry in resolvedRaces, and then spins waiting for it to
//   become true.
// - The thread side spins, waiting to see the entry in resolvedRaces.  When it sees it, it
//   sets the entry to true, and then exits allowing its memory to be released
// - The instance side sees the entry go to true, and exits
//
// Note that these semantics are the same as a thread barrier.  Possibly we could simplify
// by implementing it that way.

static std::mutex racesMutex;
static std::map<void *, bool> resolvedRaces;  // really a map of Value * to is client done

// Resolve from the client side.  The freeSetLock should be unlocked before calling.
void resolveDestroyRaceThreadSide(void * val)
{
    // Wait for the instance side to tell us that it's been resolved
    //::fprintf(stderr, "RACE: thread\n");

    for (;;) {
        // Spin on the race being acknowledged by the instance side
        std::unique_lock<std::mutex> guard(racesMutex);
        auto it = resolvedRaces.find(val);
        if (it == resolvedRaces.end())
            continue;
        it->second = true;
        return;
    }
}

// Resolve from the instance side
void resolveDestroyRaceInstanceSide(void * val)
{
    //::fprintf(stderr, "RACE: instance\n");

    std::unique_lock<std::mutex> guard(racesMutex);
    auto res = resolvedRaces.emplace(val, false);
    auto it = res.first;
    bool inserted = res.second;
    if (inserted == false) {
        ::fprintf(stderr, "logic error in ThreadSpecificInstanceInfo race resolution\n");
        abort();
    }

    racesMutex.unlock();

    // Now spin until the client acknowledges that it's done
    for (;;) {
        std::unique_lock<std::mutex> guard(racesMutex);
        if (it->second == false)
            continue;
        resolvedRaces.erase(it);
        return;
    }
}

} // namespace MLDB
