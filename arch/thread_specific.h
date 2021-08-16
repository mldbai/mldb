/** thread_specific.h                                              -*- C++ -*-
    Jeremy Barnes, 13 November 2011
    Copyright (c) 2011 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Placed under the BSD license.

    Contains code to deal with thread specific data in such a way that:
    a) it's deleted (unlike the __thread keyword);
    b) it's not dog slow (unlike the boost::thread_specific_ptr)

    The downside is that each needs to be tagged with a unique type as
    it requires static variables.
*/

#pragma once

#include "spinlock.h"
#include "mldb/compiler/compiler.h"
#include <thread>
#include <deque>
#include <unordered_set>
#include <map>
#include <mutex>

namespace MLDB {

/*****************************************************************************/
/* THREAD SPECIFIC INSTANCE INFO                                             */
/*****************************************************************************/

/** This structure allows information to be stored per instance of a variable
    per thread.  To do so, include this structure somewhere in the
    class that you want to associate the info with.

    Note that while this class has several locks, they're only grabbed when an
    instance is created, destroyed or first accessed. Past the first access,
    reads equate to a deque probe.

*/
template<typename T>
struct ThreadSpecificInstanceInfo
{
    typedef Spinlock Lock;

    /// Holds the actual object of type T that's allocated per instance per thread,
    /// plus some housekeeping information.
    struct Value {
        Value() : object(nullptr) {}

        Value(const Value & other)
            : object(other.object.load())
        {

        }

        ~Value()
        {
            ThreadSpecificInstanceInfo* oldObject = destruct();
            if (!oldObject) return;

            std::unique_lock<Lock> guard(oldObject->freeSetLock);
            if (!oldObject->freeSet.erase(this)) {
                // The value is being simultaneously destroyed by a) the destruction of
                // the thread that owns it and b) the destruction of the instance that
                // owns it.  Since this Value lives in the deque that's tied to the
                // lifetime of this thread, we need to ensure that the object lives
                // long enough for the instance destruction to call destruct (where it
                // will discover that there is nothing left to do).

                // To make this happen, we synchronize on the oldowner's freeing lock.
                // Once that lock is released, we know that we can safely finish deallocating
                // values.
                resolveDestroyRaceThreadSide(this, oldObject, guard);
            }
        }

        ThreadSpecificInstanceInfo* destruct()
        {
            std::lock_guard<Lock> guard(destructLock);
            if (!object) return nullptr;

            storage.value.~T();

            return object.exchange(nullptr);
        }

        // This can't raise with either object destruction or thread destruction
        // so no locks are needed.
        void construct(ThreadSpecificInstanceInfo* newObject)
        {
            new (&storage.value) T();
            object.store(newObject);
        }

        /** The odd setup is to prevent spurious calls to the T constructor and
            destructor when we construct our parent class Value.

            Note that using a union is a well defined type-punning construct in
            gcc while reinterpret_cast<> could cause problems when used with
            strict-aliasing (I think). Feel free to simplify it if I'm wrong.
         */
        union Storage
        {
            Storage() {}
            ~Storage() {}

            T value;
            uint8_t unused[sizeof(T)];
        } storage;


        Lock destructLock;
        std::atomic<ThreadSpecificInstanceInfo*> object;
    };

    typedef std::deque<Value> PerThreadInfo;

    ThreadSpecificInstanceInfo()
    {
        std::lock_guard<Lock> guard(freeIndexLock);

        if (!freeIndexes.empty()) {
            index = freeIndexes.front();
            freeIndexes.pop_front();
        }
        else index = ++nextIndex;
    }

    ~ThreadSpecificInstanceInfo()
    {
        // We don't want to be holding the freeSet lock when calling destruct
        // because thread destruction will also attempt to lock our freeSet lock
        // which is a receipie for deadlocks.
        std::unordered_set<Value*> freeSetCopy;
        {
            {
                std::unique_lock<Lock> guard(freeSetLock);
                freeSetCopy = std::move(freeSet);
            }

            for (Value* toFree : freeSetCopy) {
                ThreadSpecificInstanceInfo * owner = toFree->destruct();
                if (owner == nullptr) {
                    // We got a race; here we're in thread destruction trying to destroy the
                    // values that belong to this thread, but elsewhere we're in instance
                    // destruction trying to destroy the values that belong to a particular
                    // instance.  We resolve this with a dance.
                    resolveDestroyRaceInstanceSide(toFree, this);
                }
            }
        }

        std::lock_guard<Lock> guard(freeIndexLock);
        freeIndexes.push_back(index);
    }

    static PerThreadInfo * getThisThread(bool * hadThreadInfo = nullptr)
    {
        return getStaticInfo(hadThreadInfo);
    }

    T * get(PerThreadInfo * & info, bool * hadInfo = nullptr) const
    {
        if (!info) info = getStaticInfo();
        return load(info, hadInfo);
    }

    T * get(PerThreadInfo * const & info, bool * hadInfo = nullptr) const
    {
        load(info, hadInfo);
    }

    /** Return the data for this thread for this instance of the class. */
    T * get(bool * hadInfo = nullptr) const
    {
        PerThreadInfo * info = getStaticInfo();
        return load(info, hadInfo);
    }

private:

    // Return the static info for the calling thread.  If hadThreadInfo is
    // non-null, then return a bool saying whether it was already there
    // (false) or newly created (true).
    //
    // This is a static function due to some changes between Ubuntu 16.04
    // and 18.04 (probably a bug in the thread support library) which mean
    // that a static thread_local member of a templated class always returns
    // a different thread specific object per access.  Putting it into a
    // method-local static variable fixes the problem, and has the added
    // bonus of improving performance.
    
    static PerThreadInfo * getStaticInfo(bool * hadThreadInfo = nullptr)
    {
        static thread_local std::unique_ptr<PerThreadInfo> info_;
        std::unique_ptr<PerThreadInfo> & info = info_;
        
        if (MLDB_LIKELY(info.get() != nullptr)) {
            if (MLDB_UNLIKELY(hadThreadInfo != nullptr))
                *hadThreadInfo = true;
            return info.get();
        }

        info.reset(new PerThreadInfo());
        if (MLDB_UNLIKELY(hadThreadInfo != nullptr))
            *hadThreadInfo = false;
        return info.get();
        
    }
    
    T * load(PerThreadInfo * info, bool * hadInfo) const
    {
        while (info->size() <= index)
            info->emplace_back();

        Value& val = (*info)[index];

        if (MLDB_UNLIKELY(!val.object)) {
            if (hadInfo)
                *hadInfo = false;
            val.construct(const_cast<ThreadSpecificInstanceInfo*>(this));
            std::lock_guard<Lock> guard(freeSetLock);
            freeSet.insert(&val);
        }
        else {
            if (hadInfo)
                *hadInfo = true;
        }

        return &val.storage.value;
    }

    static Spinlock freeIndexLock;
    static std::deque<size_t> freeIndexes;
    static unsigned nextIndex;
    int index;

    /// Mutex protecting the access fo the freeSet
    mutable Spinlock freeSetLock;

    /// Set of Values that need to be freed
    mutable std::unordered_set<Value*> freeSet;

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

    // Resolve from the client side.  The freeSetLock should be locked.
    static void resolveDestroyRaceThreadSide(Value * val, ThreadSpecificInstanceInfo * object, std::unique_lock<Lock> & freeSetLock)
    {
        // Wait for the instance side to tell us that it's been resolved
        //::fprintf(stderr, "RACE: thread\n");
        freeSetLock.unlock();

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
    static void resolveDestroyRaceInstanceSide(Value * val, ThreadSpecificInstanceInfo * object)
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
};

template<typename T>
Spinlock
ThreadSpecificInstanceInfo<T>::freeIndexLock;

template<typename T>
std::deque<size_t>
ThreadSpecificInstanceInfo<T>::freeIndexes;

template<typename T>
unsigned
ThreadSpecificInstanceInfo<T>::nextIndex = 0;

template<typename T>
std::mutex
ThreadSpecificInstanceInfo<T>::racesMutex;

template<typename T>
std::map<void *, bool>
ThreadSpecificInstanceInfo<T>::resolvedRaces;

} // namespace MLDB
