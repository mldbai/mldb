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
template<typename T, typename Tag>
struct ThreadSpecificInstanceInfo
{
    typedef Spinlock Lock;

    struct Value
    {
        Value() : object(nullptr) {}
        ~Value()
        {
            ThreadSpecificInstanceInfo* oldObject = destruct();
            if (!oldObject) return;

            std::lock_guard<Lock> guard(oldObject->freeSetLock);
            oldObject->freeSet.erase(this);
        }

        ThreadSpecificInstanceInfo* destruct()
        {
            std::lock_guard<Lock> guard(destructLock);
            if (!object) return nullptr;

            storage.value.~T();
            auto oldObject = object;
            object = nullptr;

            return oldObject;
        }

        // This can't raise with either object destruction or thread destruction
        // so no locks are needed.
        void construct(ThreadSpecificInstanceInfo* newObject)
        {
            new (&storage.value) T();
            object = newObject;
        }

        /** The odd setup is to prevent spurious calls to the T constructor and
            destructor when we construct our parent class Value.

            Note that using a union is a well defined type-puning construct in
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
        ThreadSpecificInstanceInfo* object;
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
            std::lock_guard<Lock> guard(freeSetLock);
            freeSetCopy = std::move(freeSet);
        }

        for (Value* toFree : freeSetCopy)
            toFree->destruct();

        std::lock_guard<Lock> guard(freeIndexLock);
        freeIndexes.push_back(index);
    }

    static PerThreadInfo * getThisThread(bool * hadThreadInfo = nullptr)
    {
        return staticInfo.get(hadThreadInfo);
    }

    T * get(PerThreadInfo * & info, bool * hadInfo = nullptr) const
    {
        if (!info) info = staticInfo.get();
        return load(info, hadInfo);
    }

    T * get(PerThreadInfo * const & info, bool * hadInfo = nullptr) const
    {
        load(info, hadInfo);
    }

    /** Return the data for this thread for this instance of the class. */
    T * get(bool * hadInfo = nullptr) const
    {
        PerThreadInfo * info = staticInfo.get();
        return load(info, hadInfo);
    }

private:

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

    static thread_local std::unique_ptr<PerThreadInfo> staticInfo;

    static Spinlock freeIndexLock;
    static std::deque<size_t> freeIndexes;
    static unsigned nextIndex;
    int index;

    mutable Spinlock freeSetLock;
    mutable std::unordered_set<Value*> freeSet;
};

template<typename T, typename Tag>
thread_local std::unique_ptr<typename ThreadSpecificInstanceInfo<T, Tag>::PerThreadInfo>
ThreadSpecificInstanceInfo<T, Tag>::staticInfo(new typename ThreadSpecificInstanceInfo<T, Tag>::PerThreadInfo());

template<typename T, typename Tag>
Spinlock
ThreadSpecificInstanceInfo<T, Tag>::freeIndexLock;

template<typename T, typename Tag>
std::deque<size_t>
ThreadSpecificInstanceInfo<T, Tag>::freeIndexes;

template<typename T, typename Tag>
unsigned
ThreadSpecificInstanceInfo<T, Tag>::nextIndex = 0;

} // namespace MLDB
