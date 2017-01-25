/* rcu_protected.h                                                 -*- C++ -*-
   Jeremy Barnes, 12 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Building blocks for RCU protected data structures.
*/

#pragma once

#include "gc_lock.h"
#include "mldb/jml/utils/unnamed_bool.h"
#include <memory>
#include <atomic>

namespace MLDB {

template<typename T>
struct RcuLocked {
    RcuLocked(T * ptr = nullptr, GcLock * lock = nullptr)
        : ptr(ptr), lock(lock)
    {
        if (lock)
            lock->lockShared();
    }

    /// Transfer from another lock
    template<typename T2>
    RcuLocked(T * ptr, RcuLocked<T2> && other)
        : ptr(ptr), lock(other.lock)
    {
        other.lock = nullptr;
        other.ptr = nullptr;
    }

    /// Copy from another lock
    template<typename T2>
    RcuLocked(T * ptr, const RcuLocked<T2> & other)
        : ptr(ptr), lock(other.lock)
    {
        if (lock)
            lock->lockShared();
    }

    template<typename T2>
    RcuLocked(RcuLocked<T2> && other)
        : ptr(other.ptr), lock(other.lock)
    {
        other.lock = nullptr;
    }

    RcuLocked & operator = (RcuLocked && other)
    {
        if (lock != other.lock) {
            unlock();
        }
        lock = other.lock;
        ptr = other.ptr;
        other.lock = nullptr;
        other.ptr = nullptr;
        return *this;
    }

    template<typename T2>
    RcuLocked & operator = (RcuLocked<T2> && other)
    {
        unlock();
        lock = other.lock;
        ptr = other.ptr;
        other.lock = nullptr;
        return *this;
    }

    ~RcuLocked()
    {
        unlock();
    }

    void unlock()
    {
        if (lock) {
            lock->unlockShared();
            lock = nullptr;
            ptr = nullptr;
        }
    }

    T * ptr;
    GcLock * lock;

    operator T * () const
    {
        return ptr;
    }

    T * operator -> () const
    {
        if (!ptr)
            throw MLDB::Exception("dereferencing null RCUResult");
        return ptr;
    }

    T & operator * () const
    {
        if (!ptr)
            throw MLDB::Exception("dereferencing null RCUResult");
        return *ptr;
    }
};

template<typename T>
struct RcuProtected {
    std::atomic<T *> val;
    GcLock * lock;

    template<typename... Args>
    RcuProtected(GcLock & lock, Args&&... args)
        : val(new T(std::forward<Args>(args)...)), lock(&lock)
    {
        //ExcAssert(this->lock);
    }

    RcuProtected(T * val, GcLock & lock)
        : val(val), lock(&lock)
    {
        //ExcAssert(this->lock);
    }

    RcuProtected(RcuProtected && other)
        : val(other.val), lock(other.lock)
    {
        //ExcAssert(this->lock);
        other.val = 0;
    }

    RcuProtected & operator = (RcuProtected && other)
    {
        auto toDelete = val;
        val = other.val;
        lock = other.lock;
        other.val = 0;
        lock->deferDelete(toDelete);
        //ExcAssert(lock);
        return *this;
    }

    ~RcuProtected()
    {
        lock->deferDelete(val.load());
        val = 0;
    }

    MLDB_IMPLEMENT_OPERATOR_BOOL(val);

    RcuLocked<T> operator () ()
    {
        //ExcAssert(lock);
        return RcuLocked<T>(val, lock);
    }

    RcuLocked<const T> operator () () const
    {
        //ExcAssert(lock);
        return RcuLocked<const T>(val, lock);
    }

    RcuLocked<const T> getImmutable() const
    {
        //ExcAssert(lock);
        return RcuLocked<const T>(val, lock);
    }

    T * unsafePtr() const
    {
        return val;
    }

    void replace(T * newVal, bool defer = true,
                 void (*cleanup) (T *) = GcLock::doDelete<T>)
    {
        T * toDelete = val.exchange(newVal);
        if (toDelete) {
            ExcAssertNotEqual(toDelete, val);
            if (defer) {
                if (cleanup)
                    lock->defer(cleanup, toDelete);
                else lock->deferDelete(toDelete);
            }
            else {
                lock->visibleBarrier();
                cleanup(toDelete);
            }
        }
    }

    std::unique_ptr<T> replaceCustomCleanup(T * newVal)
    {
        return std::unique_ptr<T>(val.exchange(newVal));
    }
    
    template<typename DELETER>
    bool cmp_xchg(RcuLocked<T> & current,
                  std::unique_ptr<T, DELETER> & newValue,
                  bool defer = true,
                  void (*cleanup) (T *) = GcLock::doDelete<T>)
    {
        T * currentVal = current.ptr;
        if (val.compare_exchange_strong(currentVal, newValue.get())) {
            ExcAssertNotEqual(currentVal, val);
            if (currentVal && cleanup) {
                if (defer) {
                    lock->defer(cleanup, currentVal);
                }
                else {
                    current.unlock();  // so that visible barrier can pass
                    lock->visibleBarrier();
                    cleanup(currentVal);
                }
            }
            current.ptr = newValue.get();
            newValue.release();
            return true;
        }
        return false;
    }

private:
    // Don't allow copy semantics (use RcuProtectedCopyable for that).  Just
    // move semantics are OK.
    RcuProtected();
    RcuProtected(const RcuProtected & other);
    void operator = (const RcuProtected & other);
};

template<typename T>
struct RcuProtectedCopyable : public RcuProtected<T> {

    using RcuProtected<T>::val;
    using RcuProtected<T>::lock;
    using RcuProtected<T>::operator ();
    using RcuProtected<T>::replace;

    template<typename... Args>
    RcuProtectedCopyable(GcLock & lock, Args&&... args)
        : RcuProtected<T>(lock, std::forward<Args>(args)...)
    {
    }

    RcuProtectedCopyable(const RcuProtectedCopyable & other)
        : RcuProtected<T>(new T(*other()), *other.lock)
    {
    }

    RcuProtectedCopyable(RcuProtectedCopyable && other)
        : RcuProtected<T>(static_cast<RcuProtected<T> &&>(other))
    {
    }

    RcuProtectedCopyable & operator = (const RcuProtectedCopyable & other)
    {
        //ExcAssert(lock);
        auto toDelete = val;
        lock = other.lock;
        val = new T(*other());
        if (toDelete) lock->deferDelete(toDelete);
        return *this;
    }

    RcuProtectedCopyable & operator = (RcuProtectedCopyable && other)
    {
        //ExcAssert(lock);
        auto toDelete = val;
        lock = other.lock;
        val = other.val;
        other.val = 0;
        if (toDelete) lock->deferDelete(toDelete);
        return *this;
    }

};

} // namespace MLDB
   
