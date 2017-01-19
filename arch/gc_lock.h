/* gc_lock.h                                                       -*- C++ -*-
   Jeremy Barnes, 19 November 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   "Lock" that works by deferring the destruction of objects into a garbage collection
   process which is only run when nothing could be using them.
*/

#pragma once

#include "mldb/base/exc_assert.h"
#include "mldb/arch/thread_specific.h"
#include <vector>
#include <memory>

/** Deterministic memory reclamation is a fundamental problem in lock-free
    algorithms and data structures.

    When concurrently updating a lock-free data structure, it is not safe to
    immediately reclaim the old value since a bazillon other threads might still
    hold a reference to the old value. What we need is a safe memory reclamation
    mechanism. That is why GcLock exists. GcLock works by deferring the
    destruction of an object until it is safe to do; when the system decides
    that nobody still holds a reference to it.

    GcLock works by defining "critical sections" that a thread should hold to
    safely read a shared object.

    Note that this class contains many types of critical sections which are all
    specialized for various situations:

    - Shared CS: Regular read side critical sections.
    - Exclusive CS: Acts as the write op in a read-write lock with the shared CS
    - Speculative CS: Optimization of Shared CS whereby the CS may or may not be
      unlocked when requested to save on repeated accesses to the CS.

    Further details is available in the documentation of each respective
    operand.

*/

namespace MLDB {

extern int32_t SpeculativeThreshold;

/*****************************************************************************/
/* GC LOCK BASE                                                              */
/*****************************************************************************/

struct GcLockBase {

    GcLockBase(const GcLockBase &) = delete;
    void operator = (const GcLockBase &) = delete;

public:

    /** Enum for type safe specification of whether or not we run deferrals on
        entry or exit to a critical sections.  Thoss places that are latency
        sensitive should use RD_NO.
    */
    enum RunDefer {
        RD_NO = 0,      ///< Don't run deferred work on this call
        RD_YES = 1      ///< Potentially run deferred work on this call
    };

    /// A thread's bookkeeping info about each GC area
    struct ThreadGcInfoEntry {
        ThreadGcInfoEntry();
        ~ThreadGcInfoEntry();

        int inEpoch;  // 0, 1, -1 = not in 
        int readLocked;
        int writeLocked;

        int specLocked;
        int specUnlocked;

        GcLockBase *owner;

        void init(const GcLockBase * const self);
        void lockShared(RunDefer runDefer);
        void unlockShared(RunDefer runDefer);
        bool isLockedShared();
        void lockExclusive();
        void unlockExclusive();
        void lockSpeculative(RunDefer runDefer);
        void unlockSpeculative(RunDefer runDefer);
        void forceUnlock(RunDefer runDefer);
        std::string print() const;
    };

    typedef ThreadSpecificInstanceInfo<ThreadGcInfoEntry, GcLockBase>
        GcInfo;
    typedef typename GcInfo::PerThreadInfo ThreadGcInfo;

    struct Atomic;
    struct Data;

    void enterCS(ThreadGcInfoEntry * entry = 0, RunDefer runDefer = RD_YES);
    void exitCS(ThreadGcInfoEntry * entry = 0, RunDefer runDefer = RD_YES);
    void enterCSExclusive(ThreadGcInfoEntry * entry = 0);
    void exitCSExclusive(ThreadGcInfoEntry * entry = 0);

    int myEpoch(GcInfo::PerThreadInfo * threadInfo = 0) const;
    int currentEpoch() const;

    MLDB_ALWAYS_INLINE ThreadGcInfoEntry &
    getEntry(GcInfo::PerThreadInfo * info = 0) const
    {
        ThreadGcInfoEntry *entry = gcInfo.get(info);
        entry->init(this);
        return *entry;

        //return *gcInfo.get(info);
    }

    GcLockBase();

    virtual ~GcLockBase();

    /** Permanently deletes any resources associated with this lock. */
    virtual void unlink() = 0;

    void lockShared(GcInfo::PerThreadInfo * info = 0,
                    RunDefer runDefer = RD_YES);
    void unlockShared(GcInfo::PerThreadInfo * info = 0, 
                      RunDefer runDefer = RD_YES);

    /** Speculative critical sections should be used for hot loops doing
        repeated but short reads on shared objects where it's acceptable to keep
        hold of the section in between read operations because you're likely to
        need it again soon.

        This is an optimization of lockShared since it can avoid repeated entry
        and exit of the CS when it's likely to be reused shortly after. It also
        has the effect of heavily reducing contention on the lock under heavy
        contention scenarios.

        Usage example:

            GcLock gc;
            for (condition) {
                gc.enterSpeculative();
                // In critical section

                gc.exitSpeculative();
                // After the call, gc might or might not be unlocked
            }

            gc.forceUnlock();

        Note the call to forceUnlock() after the loop which ensure that we've
        exited the critical section. Also note that the speculative functions
        are called directly in this example for illustrative purposes. In actual
        code, use the SpeculativeGuard class which provides full RAII
        guarantees.
    */
    void lockSpeculative(GcInfo::PerThreadInfo * info = 0,
                         RunDefer runDefer = RD_YES);

    void unlockSpeculative(GcInfo::PerThreadInfo * info = 0,
                           RunDefer runDefer = RD_YES);

    /** Ensures that after the call, the Gc is "unlocked".

        This should be used in conjunction with the speculative lock to notify
        the Gc Lock to exit any leftover speculative sections for the current
        thread. If multiple threads can hold a speculative region, this function
        has to be called in each thread respectively. Note that it will be
        called automatically when a thread is destroyed.
     */
    void forceUnlock(GcInfo::PerThreadInfo * info = 0,
                     RunDefer runDefer = RD_YES);
        
    int isLockedShared(GcInfo::PerThreadInfo * info = 0) const;

    int lockedInEpoch(GcInfo::PerThreadInfo * info = 0) const;

    void lockExclusive(GcInfo::PerThreadInfo * info = 0);

    void unlockExclusive(GcInfo::PerThreadInfo * info = 0);

    int isLockedExclusive(GcInfo::PerThreadInfo * info = 0) const;

    bool isLockedByAnyThread() const;

    enum DoLock {
        DONT_LOCK = 0,
        DO_LOCK = 1
    };

    struct SharedGuard {
        SharedGuard(GcLockBase & lock,
                    RunDefer runDefer = RD_YES,
                    DoLock doLock = DO_LOCK)
            : lock_(lock),
              runDefer_(runDefer),
              doLock_(doLock)
        {
            if (doLock_)
                lock_.lockShared(0, runDefer_);
        }

        ~SharedGuard()
        {
            if (doLock_)
                lock_.unlockShared(0, runDefer_);
        }
        
        void lock()
        {
            if (doLock_)
                return;
            lock_.lockShared(0, runDefer_);
            doLock_ = DO_LOCK;
        }

        void unlock()
        {
            if (!doLock_)
                return;
            lock_.unlockShared(0, runDefer_);
            doLock_ = DONT_LOCK;
        }

        GcLockBase & lock_;
        const RunDefer runDefer_;  ///< Can this do deferred work?
        DoLock doLock_;      ///< Do we really lock?
    };

    struct ExclusiveGuard {
        ExclusiveGuard(GcLockBase & lock)
            : lock(lock)
        {
            lock.lockExclusive();
        }

        ~ExclusiveGuard()
        {
            lock.unlockExclusive();
        }

        GcLockBase & lock;
    };

    struct SpeculativeGuard {
        SpeculativeGuard(GcLockBase &lock,
                         RunDefer runDefer = RD_YES)
            : lock(lock),
              runDefer_(runDefer) 
        {
            lock.lockSpeculative(0, runDefer_);
        }

        ~SpeculativeGuard() 
        {
            lock.unlockSpeculative(0, runDefer_);
        }

        GcLockBase & lock;
        const RunDefer runDefer_;
    };


    /** Wait until everything that's currently visible is no longer
        accessible.
        
        You can't call this if a guard is held, as it would deadlock waiting
        for itself to exit from the critical section.
    */
    void visibleBarrier();

    /** Wait until all defer functions that have been registered have been
        run.
    
        You can't call this if a guard is held, as it would deadlock waiting
        for itself to exit from the critical section.
    */
    void deferBarrier();

    void defer(std::function<void ()> work);

    typedef void (WorkFn1) (void *);
    typedef void (WorkFn2) (void *, void *);
    typedef void (WorkFn3) (void *, void *, void *);

    void defer(void (work) (void *), void * arg);
    void defer(void (work) (void *, void *), void * arg1, void * arg2);
    void defer(void (work) (void *, void *, void *), void * arg1, void * arg2, void * arg3);

    template<typename T>
    void defer(void (*work) (T *), T * arg)
    {
        defer((WorkFn1 *)work, (void *)arg);
    }

    template<typename T>
    static void doDelete(T * arg)
    {
        delete arg;
    }

    template<typename T>
    void deferDelete(T * toDelete)
    {
        if (!toDelete) return;
        defer(doDelete<T>, toDelete);
    }

    template<typename... Args>
    void doDefer(void (fn) (Args...), Args...);

    template<typename Fn, typename... Args>
    void deferBind(Fn fn, Args... args)
    {
        std::function<void ()> bound = std::bind<void>(fn, args...);
        this->defer(bound);
    }

    void dump();

protected:
    Data* data;
    /// How many bytes does data require?
    static size_t dataBytesRequired();
    /// Placement construct a data instance
    static Data* uninitializedConstructData(void * memory);
private:
    struct Deferred;
    struct DeferredList;

    GcInfo gcInfo;

    Deferred * deferred;   ///< Deferred workloads (hidden structure)

    /** Update with the new value after first checking that the current
        value is the same as the old value.  Returns true if it
        succeeded; otherwise oldValue is updated with the new old
        value.

        As part of doing this, it will calculate the correct value for
        visibleEpoch() and, if it has changed, wake up anything waiting
        on that value, and will run any deferred handlers registered for
        that value.
    */
    bool updateAtomic(Atomic & oldValue, Atomic & newValue, RunDefer runDefer);

    /** Executes any available deferred work. */
    void runDefers();

    /** Check what deferred updates need to be run and do them.  Must be
        called with deferred locked.
    */
    std::vector<DeferredList *> checkDefers();
};


/*****************************************************************************/
/* GC LOCK                                                                   */
/*****************************************************************************/

/** GcLock for use within a single process. */

struct GcLock : public GcLockBase
{
    GcLock();
    virtual ~GcLock();

    virtual void unlink();

private:
    std::unique_ptr<Data> localData;
};

} // namespace MLDB
