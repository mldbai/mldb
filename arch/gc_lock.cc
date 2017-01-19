/* gc_lock.cc
   Jeremy Barnes, 19 November 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

// Turn off inline for the gc_lock methods, so that they will be instantiated
// into the object files.
#define GC_LOCK_INLINE_OFF 1

#include "gc_lock.h"
#include "gc_lock_impl.h"
#include "mldb/arch/arch.h"
#include "mldb/arch/tick_counter.h"
#include "mldb/arch/spinlock.h"
#include "mldb/arch/futex.h"
#include "mldb/base/exc_check.h"
#include "mldb/jml/utils/guard.h"
#include <iterator>
#include <iostream>
#include <map>
#include <cstring>


using namespace std;
using namespace ML;

namespace MLDB {

/*****************************************************************************/
/* Utility                                                                   */
/*****************************************************************************/

/** Externally visible initializer for the GcLock's epoch which can be used to
    test for overflows.
*/
int32_t gcLockStartingEpoch = 0;

int32_t SpeculativeThreshold = 5;

/** A safe comparaison of epochs that deals with potential overflows.
    returns 0 if equal, -1 if a is earlier than b, or 1 if a is greater than b
    \todo So many possible bit twiddling hacks... Must resist...
*/
template<typename T, typename T2, size_t Bits = sizeof(T)*8>
static inline
int
compareEpochs (T a, T2 b)
{
    static_assert(Bits >= 2, "Bits is too low to guarantee isolation");

    if (a == b) return 0;

    enum { MASK = (3ULL << (Bits - 2)) };

    // We use the last 2 bits for overflow detection.
    //   We don't use T to avoid problems with the sign bit.
    const uint64_t aMasked = a & MASK;
    const uint64_t bMasked = b & MASK;

    // Normal case.
    if (aMasked == bMasked) return a < b ? -1 : 1;

    // Check for overflow.
    else if (aMasked == 0) return bMasked == MASK ? 1 : -1;
    else if (bMasked == 0) return aMasked == MASK ? -1 : 1;

    // No overflow so just compare the masks.
    return aMasked < bMasked ? -1 : 1;
}


/*****************************************************************************/
/* GC LOCK BASE                                                              */
/*****************************************************************************/

struct DeferredEntry1 {
    DeferredEntry1(void (fn) (void *) = 0, void * data = 0)
        : fn(fn), data(data)
    {
    }

    void run()
    {
        fn(data);
    }
        
    void (*fn) (void *);
    void * data;
};

struct DeferredEntry2 {
    DeferredEntry2(void (fn) (void *, void *) = 0, void * data1 = 0,
                   void * data2 = 0)
        : fn(fn), data1(data1), data2(data2)
    {
    }
        
    void run()
    {
        fn(data1, data2);
    }


    void (*fn) (void *, void *);
    void * data1;
    void * data2;
};

struct DeferredEntry3 {
    DeferredEntry3(void (fn) (void *, void *, void *) = 0, void * data1 = 0,
                   void * data2 = 0, void * data3 = 0)
        : fn(fn), data1(data1), data2(data2), data3(data3)
    {
    }
        
    void run()
    {
        fn(data1, data2, data3);
    }


    void (*fn) (void *, void *, void *);
    void * data1;
    void * data2;
    void * data3;
};


/// Data about each epoch
struct GcLockBase::DeferredList {
    DeferredList()
    {
    }

    ~DeferredList()
    {
        //if (lock.locked())
        //    throw MLDB::Exception("deleting deferred in locked condition");
        if (size() != 0) {
            cerr << "deleting non-empty deferred with " << size()
                 << " entries" << endl;
            //throw MLDB::Exception("deleting non-empty deferred");
        }
    }

    void swap(DeferredList & other)
    {
        //ExcAssertEqual(lock.locked(), 0);
        //ExcAssertEqual(other.lock.locked(), 0);

        //std::lock_guard<Spinlock> guard(lock);
        //std::lock_guard<Spinlock> guard2(other.lock);

        deferred1.swap(other.deferred1);
        deferred2.swap(other.deferred2);
        deferred3.swap(other.deferred3);
    }

    std::vector<DeferredEntry1> deferred1;
    std::vector<DeferredEntry2> deferred2;
    std::vector<DeferredEntry3> deferred3;
    //mutable Spinlock lock;

    bool addDeferred(int forEpoch, void (fn) (void *), void * data)
    {
        //std::lock_guard<Spinlock> guard(lock);
        deferred1.push_back(DeferredEntry1(fn, data));
        return true;
    }

    bool addDeferred(int forEpoch, void (fn) (void *, void *),
                     void * data1, void * data2)
    {
        //std::lock_guard<Spinlock> guard(lock);
        deferred2.push_back(DeferredEntry2(fn, data1, data2));
        return true;
    }

    bool addDeferred(int forEpoch, void (fn) (void *, void *, void *),
                     void * data1, void * data2, void * data3)
    {
        //std::lock_guard<Spinlock> guard(lock);
        deferred3.push_back(DeferredEntry3(fn, data1, data2, data3));
        return true;
    }
        
    size_t size() const
    {
        //std::lock_guard<Spinlock> guard(lock);
        return deferred1.size() + deferred2.size() + deferred3.size();
    }

    void runAll()
    {
        // Spinlock should be unnecessary...
        //std::lock_guard<Spinlock> guard(lock);

        for (unsigned i = 0;  i < deferred1.size();  ++i) {
            try {
                deferred1[i].run();
            } catch (...) {
            }
        }

        deferred1.clear();

        for (unsigned i = 0;  i < deferred2.size();  ++i) {
            try {
                deferred2[i].run();
            } catch (...) {
            }
        }

        deferred2.clear();

        for (unsigned i = 0;  i < deferred3.size();  ++i) {
            try {
                deferred3[i].run();
            } catch (...) {
            }
        }

        deferred3.clear();
    }
};

struct GcLockBase::Deferred {
    mutable Spinlock lock;
    std::map<int32_t, DeferredList *> entries;
    std::vector<DeferredList *> spares;

    bool empty() const
    {
        std::lock_guard<Spinlock> guard(lock);
        return entries.empty();
    }
};

std::string
GcLockBase::ThreadGcInfoEntry::
print() const
{
    return MLDB::format("inEpoch: %d, readLocked: %d, writeLocked: %d",
                      inEpoch, readLocked, writeLocked);
}


/*****************************************************************************/
/* THREAD GC INFO ENTRY                                                      */
/*****************************************************************************/

GcLockBase::ThreadGcInfoEntry::
ThreadGcInfoEntry()
    : inEpoch(-1), readLocked(0), writeLocked(0),
      specLocked(0), specUnlocked(0),
      owner(0)
{
}

GcLockBase::ThreadGcInfoEntry::
~ThreadGcInfoEntry()
{
    /* We are not in a speculative critical section, check if
     * Gc has been left locked
     */
    if (!specLocked && !specUnlocked && (readLocked || writeLocked)) {
        ::fprintf(stderr, "Thread diad but GcLock is still locked");
        std::terminate();
    }

    /* We are in a speculative CS but Gc has not beed unlocked
     */
    else if (!specLocked && specUnlocked) {
        unlockShared(RD_YES);
        specUnlocked = 0;
    }
           
} 


/*****************************************************************************/
/* ATOMIC                                                                    */
/*****************************************************************************/

struct GcLockBase::Atomic {
    Atomic();

    Atomic(const Atomic & other);

    Atomic & operator = (const Atomic & other);

    bool operator == (const Atomic & other) const
    {
        return bits == other.bits;
    }

    bool operator != (const Atomic & other) const
    {
        return ! operator == (other);
    }

    bool compareExchange(Atomic & oldValue, const Atomic & newValue)
    {
        return atomicBits.compare_exchange_strong
            (oldValue.bits, newValue.bits,
             std::memory_order_seq_cst);
    }

    /** Human readable string. */
    std::string print() const;

    static constexpr uint16_t EXCLUSIVE_MASK = 0x8000;
    static constexpr uint16_t IN_MASK        = 0x7fff;

    bool anyInCurrent() const { return in[epoch & 1] & IN_MASK; }
    bool anyInOld() const { return in[(epoch - 1)&1] & IN_MASK; }

    bool exclusive() const { return in[0] & EXCLUSIVE_MASK; }

    void setExclusive()
    {
        in[0] |= EXCLUSIVE_MASK;
    }

    void resetExclusive()
    {
        in[0] &= IN_MASK;
    }

    void resetExclusiveAtomic()
    {
        uint16_t oldExclusive = in[0]
            .fetch_and(IN_MASK, std::memory_order_seq_cst);
        ExcAssert(oldExclusive & EXCLUSIVE_MASK);
    }

    // Atomically decrement the in counter for the given epoch, and
    // return the in value before the counter was decremented.  If this
    // returns 1, it means that we are the last one to leave this epoch.
    uint16_t decrementInAtomic(uint32_t epoch)
    {
        return in[epoch & 1]
            .fetch_add(-1, std::memory_order_seq_cst) & IN_MASK;
    }

    void setIn(int32_t epoch, int val)
    {
        in[epoch & 1] = (in[epoch & 1] & EXCLUSIVE_MASK) | (val & IN_MASK);
    }

    void addIn(int32_t epoch, int val)
    {
        in[epoch & 1] += val;
    }

    /** Check that the invariants all hold.  Throws an exception if not. */
    void validate() const;

    typedef uint32_t epoch_t;

    epoch_t visibleEpoch() const
    {
        // Set the visible epoch
        if (!anyInCurrent() && !anyInOld())
            return epoch;
        else if (!anyInOld())
            return epoch - 1;
        else return epoch - 2;
    }

    volatile union {
        struct {
            epoch_t epoch;         ///< Current epoch number (could be smaller).
            std::atomic<uint16_t> in[2];         ///< How many threads in each epoch?  High bit of number 0 means exclusive
        };
        uint64_t bits;
        std::atomic<uint64_t> atomicBits;
    };
};

struct GcLockBase::Data {
    Data()
        : exclusiveFutex(0)
    {
        visibleFutex.store(atomic.visibleEpoch());
    }

    Atomic atomic;
   
    std::atomic<int32_t> visibleFutex;
    std::atomic<int32_t> exclusiveFutex;

    /** Human readable string. */
    std::string print() const;
};

inline GcLockBase::Atomic::
Atomic()
{
    std::memset((void *)this, 0, sizeof(*this));
    epoch = gcLockStartingEpoch; // makes it easier to test overflows.
}

inline GcLockBase::Atomic::
Atomic(const Atomic & other)
{
    bits = other.bits;
}

inline GcLockBase::Atomic &
GcLockBase::Atomic::
operator = (const Atomic & other)
{
    bits = other.bits;
    return *this;
}

void
GcLockBase::Atomic::
validate() const
{
}

std::string
GcLockBase::Atomic::
print() const
{
    return MLDB::format("epoch: %d, in: %d, in-1: %d, visible: %d, exclusive: %d",
                      epoch, anyInCurrent(), anyInOld(), visibleEpoch(),
                      (int)exclusive());
}

std::string
GcLockBase::Data::
print() const
{
    return atomic.print();
}

GcLockBase::
GcLockBase()
{
    deferred = new Deferred();
}

GcLockBase::
~GcLockBase()
{
    if (!deferred->empty()) {
        dump();
    }

    delete deferred;
}

bool
GcLockBase::
updateAtomic(Atomic & oldValue, Atomic & newValue, RunDefer runDefer)
{
    bool wake;
    try {
        ExcAssertGreaterEqual(compareEpochs(newValue.epoch, oldValue.epoch), 0);
        wake = newValue.visibleEpoch() != oldValue.visibleEpoch();
    } catch (...) {
        cerr << "update: oldValue = " << oldValue.print() << endl;
        cerr << "newValue = " << newValue.print() << endl;
        throw;
    }

    newValue.validate();

#if 0
    // Do an extra check before we assert lock
    Atomic upToDate = data->atomic;
    if (upToDate != oldValue) {
        oldValue = upToDate;
        return false;
    }
#endif

    if (!data->atomic.compareExchange(oldValue, newValue))
        return false;

    if (wake) {
        // We updated the current visible epoch.  We can now wake up
        // anything that was waiting for it to be visible and run any
        // deferred handlers.
        
        // TODO TODO TODO
        // There can be races here.  We need to make sure that we
        // publish the highest visible epoch, which can change
        // uncontrollably.
        ++data->visibleFutex;
        //data->visibleFutex = newValue.visibleEpoch();
        futex_wake(data->visibleFutex);
        if (runDefer) {
            runDefers();
        }
    }

    return true;
}

void
GcLockBase::
runDefers()
{
    std::vector<DeferredList *> toRun;
    {
        std::lock_guard<Spinlock> guard(deferred->lock);
        toRun = checkDefers();
    }

    for (unsigned i = 0;  i < toRun.size();  ++i) {
        toRun[i]->runAll();
        delete toRun[i];
    }
}

std::vector<GcLockBase::DeferredList *>
GcLockBase::
checkDefers()
{
    std::vector<DeferredList *> result;

    while (!deferred->entries.empty() &&
            compareEpochs(
                    deferred->entries.begin()->first,
                    data->atomic.visibleEpoch()) <= 0)
    {
        result.reserve(deferred->entries.size());

        for (auto it = deferred->entries.begin(),
                 end = deferred->entries.end();
             it != end;  /* no inc */) {

            if (compareEpochs(it->first, data->atomic.visibleEpoch()) > 0)
                break;  // still visible

            ExcAssert(it->second);
            result.push_back(it->second);
            //it->second->runAll();
            auto toDelete = it;
            it = std::next(it);
            deferred->entries.erase(toDelete);
        }
    }

    return result;
}

void
GcLockBase::
enterCS(ThreadGcInfoEntry * entry, RunDefer runDefer)
{
    if (!entry) entry = &getEntry();
        
    ExcAssertEqual(entry->inEpoch, -1);

    Atomic current = data->atomic;

    for (;;) {
        Atomic newValue = current;

        if (newValue.exclusive()) {
            // We don't check the error, as a spurious wakeup will just
            // make the loop continue.
            futex_wait(data->exclusiveFutex, 1);
            current = data->atomic;
            continue;
        }

        if (newValue.anyInOld() == 0) {
            // We're entering a new epoch
            newValue.epoch += 1;
            newValue.setIn(newValue.epoch, 1);
        }
        else {
            // No new epoch as the old one isn't finished yet
            newValue.addIn(newValue.epoch, 1);
        }

        entry->inEpoch = newValue.epoch & 1;
            
        if (updateAtomic(current, newValue, runDefer)) break;
    }
}

void
GcLockBase::
exitCS(ThreadGcInfoEntry * entry, RunDefer runDefer /* = true */)
{
    if (entry->inEpoch == -1)
        throw MLDB::Exception("not in a CS");

    ExcCheck(entry->inEpoch == 0 || entry->inEpoch == 1,
            "Invalid inEpoch");

#if 0
    // Fast path
    if (data->atomic.decrementInAtomic(entry->inEpoch) > 1) {
        entry->inEpoch = -1;
        return;
    }
#endif

    Atomic current = data->atomic;

    // Slow path; an epoch may have come to an end

    for (;;) {
        Atomic newValue = current;
        newValue.decrementInAtomic(entry->inEpoch);
        if (updateAtomic(current, newValue, runDefer)) break;
    }

    entry->inEpoch = -1;
}

void
GcLockBase::
enterCSExclusive(ThreadGcInfoEntry * entry)
{
    ExcAssertEqual(entry->inEpoch, -1);

    Atomic current = data->atomic, newValue;

    for (;;) {
        if (current.exclusive()) {
            futex_wait(data->exclusiveFutex, 1);
            current = data->atomic;
            continue;
        }

        ExcAssertEqual(current.exclusive(), 0);

        newValue = current;
        newValue.setExclusive();
        if (updateAtomic(current, newValue, RD_YES)) {
            current = newValue;
            break;
        }
    }

    ExcAssertEqual(data->atomic.exclusive(), 1);
    data->exclusiveFutex.store(1, std::memory_order_release);

    // At this point, we have exclusive access... now wait for everything else
    // to exit.  This is kind of a critical section barrier.
    int startEpoch = current.epoch;
    
    visibleBarrier();
    
    ExcAssertEqual(data->atomic.epoch, startEpoch);


#if 0
    // Testing
    for (unsigned i = 0;  i < 100;  ++i) {
        Atomic current = data->atomic;

        try {
            ExcAssertEqual(current.exclusive(), 1);
            ExcAssertEqual(current.anyInCurrent(), 0);
            ExcAssertEqual(current.anyInOld(), 0);
        } catch (...) {
            ThreadGcInfoEntry & entry = getEntry();
            cerr << "entry->inEpoch = " << entry->inEpoch << endl;
            cerr << "entry->readLocked = " << entry->readLocked << endl;
            cerr << "entry->writeLocked = " << entry->writeLocked << endl;
            cerr << "current: " << current.print() << endl;
            cerr << "data: " << data->print() << endl;
            throw;
        }
    }
#endif

    ExcAssertEqual(data->atomic.epoch, startEpoch);

    entry->inEpoch = startEpoch & 1;
}

void
GcLockBase::
exitCSExclusive(ThreadGcInfoEntry * entry)
{
    if (!entry) entry = &getEntry();
#if 0
    Atomic current = data->atomic;

    try {
        ExcAssertEqual(current.exclusive(), 1);
        ExcAssertEqual(current.anyInCurrent(), 0);
        ExcAssertEqual(current.anyInOld(), 0);
    } catch (...) {
        cerr << "entry->inEpoch = " << entry->inEpoch << endl;
        cerr << "entry->readLocked = " << entry->readLocked << endl;
        cerr << "entry->writeLocked = " << entry->writeLocked << endl;
        cerr << "current: " << current.print() << endl;
        cerr << "data: " << data->print() << endl;
        throw;
    }
#endif
    data->atomic.resetExclusiveAtomic();
    
    // Wake everything waiting on the exclusive lock
    data->exclusiveFutex.store(0, std::memory_order_release);
    futex_wake(data->exclusiveFutex);
    
    entry->inEpoch = -1;
}

void
GcLockBase::
visibleBarrier()
{
    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    ThreadGcInfoEntry & entry = getEntry();

    if (entry.inEpoch != -1)
        throw MLDB::Exception("visibleBarrier called in critical section will "
                            "deadlock");

    Atomic current = data->atomic;
    int startEpoch = data->atomic.epoch;
    
    // Spin until we're visible
    for (unsigned i = 0;  ;  ++i, current = data->atomic) {
        
        //int i = startEpoch & 1;

        // Have we moved on?  If we're 2 epochs ahead we're surely not visible
        if (current.epoch != startEpoch && current.epoch != startEpoch + 1) {
            //cerr << "epoch moved on" << endl;
            return;
        }

        // If there's nothing in a critical section then we're OK
        if (current.anyInCurrent() == 0 && current.anyInOld() == 0)
            return;

        if (current.visibleEpoch() == startEpoch)
            return;

        if (i % 128 == 127 || true) {
            // Note that we don't care about the actual value of the visible
            // epoch, only that it's different from the current one
            long res = futex_wait(data->visibleFutex, current.visibleEpoch());
            if (res == -1) {
                if (errno == EAGAIN || errno == EINTR) continue;
                throw MLDB::Exception(errno, "futex_wait");
            }
        }
    }
}

void
GcLockBase::
deferBarrier()
{
    // TODO: what is the interaction between a defer barrier and an exclusive
    // lock?

    ThreadGcInfoEntry & entry = getEntry();

    visibleBarrier();

    // Do it twice to make sure that everything is cycled over two different
    // epochs
    for (unsigned i = 0;  i < 2;  ++i) {
        
        // If we're in a critical section, we'll wait forever...
        ExcAssertEqual(entry.inEpoch, -1);
        
        // What does "defer barrier" mean?  It means that we wait until everything
        // that is currently enqueued to be deferred is finished.
        
        // TODO: this is a very inefficient implementation... we could do a lot
        // better especially in the non-contended case
        
        union {
            std::atomic<int> a;
            int i;
        } lock;
        
        lock.i = 0;

        defer(futex_unlock, &lock.i);
        
        lock.a -= 1;
        
        futex_wait(lock.i, -1);
    }

    // If certain threads aren't allowed to execute deferred work
    // then it's possible that not all deferred work will have been executed.
    // To be sure, we run any leftover work.
    runDefers();
}

/** Helper function to call an arbitrary std::function passed through with a void * */
static void callFn(void * arg)
{
    std::function<void ()> * fn
        = reinterpret_cast<std::function<void ()> *>(arg);
    try {
        (*fn)();
    } catch (...) {
        delete fn;
        throw;
    }
    delete fn;
}

void
GcLockBase::
defer(std::function<void ()> work)
{
    defer(callFn, new std::function<void ()>(work));
}

template<typename... Args>
void
GcLockBase::
doDefer(void (fn) (Args...), Args... args)
{
    // INVARIANT
    // If there is another thread that is provably in a critical section at
    // this moment, then the function will only be run when all such threads
    // have exited the critical section.
    //
    // If there are no threads in either the current or the old epoch, then
    // we can run it straight away.
    //
    // If there are threads in the old epoch but not the current epoch, then
    // we need to wait until all threads have exited the old epoch.  In other
    // words, it goes on the old epoch's defer queue.
    //
    // If there are threads in the current epoch (irrespective of the old
    // epoch) then we need to wait until the current epoch is done.

    Atomic current = data->atomic;

    int32_t newestVisibleEpoch = current.epoch;
    if (current.anyInCurrent() == 0) --newestVisibleEpoch;

#if 1
    // Nothing is in a critical section; we can run it inline
    if (current.anyInCurrent() + current.anyInOld() == 0) {
        fn(std::forward<Args>(args)...);
        return;
    }
#endif

    for (int i = 0; i == 0; ++i) {
        // Lock the deferred structure
        std::lock_guard<Spinlock> guard(deferred->lock);

#if 1
        // Get back to current again
        current = data->atomic;

        // Find the oldest live epoch
        int oldestLiveEpoch = -1;
        if (current.anyInOld() > 0)
            oldestLiveEpoch = current.epoch - 1;
        else if (current.anyInCurrent() > 0)
            oldestLiveEpoch = current.epoch;
    
        if (oldestLiveEpoch == -1 || 
                compareEpochs(oldestLiveEpoch, newestVisibleEpoch) > 0)
        {
            // Nothing in a critical section so we can run it now and exit
            break;
        }
    
        // Nothing is in a critical section; we can run it inline
        if (current.anyInCurrent() + current.anyInOld() == 0)
            break;
#endif

        // OK, get the deferred list
        auto epochIt
            = deferred->entries.insert
            (make_pair(newestVisibleEpoch, (DeferredList *)0)).first;
        if (epochIt->second == 0) {
            // Create a new list
            epochIt->second = new DeferredList();
        }
        
        DeferredList & list = *epochIt->second;
        list.addDeferred(newestVisibleEpoch, fn, std::forward<Args>(args)...);

        // TODO: we only need to do this if the newestVisibleEpoch has
        // changed since we last calculated it...
        //checkDefers();

        return;
    }
    
    // If we got here we can run it straight away
    fn(std::forward<Args>(args)...);
    return;
}

void
GcLockBase::
defer(void (work) (void *), void * arg)
{
    doDefer(work, arg);
}

void
GcLockBase::
defer(void (work) (void *, void *), void * arg1, void * arg2)
{
    doDefer(work, arg1, arg2);
}

void
GcLockBase::
defer(void (work) (void *, void *, void *), void * arg1, void * arg2, void * arg3)
{
    doDefer(work, arg1, arg2, arg3);
}

void
GcLockBase::
dump()
{
    Atomic current = data->atomic;
    cerr << "epoch " << current.epoch << " in " << current.anyInCurrent()
         << " in-1 " << current.anyInOld() << " vis " << current.visibleEpoch()
         << " excl " << current.exclusive() << endl;
    cerr << "deferred: ";
    {
        std::lock_guard<Spinlock> guard(deferred->lock);
        cerr << deferred->entries.size() << " epochs: ";
        
        for (auto it = deferred->entries.begin(), end = deferred->entries.end();
             it != end;  ++it) {
            cerr << " " << it->first << " (" << it->second->size()
                 << " entries)";
        }
    }
    cerr << endl;
}

int
GcLockBase::
currentEpoch() const
{
    return data->atomic.epoch;
}

bool
GcLockBase::
isLockedByAnyThread() const
{
    return data->atomic.in[0] || data->atomic.in[1] ;
}

size_t 
GcLockBase::
dataBytesRequired()
{
    return sizeof(Data);
}

GcLockBase::Data *
GcLockBase::
uninitializedConstructData(void * memory)
{
    return new (memory) Data();
}

/*****************************************************************************/
/* GC LOCK                                                                   */
/*****************************************************************************/

GcLock::
GcLock()
    : localData(new Data())
{
    data = localData.get();
}

GcLock::
~GcLock()
{
    // Nothing to cleanup.
}

void
GcLock::
unlink()
{
    // Nothing to cleanup.
}



} // namespace MLDB

