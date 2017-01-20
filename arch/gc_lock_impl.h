/* gc_lock_impl.h
   Jeremy Barnes, 19 November 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Inline method definitions for gc_lock_impl.h
*/


#pragma once

#include "gc_lock.h"
#include <iostream>

#define GC_LOCK_DEBUG 0

#ifndef GC_LOCK_INLINE_OFF
# define GC_LOCK_INLINE inline
#else
# define GC_LOCK_INLINE 
#endif

namespace MLDB {


/*****************************************************************************/
/* THREAD GC INFO ENTRY                                                      */
/*****************************************************************************/

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
init(const GcLockBase * const self)
{
    if (!owner) 
        owner = const_cast<GcLockBase *>(self);
}
                
GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
lockShared(RunDefer runDefer)
{
    if (!readLocked && !writeLocked)
        owner->enterCS(this, runDefer);

    ++readLocked;
}

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
unlockShared(RunDefer runDefer)
{
    if (readLocked <= 0)
        throw MLDB::Exception("Bad read lock nesting");

    --readLocked;
    if (!readLocked && !writeLocked) 
        owner->exitCS(this, runDefer);
}

GC_LOCK_INLINE bool
GcLockBase::ThreadGcInfoEntry::
isLockedShared()
{
    return readLocked + writeLocked;
}

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
lockExclusive()
{
    if (!writeLocked)
        owner->enterCSExclusive(this);
            
    ++writeLocked;
}

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
unlockExclusive()
{
    if (writeLocked <= 0)
        throw MLDB::Exception("Bad write lock nesting");

    --writeLocked;
    if (!writeLocked)
        owner->exitCSExclusive(this);
}

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
lockSpeculative(RunDefer runDefer)
{
    if (!specLocked && !specUnlocked) 
        lockShared(runDefer);

    ++specLocked;
}

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
unlockSpeculative(RunDefer runDefer)
{
    if (!specLocked) 
        throw MLDB::Exception("Bad speculative lock nesting");

    --specLocked;
    if (!specLocked) {
        if (++specUnlocked == SpeculativeThreshold) {
            unlockShared(runDefer);
            specUnlocked = 0;
        }
    }
}

GC_LOCK_INLINE void
GcLockBase::ThreadGcInfoEntry::
forceUnlock(RunDefer runDefer)
{
    ExcCheckEqual(specLocked, 0, "Bad forceUnlock call");

    if (specUnlocked) {
        unlockShared(runDefer);
        specUnlocked = 0;
    }
}


/*****************************************************************************/
/* GC LOCK BASE                                                              */
/*****************************************************************************/


GC_LOCK_INLINE void
GcLockBase::
lockShared(GcInfo::PerThreadInfo * info,
           RunDefer runDefer)
{
    ThreadGcInfoEntry & entry = getEntry(info);

    entry.lockShared(runDefer);

#if GC_LOCK_DEBUG
    using namespace std;
    cerr << "lockShared "
         << this << " index " << index
         << ": now " << entry.print() << " data "
         << data->print() << endl;
#endif
}

GC_LOCK_INLINE void
GcLockBase::
unlockShared(GcInfo::PerThreadInfo * info, 
             RunDefer runDefer)
{
    ThreadGcInfoEntry & entry = getEntry(info);

    entry.unlockShared(runDefer);

#if GC_LOCK_DEBUG
    using namespace std;
    cerr << "unlockShared "
         << this << " index " << index
         << ": now " << entry.print() << " data "
         << data->print() << endl;
#endif
}

GC_LOCK_INLINE void
GcLockBase::
lockSpeculative(GcInfo::PerThreadInfo * info,
                RunDefer runDefer)
{
    ThreadGcInfoEntry & entry = getEntry(info); 

    entry.lockSpeculative(runDefer);
}

GC_LOCK_INLINE void
GcLockBase::
unlockSpeculative(GcInfo::PerThreadInfo * info,
                  RunDefer runDefer)
{
    ThreadGcInfoEntry & entry = getEntry(info);

    entry.unlockSpeculative(runDefer);
}

GC_LOCK_INLINE void
GcLockBase::
forceUnlock(GcInfo::PerThreadInfo * info,
            RunDefer runDefer)
{
    ThreadGcInfoEntry & entry = getEntry(info);

    entry.forceUnlock(runDefer);
}
        
GC_LOCK_INLINE int
GcLockBase::
isLockedShared(GcInfo::PerThreadInfo * info) const
{
    ThreadGcInfoEntry & entry = getEntry(info);

    return entry.isLockedShared();
}

GC_LOCK_INLINE int
GcLockBase::
lockedInEpoch(GcInfo::PerThreadInfo * info) const
{
    ThreadGcInfoEntry & entry = getEntry(info);

    return entry.inEpoch;
}

GC_LOCK_INLINE void
GcLockBase::
lockExclusive(GcInfo::PerThreadInfo * info)
{
    ThreadGcInfoEntry & entry = getEntry(info);

    entry.lockExclusive();
#if GC_LOCK_DEBUG
    using namespace std;
    cerr << "lockExclusive "
         << this << " index " << index
         << ": now " << entry.print() << " data "
         << data->print() << endl;
#endif
}

GC_LOCK_INLINE void
GcLockBase::
unlockExclusive(GcInfo::PerThreadInfo * info)
{
    ThreadGcInfoEntry & entry = getEntry(info);

    entry.unlockExclusive();

#if GC_LOCK_DEBUG
    using namespace std;
    cerr << "unlockExclusive"
         << this << " index " << index
         << ": now " << entry.print()
         << " data " << data->print() << endl;
#endif
}

GC_LOCK_INLINE int
GcLockBase::
myEpoch(GcInfo::PerThreadInfo * threadInfo) const
{
    return getEntry(threadInfo).inEpoch;
}

GC_LOCK_INLINE int
GcLockBase::
isLockedExclusive(GcInfo::PerThreadInfo * info) const
{
    ThreadGcInfoEntry & entry = getEntry(info);

    return entry.writeLocked;
}

} // namespace MLDB
