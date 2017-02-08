/* gc_lock.h                                                       -*- C++ -*-
   Jeremy Barnes, 19 November 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   GcLock that can be shared amongst multiple processes on a machine with
   a coherent memory architecture.
*/

#pragma once

#include "gc_lock.h"

namespace MLDB {

/*****************************************************************************/
/* SHARED GC LOCK                                                            */
/*****************************************************************************/

/** Constants that can be used to control how resources are opened.
    Note that these are structs so we can more easily overload constructors.
*/
extern struct GcCreate {} GC_CREATE; ///< Open and initialize a new resource.
extern struct GcOpen {} GC_OPEN;     ///< Open an existing resource.


/** GcLock to be shared among multiple processes. */

struct SharedGcLock : public GcLockBase
{
    SharedGcLock(GcCreate, const std::string& name);
    SharedGcLock(GcOpen, const std::string& name);
    virtual ~SharedGcLock();

    /** Permanently deletes any resources associated with the gc lock. */
    virtual void unlink();

private:

    /** mmap an shm file into memory and set the data member of GcLock. */
    void doOpen(bool create);

    std::string name;
    int fd;
    void* addr;

};

} // namespace MLDB
