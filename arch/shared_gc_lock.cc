/* gc_lock.cc
   Jeremy Barnes, 19 November 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "shared_gc_lock.h"
#include "gc_lock_impl.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

using namespace std;
namespace ipc = boost::interprocess;


namespace MLDB {

/*****************************************************************************/
/* SHARED GC LOCK                                                            */
/*****************************************************************************/

// We want to mmap the file so it has to be the size of a page.

namespace { size_t GcLockFileSize = 1ULL << 12; }


GcCreate GC_CREATE; ///< Open and initialize a new gcource.
GcOpen GC_OPEN;     ///< Open an existing gcource.

void
SharedGcLock::
doOpen(bool create)
{
    int flags = O_RDWR | O_CREAT;
    if (create) flags |= O_EXCL;

    ipc::named_mutex mutex(ipc::open_or_create, name.c_str());
    {
        // Lock is used to create and truncate the file atomically.
        ipc::scoped_lock<ipc::named_mutex> lock(mutex);

        // We don't want the locks to be persisted so an shm_open will do fine.
        fd = shm_open(name.c_str(), flags, 0644);
        ExcCheckErrno(fd >= 0, "shm_open failed");

        struct stat stats;
        int res = fstat(fd, &stats);
        ExcCheckErrno(!res, "failed to get the file size");

        if (stats.st_size != GcLockFileSize) {
            int res = ftruncate(fd, GcLockFileSize);
            ExcCheckErrno(!res, "failed to resize the file.");
        }
    }

    // Map the region so that all the processes can see the writes.
    addr = mmap(0, GcLockFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ExcCheckErrno(addr != MAP_FAILED, "failed to map the shm file");

    // Initialize and set the member used by GcLockBase.
    if (create) uninitializedConstructData(addr);
    data = reinterpret_cast<Data*>(addr);
}

SharedGcLock::
SharedGcLock(GcCreate, const string& name) :
    name("gc." + name)
{
    doOpen(true);
}

SharedGcLock::
SharedGcLock(GcOpen, const string& name) :
    name("gc." + name)
{
    doOpen(false);
}

SharedGcLock::
~SharedGcLock()
{
    munmap(addr, GcLockFileSize);
    close(fd);
}

void
SharedGcLock::
unlink()
{
    shm_unlink(name.c_str());
    (void) ipc::named_mutex::remove(name.c_str());
}

} // namespace MLDB
