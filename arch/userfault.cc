/** userfault.cc
    Jeremy Barnes, 2 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Machinery for user space page faulting.
*/

#include "userfault.h"
#include "exception.h"
#include "vm.h"
#include "segv.h"

#include "mldb/base/exc_assert.h"
#include <thread>
#include <mutex>
#include <map>
#include <cstring>

#include <sys/types.h>
#include <linux/userfaultfd.h>

#include <sys/mman.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>
#include <string.h>
#include <unistd.h>

#include <sys/eventfd.h>
#include <poll.h>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* PAGE FAULT HANDLER                                                        */
/*****************************************************************************/

PageFaultHandler &
PageFaultHandler::
instance()
{
    static PageFaultHandler result;  // singleton
    return result;
}

struct PageFaultHandler::Itl {
    Itl()
    {
        // Get the userspace filesystem code
        fd = syscall(__NR_userfaultfd, O_CLOEXEC);
        if (fd == -1) {
            throw Exception("userfaultfd: %s", strerror(errno));
        }
        
        uffdio_api api;
        std::memset(&api, 0, sizeof(api));
        api.api = UFFD_API;
        api.features = 0;

        int res = ioctl(fd, UFFDIO_API, &api);
        if (res == -1) {
            throw Exception("userfaultfd ioctl UFFDIO_API: %s", strerror(errno));
        }

        wakeupfd = ::eventfd(0, 0);
        
        //service.reset(new std::thread([this] () { this->mainThread(); }));
    }

    ~Itl()
    {
        int res = eventfd_write(wakeupfd, 1);
        if (res == -1) {
            // will abort, from destructor
            throw MLDB::Exception(errno, "eventfd write()");
        }

        if (fd != -1)
            ::close(fd);
        
        entries.clear();
        if (service) {
            service->join();
            service.reset();
        }

        if (wakeupfd != -1)
            ::close(wakeupfd);
    }
    
    int fd = -1;
    int wakeupfd = -1;
    std::unique_ptr<std::thread> service;

    struct Entry {
    };

    std::recursive_mutex entriesMutex;
    std::map<const void *, Entry> entries;
    
    void mainThread()
    {
        pollfd fds[2] = {
            { fd, POLLIN, 0 },
            { wakeupfd, POLLIN, 0 } };

        for (;;) {
            int res = poll(fds, 2 /* num fds */, -1 /* timeout */);
            if (res == -1 && errno == EINTR) {
                continue;
            }
            if (res == -1) {
                // Will abort the process, since in a thread...
                throw Exception("poll: %s", strerror(errno));
            }

            // Time to finish
            if (fds[1].revents)
                break;

            if (fds[0].revents & POLLIN) {
                // Deal with a user fd
                uffd_msg msg = {0};
		int res = read(fd, &msg, sizeof(msg));
                if (res != sizeof(msg)) {
                    throw Exception("read: %s", strerror(errno));

                }
                cerr << "faulting with address " << msg.arg.pagefault.address
                     << endl;
                cerr << "with flags " << msg.arg.pagefault.flags << endl;
            } else if (fds[0].revents & POLLHUP) {
                //break;  // error or the fd was closed
            }
        }
    }
};

struct PageFaultHandler::RangeHandler::Itl {
    Itl(PageFaultHandler::Itl * owner,
        size_t numPages,
        Permissions perm,
        EventHandlers eventHandlersIn)
        : owner(owner),
          numPages(numPages),
          eventHandlers(std::move(eventHandlersIn))
    {
        mapping = allocateBackingPages(numPages, perm);
        backing = allocateBackingPages(numPages, PERM_READ_WRITE);
        
        // Associate our mapping
        uffdio_register reg;
        reg.range.start = mapping.get() - (const char *)0;
        reg.range.len = numPages * page_size;

        reg.mode = UFFDIO_REGISTER_MODE_MISSING;  // Handling missing pages

        int res = ioctl(owner->fd, UFFDIO_REGISTER, &reg);
        if (res == -1) {
            throw Exception("userfaultfd ioctl UFFDIO_REGISTER: %s",
                            strerror(errno));
        }

        if (eventHandlers.handlePageFault) {
        }
    }

    ~Itl()
    {
        uffdio_range range{uint64_t(mapping.get() - (const char *)0),
                           numPages * page_size};
        
        int res = ioctl(owner->fd, UFFDIO_UNREGISTER, &range);

        if (res == -1) {
            throw Exception("userfaultfd ioctl UFFDIO_UNREGISTER: %s",
                            strerror(errno));
        }

        if (segvRegion != -1) {
            cerr << "unregistering segv region " << segvRegion << endl;
            unregisterSegvRegion(segvRegion);
            cerr << "done unregistering segv region " << segvRegion << endl;
        }
    }
    
    PageFaultHandler::Itl * const owner;
    const size_t numPages;
    EventHandlers eventHandlers;
    std::shared_ptr<char> mapping;
    std::shared_ptr<char> backing;
    int segvRegion = -1;
    
    size_t getNumPages() const
    {
        return numPages;
    }

    size_t getPageSize() const
    {
        return page_size;
    }
        
    char * getBackingStart() const
    {
        return backing.get();
    }

    char * getBackingPage(size_t pageNumber) const
    {
        return backing.get() + (pageNumber * page_size);
    }

    const char * getMappingStart() const
    {
        return mapping.get();
    }

    const char * getMappingPage(size_t pageNumber) const
    {
        return mapping.get() + (pageNumber * page_size);
    }

    size_t getPageNumberForFaultAddress(const void * mappingAddress)
    {
        if (mappingAddress < mapping.get()
            || mappingAddress >= mapping.get() + (numPages * page_size)) {
            throw Exception("Attempt to get page number for fault address "
                            "outside of mapped range");
        }
        return ((const char *)mappingAddress - mapping.get())
            / page_size;
    }
        
    bool copyPages(size_t startPage, size_t numPages)
    {
        return copyPages(getBackingPage(startPage),
                         getMappingPage(startPage),
                         numPages * page_size);
    }

    void setExceptionOnAccess(std::function<void (const void *)> exc)
    {
        installSegvHandler();
        static bool init = false;
        if (!init) {
            init = true;
        }

        // Trampoline to call the exception, to be executed within the signal
        // handler.  This requires all of the code that may be accessing the
        // memory to have stack frames set up properly and unwind tables
        // accessible.
        auto handleSegv = [=] (const void * addr)
            {
                cerr << "handling segv" << endl;
                exc(addr);
                return false;
            };
        
        // Register the handler for segfaults within the region
        segvRegion = registerSegvRegion(getMappingStart(),
                                        getMappingPage(numPages),
                                        handleSegv);

        // We now cause a SEGV signal memory access to the range.  This
        // will trigger the handler in the segv region, which bounces to
        // the lambda above and finally throws the exception.
        failAccessWithSegv();
    }
    
    void notifyFinished()
    {
        
    }

    bool copyPages(const void * sourceAddress,
                   const void * destAddress,
                   size_t length)
    {
        //cerr << "copyPages of length " << length << endl;
        //cerr << "from " << sourceAddress << " to " << destAddress << endl;
        //cerr << "start = " << (void *)start << " end = " << (void *)end
        //     << endl;
            
        //ExcAssertGreaterEqual(destAddress, start);
        //ExcAssertLessEqual((const char *)destAddress + length, end);
        //ExcAssertEqual(length % page_size, 0);
            
        uffdio_copy copy = {
            .dst = (unsigned long)destAddress,
            .src = (unsigned long)sourceAddress,
            .len = length
        };

        int res = ioctl(owner->fd, UFFDIO_COPY, &copy);

        if (res == -1) {
            throw Exception("ioctl UFFDIO_COPY: %s",
                            strerror(errno));
        }
            
        return true;
    }

    void failAccessWithSegv()
    {
        // First, protect the range to have no valid access
        int res = mprotect((void *)getMappingStart(),
                           numPages * page_size,
                           PROT_NONE);
        
        if (res == -1) {
            throw Exception("failAccessWithSegv mprotect: %s",
                            strerror(errno));
        }

        copyPages(0, numPages);
    }

};


PageFaultHandler::
PageFaultHandler()
    : itl(new Itl())
{
}

PageFaultHandler::
~PageFaultHandler()
{
}

std::shared_ptr<char>
PageFaultHandler::
allocateBackingPages(size_t numPages, Permissions perm)
{
    size_t length = numPages * page_size;

    int flags = 0;
    if ((perm & PERM_READ) == PERM_READ)
        flags |= PROT_READ;
    if ((perm & PERM_WRITE) == PERM_WRITE)
        flags |= PROT_WRITE;
    
    void * mem = mmap(nullptr, length, flags,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1 /* fd */, 0);
    if (!mem) {
        throw Exception("mmap: %s", strerror(errno));
    }
    
    return std::shared_ptr<char>
        ((char *)mem,
         [=] (char *) { ::munmap(mem, length); });
}

std::shared_ptr<PageFaultHandler::RangeHandler>
PageFaultHandler::
addRange(size_t length, Permissions perm)
{
    EventHandlers handlers;
    return addRangeImpl(length, handlers, perm);
}

std::shared_ptr<const char>
PageFaultHandler::
addRange(size_t length, EventHandlers handlers, Permissions perm)
{
    auto rangeHandler = addRangeImpl(length, handlers, perm);
    return std::shared_ptr<const char>(rangeHandler,
                                       rangeHandler->getMappingStart());
}

std::shared_ptr<PageFaultHandler::RangeHandler>
PageFaultHandler::
addRangeImpl(size_t length, EventHandlers handlers, Permissions perm)
{
    size_t numPages = (length + page_size - 1) / page_size;
    return std::make_shared<RangeHandler>
        (new RangeHandler::Itl(itl.get(), numPages, perm, std::move(handlers)));
}

#if 0
std::shared_ptr<RangeHandler>
PageFaultHandler::
addRange(const void * start, size_t length,
         EventHandlers handlers)
{
    return std::make_shared<RangeHandler>
        (this, (const char *)start, (const char *)start + page_length);
}
#endif
    

/*****************************************************************************/
/* RANGE HANDLER                                                             */
/*****************************************************************************/

PageFaultHandler::RangeHandler::
RangeHandler(Itl * itl)
    : itl(itl)
{
}

PageFaultHandler::RangeHandler::
~RangeHandler()
{
}

size_t
PageFaultHandler::RangeHandler::
getNumPages() const
{
    return itl->getNumPages();
}

size_t
PageFaultHandler::RangeHandler::
getPageSize() const
{
    return itl->getPageSize();
}
        
char *
PageFaultHandler::RangeHandler::
getBackingStart() const
{
    return itl->getBackingStart();
}

char *
PageFaultHandler::RangeHandler::
getBackingPage(size_t pageNumber) const
{
    return itl->getBackingPage(pageNumber);
}

const char *
PageFaultHandler::RangeHandler::
getMappingStart() const
{
    return itl->getMappingStart();
}

const char *
PageFaultHandler::RangeHandler::
getMappingPage(size_t pageNumber) const
{
    return itl->getMappingPage(pageNumber);
}

size_t
PageFaultHandler::RangeHandler::
getPageNumberForFaultAddress(const void * mappingAddress)
{
    return itl->getPageNumberForFaultAddress(mappingAddress);
}

bool
PageFaultHandler::RangeHandler::
copyPages(size_t startPage, size_t numPages)
{
    return itl->copyPages(startPage, numPages);
}

void
PageFaultHandler::RangeHandler::
notifyFinished()
{
    itl->notifyFinished();
}

void
PageFaultHandler::RangeHandler::
failAccessWithSegv()
{
    itl->failAccessWithSegv();
}

void
PageFaultHandler::RangeHandler::
setExceptionOnAccess(std::function<void (const void *)> exc)
{
    itl->setExceptionOnAccess(std::move(exc));
}

} // namespace MLDB
