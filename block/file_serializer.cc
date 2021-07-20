/** file_serialier.cc                                              -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into the filesystem.
*/

#include "file_serializer.h"
#include "memory_region_impl.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/arch/vm.h"

#include <mutex>

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>



using namespace std;

namespace MLDB {


/*****************************************************************************/
/* FILE SERIALIZER                                                           */
/*****************************************************************************/

struct FileSerializer::Itl {
    Itl(Utf8String filename_)
        : filename(std::move(filename_))
    {
        fd = open(filename.rawData(), O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            throw AnnotatedException
                (400, "Failed to open memory map file: "
                 + string(strerror(errno)));
        }
    }
    
    ~Itl()
    {
        if (arenas.empty())
            return;

        commit();

        arenas.clear();

        ::close(fd);
    }

    std::shared_ptr<void>
    allocateWritable(uint64_t bytesRequired, size_t alignment)
    {
        std::unique_lock<std::mutex> guard(mutex);
        return allocateWritableImpl(bytesRequired, alignment);
    }

    void commit()
    {
        std::unique_lock<std::mutex> guard(mutex);
        if (arenas.empty())
            return;

        size_t realLength = arenas.back().startOffset + arenas.back().currentOffset;

        int res = ::ftruncate(fd, realLength);
        if (res == -1) {
            throw AnnotatedException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }
    }

    std::shared_ptr<void>
    allocateWritableImpl(uint64_t bytesRequired, size_t alignment)
    {
        if (bytesRequired == 0)
            return nullptr;

        if (arenas.empty()) {
            createNewArena(bytesRequired + alignment);
        }
        
        void * allocated = nullptr;
        
        while ((allocated = arenas.back().allocate(bytesRequired, alignment)) == nullptr) {
            if (!expandLastArena(bytesRequired + alignment)) {
                createNewArena(bytesRequired + alignment);
            }
        }

        ExcAssertEqual(((size_t)allocated) % alignment, 0);
        
#if 0
        const char * cp = (const char *)allocated;
        
        for (size_t i = 0;  i < bytesRequired;  ++i) {
            ExcAssertEqual(cp[i], 0);
        }
#endif

        return std::shared_ptr<void>(allocated, [] (void *) {});
    }

    void createNewArena(size_t bytesRequired)
    {
        verifyLength();

        size_t numPages
            = std::max<size_t>
            ((bytesRequired + page_size - 1) / page_size,
             1024);
        // Make sure we grow geometrically (doubling every 4 updates) to
        // amortize overhead.
        numPages = std::max<size_t>
            (numPages, (currentlyAllocated + page_size - 1) / page_size / 8);

        size_t newLength = numPages * page_size;
        
        cerr << "new arena with " << newLength * 0.000001 << " MB" << endl;
        
        int res = ::ftruncate(fd, currentlyAllocated + newLength);
        if (res == -1) {
            throw AnnotatedException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }

        void * addr = mmap(nullptr, newLength,
                           PROT_READ | PROT_WRITE, MAP_SHARED,
                           fd, currentlyAllocated);
        if (addr == MAP_FAILED) {
            throw AnnotatedException
                (400, "Failed to open memory map file: "
                 + string(strerror(errno)));
        }

        arenas.emplace_back(addr, currentlyAllocated, newLength);

        currentlyAllocated += newLength;

        verifyLength();
    }

    void verifyLength() const
    {
        struct stat st;
        int res = fstat(fd, &st);
        if (res == -1) {
            throw AnnotatedException(500, "fstat");
        }
        ExcAssertEqual(st.st_size, currentlyAllocated);
    }

    bool expandLastArena(size_t bytesRequired)
    {
#if __linux__
        verifyLength();

        size_t newLength
            = arenas.back().length
            + std::max<size_t>((bytesRequired + page_size - 1) / page_size,
                               10000 * page_size);
        
        cerr << "expanding from " << arenas.back().length
             << " to " << newLength << endl;

        int res = ::ftruncate(fd, currentlyAllocated + newLength - arenas.back().length);
        if (res == -1) {
            throw AnnotatedException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }

        void * newAddr = mremap(arenas.back().addr,
                                arenas.back().length,
                                newLength,
                                0 /* flags */);

        if (newAddr != arenas.back().addr) {
            cerr << "expansion failed with " << strerror(errno) << endl;
            cerr << "newAddr = " << newAddr << endl;
            cerr << "arenas.back().addr = " << arenas.back().addr << endl;
            cerr << "wasting " << arenas.back().freeSpace() << endl;
            // undo the expansion
            if (ftruncate(fd, currentlyAllocated) == -1) {
                throw AnnotatedException(500, "Ftruncate failed: " + string(strerror(errno)));
            }
            verifyLength();
            return false;
        }

        currentlyAllocated += newLength - arenas.back().length;
        arenas.back().length = newLength;

        verifyLength();

        return true;
#else
    return false;  /* no mremap available */
#endif
    }

    std::mutex mutex;
    Utf8String filename;
    int fd = -1;
    size_t currentlyAllocated = 0;

    struct Arena {
        Arena(void * addr, size_t startOffset, size_t length)
            : addr(addr), startOffset(startOffset), length(length)
        {
        }

        ~Arena()
        {
            if (addr)
                ::munmap(addr, length);
        }

        Arena(const Arena &) = delete;
        void operator = (const Arena &) = delete;

        Arena(Arena && other)
            : addr(other.addr), startOffset(other.startOffset),
              length(other.length)
        {
            other.addr = nullptr;
        }
        
        void * addr = nullptr;
        size_t startOffset = 0;
        size_t length = 0;
        size_t currentOffset = 0;

        void * allocate(size_t bytes, size_t alignment)
        {
            size_t extraBytes = currentOffset % alignment;
            if (extraBytes > 0)
                extraBytes = alignment - extraBytes;

            if (currentOffset + bytes + extraBytes > length)
                return nullptr;

            char * data
                = reinterpret_cast<char *>(addr) + extraBytes + currentOffset;
            currentOffset += extraBytes + bytes;
            return data;
        }

        size_t freeSpace() const
        {
            return length - currentOffset;
        }
    };

    std::vector<Arena> arenas;
};

FileSerializer::
FileSerializer(Utf8String filename)
    : itl(new Itl(filename))
{
}

FileSerializer::
~FileSerializer()
{
}

void
FileSerializer::
commit()
{
    itl->commit();
}

MutableMemoryRegion
FileSerializer::
allocateWritable(uint64_t bytesRequired,
                 size_t alignment)
{
    auto handle = itl->allocateWritable(bytesRequired, alignment);
    char * mem = (char *)handle.get();
    return {std::move(handle), mem, (size_t)bytesRequired, this };
}

FrozenMemoryRegion
FileSerializer::
freeze(MutableMemoryRegion & region)
{
    return FrozenMemoryRegion(region.handle(), region.data(), region.length());
}


} // namespace MLDB
