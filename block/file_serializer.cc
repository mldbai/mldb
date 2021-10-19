/** file_serialier.cc                                              -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into the filesystem.
*/

#include "file_serializer.h"
#include "memory_region_impl.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/value_description.h"
#include "mldb/arch/vm.h"

#include <mutex>

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>



using namespace std;

namespace MLDB {


namespace {


struct MappedArena {
    MappedArena(void * addr, size_t startOffset, size_t length,
                size_t currentOffset = 0)
        : addr(addr), startOffset(startOffset), length(length),
          currentOffset(currentOffset)
    {
        //cerr << "init mapped arena with currentOffset " << currentOffset
        //     << endl;
    }

    ~MappedArena()
    {
        if (addr)
            ::munmap(addr, length);
    }

    MappedArena(const MappedArena &) = delete;
    void operator = (const MappedArena &) = delete;

    MappedArena(MappedArena && other)
        : addr(other.addr), startOffset(other.startOffset),
          length(other.length)
    {
        other.addr = nullptr;
    }
        
    void * addr = nullptr;
    size_t startOffset = 0;
    size_t length = 0;
    size_t currentOffset = 0;

    char * allocate(size_t bytes, size_t alignment)
    {
        //cerr << "allocating " << bytes << "%" << alignment
        //     << " from " << startOffset << "/" << currentOffset
        //     << "/" << length << endl;
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



} // file scope

/*****************************************************************************/
/* FILE SERIALIZER                                                           */
/*****************************************************************************/

struct FileSerializer::Itl {
    Itl(Utf8String filename__,
        bool exclusive,
        bool sync)
        : filename(std::move(filename__)),
          sync(sync)
    {
        fd = open(filename.rawData(),
                  O_CREAT | O_RDWR | O_TRUNC | (exclusive ? O_EXCL : 0),
                  S_IRUSR | S_IWUSR);
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

        if (sync) {
            for (auto & a: arenas) {
                ::msync(a.addr, a.length, MS_SYNC);
            }
#if __linux__
            ::fdatasync(fd);
#else
            ::fsync(fd);
#endif
        }
    }

    std::shared_ptr<void>
    allocateWritableImpl(uint64_t bytesRequired, size_t alignment)
    {
        if (bytesRequired == 0)
            return nullptr;

        if (arenas.empty()) {
            createNewMappedArena(bytesRequired + alignment);
        }
        
        void * allocated = nullptr;
        
        while ((allocated = arenas.back().allocate(bytesRequired, alignment)) == nullptr) {
            if (!expandLastMappedArena(bytesRequired + alignment)) {
                createNewMappedArena(bytesRequired + alignment);
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

    void createNewMappedArena(size_t bytesRequired)
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

    bool expandLastMappedArena(size_t bytesRequired)
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
    bool sync;
    int fd = -1;
    size_t currentlyAllocated = 0;

    std::vector<MappedArena> arenas;
};

FileSerializer::
FileSerializer(Utf8String filename,
               bool exclusive,
               bool sync)
    : itl(new Itl(filename, exclusive, sync))
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
    return {std::move(handle), mem, (size_t)bytesRequired };
}

FrozenMemoryRegion
FileSerializer::
freeze(MutableMemoryRegion & region)
{
    return FrozenMemoryRegion(region.handle(), region.data(), region.length());
}


/*****************************************************************************/
/* TEMPORARY FILE SERIALIZER                                                 */
/*****************************************************************************/

struct TemporaryFileSerializer::Itl {
    Itl(Utf8String directory,
        Utf8String reason)
        : directory(std::move(directory)),
          reason(std::move(reason))
    {
    }
    
    ~Itl()
    {
        if (fd != -1) {
            // Unlink before closing
            unlink(filename.c_str());
            close(fd);
        }

        arenas.clear();
    }

    void init(size_t bytesRequired)
    {
        // mutex must be held

        if (fd != -1)
            return;

        filename
            = directory.rawString()
            + "/mldb-tmp-" + reason.rawString() + "XXXXXX\0";

        fd = ::mkstemp(filename.data());
        
        if (fd == -1) {
            throw AnnotatedException(400, "Error opening temporary file: "
                                     + string(strerror(errno)));
        }

        growArena(bytesRequired);
    }

    void reserve(size_t bytesRequired)
    {
        std::unique_lock<std::mutex> guard(mutex);
        if (arenas.empty()) {
            init(bytesRequired);
        }
        else {
            growArena(bytesRequired);
        }
    }
    
    // mutex must be held
    void growArena(size_t bytesRequired)
    {
        size_t currentlyAllocated = 0;
        if (!arenas.empty())
            currentlyAllocated = arenas.back().currentOffset;

        size_t numPages
            = std::max<size_t>
            ((currentlyAllocated + bytesRequired + page_size - 1) / page_size,
             1024);

        //cerr << "currentlyAllocated = " << currentlyAllocated << endl;
        
        // Make sure we grow geometrically (doubling every update) to
        // amortize overhead and minimize number of mappings.
        numPages = std::max<size_t>
            (numPages * 2, (currentlyAllocated + page_size - 1) / page_size);

        size_t newLength = numPages * page_size;
        
        //cerr << "temp expanded to " << newLength * 0.000001 << " MB" << endl;
        
        int res = ::ftruncate(fd, newLength);
        if (res == -1) {
            throw AnnotatedException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }

        // Try to expand the current mapping with mremap
        char * newAddr = nullptr;
#if __linux__
        if (!arenas.empty()) {
            newAddr = (char *)mremap(arenas.back().addr,
                                     arenas.back().length,
                                     newLength,
                                     0 /* flags */);

            if (newAddr == arenas.back().addr) {
                // mremap succeeded; we can update the metadata
                arenas.back().length = newLength;
            }
            else newAddr = nullptr;
        }
#endif

        if (newAddr == nullptr) {
            // Remap from the beginning, so that we can return any portion of the
            // block
            void * addr = mmap(nullptr, newLength,
                               PROT_READ | PROT_WRITE, MAP_SHARED,
                               fd, 0 /* offset */);
            
            if (addr == MAP_FAILED) {
                throw AnnotatedException
                    (400, "Failed mmap file: "
                     + string(strerror(errno)));
            }
            arenas.emplace_back(addr, 0, newLength, currentlyAllocated);
        }
    }
    
    void commit()
    {
        // We can remap everything to read-only and truncate our file

        // ... TODO ...
    }

    std::shared_ptr<char>
    allocateWritable(uint64_t bytesRequired,
                     size_t alignment)
    {
        ExcAssertNotEqual(alignment, 0);

        std::unique_lock<std::mutex> guard(mutex);

        if (arenas.empty()) {
            init(bytesRequired);
        }

        if (arenas.back().currentOffset % alignment != 0) {
            throw AnnotatedException
                (500, "Mis-aligned TemporaryFileSerializer block",
                 "alignmentRequested", alignment,
                 "misAlignment", arenas.back().currentOffset % alignment);
        }
        
        char * alloc = nullptr;

        while (!(alloc = arenas.back().allocate(bytesRequired, alignment))) {
            growArena(bytesRequired + alignment - 1);
        }

        return std::shared_ptr<char>(alloc, [] (void *) {});
    }

    FrozenMemoryRegion freeze(MutableMemoryRegion & region)
    {
        // TODO: pages to read-only
        return FrozenMemoryRegion(region.handle(), region.data(),
                                  region.length());
    }

    std::tuple<size_t, const char *, size_t>
    getRangeContaining(size_t offset, size_t length) const
    {
        std::unique_lock<std::mutex> guard(mutex);

        if (arenas.empty()) {
            return make_tuple(0, nullptr, 0);
        }
        if (offset >= arenas.back().currentOffset) {
            return make_tuple(arenas.back().currentOffset, nullptr, 0);
        }
        if (offset + length > arenas.back().currentOffset) {
            length = arenas.back().currentOffset - offset;
        }
        //cerr << "temp file getRangeContaining: offset = " << offset << " len "
        //     << length << endl;
        return make_tuple(offset, ((const char *)arenas.back().addr) + offset,
                          length);
    }

    size_t length() const
    {
        if (arenas.empty())
            return 0;
        return arenas.back().currentOffset;
    }
    
    /// Directory in which we create the file
    Utf8String directory;

    /// Reason for opening it (affects file name)
    Utf8String reason;
    
    /// Basic block size we use.  Goal is to enable huge pages for large
    /// temporary files, so it's 1GB.
    static constexpr size_t blockSize = 1 * 1024 * 1024 * 1024;

    /// File that's backing the temporary data.  This can be on a tmp filesystem
    /// or a huge page file system to improve efficiency.
    std::string filename;

    /// File descriptor of the open file
    int fd = -1;

    /// Mutex protecting internal data
    mutable std::mutex mutex;

    /// Set of regions at which our data is mapped.  In some cases when we
    /// enlarge our backing file we won't be able to enlarge the mapping;
    /// in that case we need to maintain the older and the newer mapping.
    std::vector<MappedArena> arenas;
};

TemporaryFileSerializer::
TemporaryFileSerializer(Utf8String directory, Utf8String reason)
    : itl(new Itl(std::move(directory), std::move(reason)))
{
}

TemporaryFileSerializer::
~TemporaryFileSerializer()
{
}

void
TemporaryFileSerializer::
commit()
{
    itl->commit();
}

MutableMemoryRegion
TemporaryFileSerializer::
allocateWritable(uint64_t bytesRequired,
                 size_t alignment)
{
    auto handle = itl->allocateWritable(bytesRequired, alignment);
    char * mem = (char *)handle.get();
    return {std::move(handle), mem, (size_t)bytesRequired, };
}

FrozenMemoryRegion
TemporaryFileSerializer::
freeze(MutableMemoryRegion & region)
{
    return itl->freeze(region);
}

std::pair<uint64_t, FrozenMemoryRegion>
TemporaryFileSerializer::
getRangeContaining(size_t offset, size_t length) const
{
    size_t startOffset;
    const char * data;

    std::tie(startOffset, data, length)
        = itl->getRangeContaining(offset, length);

    if (!data) {
        return { startOffset, FrozenMemoryRegion() };
    }
    else {
        std::shared_ptr<const char> mem(data, [] (const char *) {});
        return {offset, FrozenMemoryRegion(std::move(mem), data, length)};
    }
}

size_t
TemporaryFileSerializer::
getLength() const
{
    return itl->length();
}

void
TemporaryFileSerializer::
reserve(uint64_t bytesRequired)
{
    itl->reserve(bytesRequired);
}

} // namespace MLDB
