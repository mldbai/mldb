/** memory_region.cc                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "memory_region.h"
#include "memory_region_impl.h"
#include <vector>
#include <cstring>
#include "mldb/arch/vm.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/path.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/arch/vm.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* MAPPED SERIALIZER                                                         */
/*****************************************************************************/

FrozenMemoryRegion
MappedSerializer::
copy(const FrozenMemoryRegion & region)
{
    auto serializeTo = allocateWritable(region.length(), 1 /* alignment */);
    std::memcpy(serializeTo.data(), region.data(), region.length());
    return serializeTo.freeze();
}

filter_ostream
MappedSerializer::
getStream()
{
    auto handler = std::make_shared<SerializerStreamHandler>();
    handler->owner = this;

    filter_ostream result;
    result.openFromStreambuf(handler->stream.rdbuf(), handler);
    
    return result;
}


/*****************************************************************************/
/* FROZEN MEMORY REGION                                                      */
/*****************************************************************************/

FrozenMemoryRegion::
FrozenMemoryRegion(std::shared_ptr<const void> handle,
                   const char * data,
                   size_t length) noexcept
    : data_(data), length_(length), handle_(std::move(handle))
{
}

FrozenMemoryRegion
FrozenMemoryRegion::
range(size_t start, size_t end) const
{
    //cerr << "returning range from " << start << " to " << end
    //     << " of " << length() << endl;
    ExcAssertGreaterEqual(end, start);
    ExcAssertLessEqual(end, length());
    return FrozenMemoryRegion(handle_, data() + start, end - start);
}

FrozenMemoryRegion
FrozenMemoryRegion::
rangeAtStart(size_t length) const
{
    return range(0, length);
}

FrozenMemoryRegion
FrozenMemoryRegion::
rangeAtEnd(size_t length) const
{
    return range(this->length() - length, this->length());
}

#if 0
void
FrozenMemoryRegion::
reserialize(MappedSerializer & serializer) const
{
    // TODO: let the serializer handle it; no need to double allocate and
    // copy here
    auto serializeTo = serializer.allocateWritable(length_, 1 /* alignment */);
    std::memcpy(serializeTo.data(), data_, length_);
    serializeTo.freeze();
}
#endif

FrozenMemoryRegion
FrozenMemoryRegion::
combined(const FrozenMemoryRegion & region1,
         const FrozenMemoryRegion & region2)
{
    return combined({region1, region2});
}

FrozenMemoryRegion
FrozenMemoryRegion::
combined(const std::vector<FrozenMemoryRegion> & regions)
{
    // For now, this is very simplistic.  There are many opportunities
    // to optimize later on.
    
    static MemorySerializer serializer;
    uint64_t totalLength = 0;
    for (auto & r: regions) {
        totalLength += r.length();
    }

    auto mem = serializer.allocateWritable(totalLength, 8 /* todo alignment */);

    size_t offset = 0;
    for (auto & r: regions) {
        if (r.length() == 0)
            continue;  // to avoid memcpy(nullptr, xxx, 0)
        memcpy(mem.data() + offset, r.data(), r.length());
        offset += r.length();
    }

    return mem.freeze();
}


/*****************************************************************************/
/* MUTABLE MEMORY REGION                                                     */
/*****************************************************************************/

struct MutableMemoryRegion::Itl {
    Itl(std::shared_ptr<const void> handle,
        char * data,
        size_t length,
        MappedSerializer * owner)
        : handle(std::move(handle)), data(data), length(length), owner(owner)
    {
        ExcAssert(owner);
    }
    
    std::shared_ptr<const void> handle;
    char * data;
    size_t length;
    MappedSerializer * owner;
};

MutableMemoryRegion::
MutableMemoryRegion(std::shared_ptr<const void> handle,
                    char * data,
                    size_t length,
                    MappedSerializer * owner)
    : itl(new Itl(std::move(handle), data, length, owner)),
      data_(data),
      length_(length)
{
}

std::shared_ptr<const void>
MutableMemoryRegion::
handle() const
{
    ExcAssert(itl);
    return itl->handle;
}

FrozenMemoryRegion
MutableMemoryRegion::
freeze()
{
    ExcAssert(itl);
    ExcAssert(itl->owner);
    return itl->owner->freeze(*this);
}

MutableMemoryRegion
MutableMemoryRegion::
range(size_t startByte, size_t endByte) const
{
    ExcAssertLessEqual(startByte, endByte);
    ExcAssertLessEqual(endByte, length_);

    ExcAssert(itl);
    ExcAssert(itl->owner);
    
    MutableMemoryRegion result;
    result.itl = this->itl;
    result.data_ = this->data_ + startByte;
    result.length_ = (endByte - startByte);

    ExcAssert(result.itl->owner);

    return result;
}

std::shared_ptr<const void>
MutableMemoryRegion::
reset()
{
    ExcAssert(itl);
    data_ = nullptr;
    length_ = 0;
    std::shared_ptr<const void> result(std::move(itl->handle));
    itl.reset();
    return result;
}

FrozenMemoryRegion
mapFile(const Url & filename, size_t startOffset, ssize_t length)
{
    if (filename.scheme() != "file") {
        throw AnnotatedException
            (500, "only file:// entities can be memory mapped (for now)");
    }
    
    // TODO: not only files...
    int fd = open(filename.path().c_str(), O_RDONLY);
    if (fd == -1) {
        throw AnnotatedException
            (400, "Couldn't open mmap file " + filename.toUtf8String()
             + ": " + strerror(errno));
    }

    if (length == -1) {
        struct stat buf;
        int res = fstat(fd, &buf);
        if (res == -1) {
            close(fd);
            throw AnnotatedException
                (400, "Couldn't stat mmap file " + filename.toUtf8String()
                 + ": " + strerror(errno));
        }
        length = buf.st_size;
    }

    cerr << "file goes from 0 for " << length << " bytes" << endl;
    
    size_t mapOffset = startOffset & ~(page_size - 1);
    size_t mapLength = (length - mapOffset + page_size -1) & ~(page_size - 1);
    
    cerr << "mapping from " << mapOffset << " for " << mapLength << " bytes"
         << endl;

    std::shared_ptr<void> addr
        (mmap(nullptr, mapLength,
              PROT_READ, MAP_SHARED, fd, mapOffset),
         [=] (void * p) { munmap(p, mapLength); close(fd); });

    if (addr.get() == MAP_FAILED) {
        throw AnnotatedException
            (400, "Failed to open memory map file: "
             + string(strerror(errno)));
    }

    const char * start = reinterpret_cast<const char *>(addr.get());
    start += (startOffset % page_size);

    cerr << "taking off " << (startOffset % page_size) << " bytes" << endl;
    cerr << "length = " << length << endl;
    
    return FrozenMemoryRegion(std::move(addr), start, length);
}


/*****************************************************************************/
/* MEMORY SERIALIZER                                                         */
/*****************************************************************************/

void
MemorySerializer::
commit()
{
}

// Return the set of pages that are completely covered by this memory block
static std::pair<void *, size_t>
getPageRange(const void * mem, size_t length)
{
    void * startAddr = (void *)((((size_t)mem) + page_size - 1) / page_size * page_size);
    //cerr << "startAddr = " << startAddr << " for " << mem << endl;
    size_t offset = (char *)startAddr - (char *)mem;
    ExcAssertLess(offset, page_size);
    if (offset >= length) {
        return { 0, 0 };
    }
    size_t realLen = length - offset;
    size_t pageLen = realLen / page_size * page_size;
    ExcAssertGreaterEqual(startAddr, mem);
    ExcAssertLessEqual(pageLen, length);
    return { startAddr, pageLen };
}

MutableMemoryRegion
MemorySerializer::
allocateWritable(uint64_t bytesRequired,
                 size_t alignment)
{
    //cerr << "allocating " << bytesRequired << " bytes" << endl;
        
    void * mem = nullptr;
    ExcAssertEqual((size_t)bytesRequired, bytesRequired);
    if (alignment < sizeof(void *)) {
        alignment = sizeof(void *);
    }

    size_t bytesToAllocate
        = (bytesRequired + alignment - 1) / alignment * alignment;
    
    int res = posix_memalign(&mem, alignment, bytesToAllocate);
    if (res != 0) {
        cerr << "bytesRequired = " << bytesRequired
             << " alignment = " << alignment << endl;
        throw AnnotatedException(400, "Error allocating writable memory: "
                                  + string(strerror(res)),
                                  "bytesRequired", bytesRequired,
                                  "alignment", alignment);
    }

    std::shared_ptr<void> handle(mem, [] (void * mem) { ::free(mem); });
    return {std::move(handle), (char *)mem, (size_t)bytesRequired, this };
}

FrozenMemoryRegion
MemorySerializer::
freeze(MutableMemoryRegion & region)
{
    char * data = region.data();
    size_t length = region.length();

    void * pageStart;
    size_t pageLen;
    std::tie(pageStart, pageLen) = getPageRange(data, length);

    std::shared_ptr<const void> handle;
    
    if (pageLen > 0 && false /* VMA exhaustion */) {
        // Set protection to read-only for full pages to ensure it's really frozen
        //cerr << "protecting " << pageStart << " at offset " << (char *)pageStart - data
        //     << " for " << pageLen
        //     << " with range " << (void *)region.data()
        //     << " for " << region.length()
        //     << endl;
        //cerr << "getPageSize() = " << getpagesize() << endl;
        int res = mprotect(pageStart, pageLen, PROT_READ);
        if (res == -1) {
            throw MLDB::Exception(errno, "mprotect READ");
        }

        // Do this late so a mprotect exception will not destroy our region
        handle = region.reset();
        
        // Keep handle so that when it goes out of scope, the memory is freed
        auto unprotectAndFree = [handle,pageStart,pageLen] (const void * mem)
            {
                int res = mprotect(pageStart, pageLen, PROT_READ | PROT_WRITE);
                if (res == -1) {
                    throw MLDB::Exception(errno, "mprotect READ|WRITE");
                }
                
                // Will be freed by handle going out of scope in the capture
            };

        handle.reset(handle.get(), std::move(unprotectAndFree));
    }
    else {
        handle = region.reset();
    }
    
    FrozenMemoryRegion result(std::move(handle), data, length);
    return result;
}


/*****************************************************************************/
/* STRUCTURED SERIALIZER                                                     */
/*****************************************************************************/

void
StructuredSerializer::
addRegion(const FrozenMemoryRegion & region,
          const PathElement & name)
{
    newEntry(name)->copy(region);
}

void
StructuredSerializer::
newObject(const PathElement & name,
          const void * val,
          const ValueDescription & desc)
{
    Utf8String printed;
    {
        Utf8StringJsonPrintingContext context(printed);
        desc.printJson(val, context);
    }
    //cerr << "doing metadata " << printed << endl;
    auto entry = newEntry(name);
    auto serializeTo = entry->allocateWritable(printed.rawLength(),
                                               1 /* alignment */);
    
    std::memcpy(serializeTo.data(), printed.rawData(), printed.rawLength());
    serializeTo.freeze();
}


/*****************************************************************************/
/* STRUCTURED RECONSTITUTER                                                  */
/*****************************************************************************/

StructuredReconstituter::
~StructuredReconstituter()
{
}

FrozenMemoryRegion
StructuredReconstituter::
getRegionRecursive(const Path & name) const
{
    ExcAssert(!name.empty());
    if (name.size() == 1)
        return getRegion(name.head());
    return getStructure(name.head())->getRegionRecursive(name.tail());
}

struct ReconstituteStreamHandler: std::streambuf {
    ReconstituteStreamHandler(const char * start, const char * end)
        : start(const_cast<char *>(start)), end(const_cast<char *>(end))
    {
        setg(this->start, this->start, this->end);
    }

    ReconstituteStreamHandler(const char * buf, size_t length)
        : ReconstituteStreamHandler(buf, buf + length)
    {
    }

    ReconstituteStreamHandler(FrozenMemoryRegion region)
        : ReconstituteStreamHandler(region.data(), region.length())
    {
        this->region = std::move(region);
    }
    
    virtual pos_type
    seekoff(off_type off, std::ios_base::seekdir dir,
            std::ios_base::openmode which) override
    {
        switch (dir) {
        case std::ios_base::cur:
            gbump(off);
            break;
        case std::ios_base::end:
            setg(start, end + off, end);
            break;
        case std::ios_base::beg:
            setg(start, start+off, end);
            break;
        default:
            throw Exception("Streambuf invalid seakoff dir");
        }
        
        return gptr() - eback();
    }
    
    virtual pos_type
    seekpos(streampos pos, std::ios_base::openmode mode) override
    {
        return seekoff(pos - pos_type(off_type(0)), std::ios_base::beg, mode);
    }

    // NOTE: these are non-const due to the somewhat archaic streambuf
    // interface
    /*const*/ char * start;
    /*const*/ char * end;
    FrozenMemoryRegion region;
};

filter_istream
StructuredReconstituter::
getStream(const PathElement & name) const
{
    auto handler
        = std::make_shared<ReconstituteStreamHandler>(getRegion(name));
    
    filter_istream result;
    result.openFromStreambuf(handler.get(),
                             std::move(handler),
                             name.toUtf8String().stealRawString());
                             
    return result;
}

filter_istream
StructuredReconstituter::
getStreamRecursive(const Path & name) const
{
    ExcAssert(!name.empty());
    if (name.size() == 1)
        return getStream(name.head());
    return getStructure(name.head())->getStreamRecursive(name.tail());
}
    
std::shared_ptr<StructuredReconstituter>
StructuredReconstituter::
getStructureRecursive(const Path & name) const
{
    std::shared_ptr<StructuredReconstituter> result;
    const StructuredReconstituter * current = this;
    
    for (auto el: name) {
        result = current->getStructure(el);
        current = result.get();
    }

    return result;
}

void
StructuredReconstituter::
getObjectHelper(const PathElement & name, void * obj,
                const std::shared_ptr<const ValueDescription> & desc) const
{
    auto entry = getRegion(name);
    Utf8StringJsonParsingContext context
        (entry.data(), entry.length(), "getObjectHelper");
    desc->parseJson(obj, context);
}


} // namespace MLDB
