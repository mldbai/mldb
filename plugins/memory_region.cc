/** frozen_column.cc                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "memory_region.h"
#include <mutex>
#include <vector>
#include <cstring>
#include "mldb/arch/vm.h"
#include "mldb/http/http_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/path.h"
#include "mldb/types/basic_value_descriptions.h"

#include <fcntl.h>

#include <sys/mman.h>
#include <sys/stat.h>

#include <archive.h>
#include <archive_entry.h>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* MAPPED SERIALIZER                                                         */
/*****************************************************************************/

struct SerializerStreamHandler {
    MappedSerializer * owner = nullptr;
    std::ostringstream stream;
    std::shared_ptr<void> baggage;  /// anything we need to stay alive

    ~SerializerStreamHandler()
    {
        // we now need to write our stream to the serializer
        auto mem = owner->allocateWritable(stream.str().size(),
                                           1 /* alignment */);
        std::memcpy(mem.data(), stream.str().data(), stream.str().size());
        mem.freeze();
    }
};


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
FrozenMemoryRegion(std::shared_ptr<void> handle,
                   const char * data,
                   size_t length)
    : data_(data), length_(length), handle_(std::move(handle))
{
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


/*****************************************************************************/
/* MUTABLE MEMORY REGION                                                     */
/*****************************************************************************/

struct MutableMemoryRegion::Itl {
    Itl(std::shared_ptr<void> handle,
        char * data,
        size_t length,
        MappedSerializer * owner)
        : handle(std::move(handle)), data(data), length(length), owner(owner)
    {
    }
    
    std::shared_ptr<void> handle;
    char * data;
    size_t length;
    MappedSerializer * owner;
};

MutableMemoryRegion::
MutableMemoryRegion(std::shared_ptr<void> handle,
                    char * data,
                    size_t length,
                    MappedSerializer * owner)
    : itl(new Itl(std::move(handle), data, length, owner)),
      data_(data),
      length_(length)
{
}

std::shared_ptr<void>
MutableMemoryRegion::
handle() const
{
    return itl->handle;
}

FrozenMemoryRegion
MutableMemoryRegion::
freeze()
{
    return itl->owner->freeze(*this);
}


/*****************************************************************************/
/* MEMORY SERIALIZER                                                         */
/*****************************************************************************/

void
MemorySerializer::
commit()
{
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
    int res = posix_memalign(&mem, alignment, bytesRequired);
    if (res != 0) {
        cerr << "bytesRequired = " << bytesRequired
             << " alignment = " << alignment << endl;
        throw HttpReturnException(400, "Error allocating writable memory: "
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
    return FrozenMemoryRegion(region.handle(), region.data(), region.length());
}


/*****************************************************************************/
/* STRUCTURED SERIALIZER                                                     */
/*****************************************************************************/

void
StructuredSerializer::
addRegion(const FrozenMemoryRegion & region,
          const PathElement & name)
{
    auto entry = newEntry(name);
    // TODO: let the serializer handle it; no need to double allocate and
    // copy here
    auto serializeTo = entry->allocateWritable(region.length(), 1 /* alignment */);
    std::memcpy(serializeTo.data(), region.data(), region.length());
    serializeTo.freeze();
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
    auto entry = newEntry("md.json");
    auto serializeTo = entry->allocateWritable(printed.rawLength(),
                                               1 /* alignment */);
    
    std::memcpy(serializeTo.data(), printed.rawData(), printed.rawLength());
    serializeTo.freeze();
}


/*****************************************************************************/
/* FILE SERIALIZER                                                           */
/*****************************************************************************/

struct FileSerializer::Itl {
    Itl(Utf8String filename_)
        : filename(std::move(filename_))
    {
        fd = open(filename.rawData(), O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            throw HttpReturnException
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
            throw HttpReturnException
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
            throw HttpReturnException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }

        void * addr = mmap(nullptr, newLength,
                           PROT_READ | PROT_WRITE, MAP_SHARED,
                           fd, currentlyAllocated);
        if (addr == MAP_FAILED) {
            throw HttpReturnException
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
            throw HttpReturnException(500, "fstat");
        }
        ExcAssertEqual(st.st_size, currentlyAllocated);
    }

    bool expandLastArena(size_t bytesRequired)
    {
        verifyLength();

        size_t newLength
            = arenas.back().length
            + std::max<size_t>((bytesRequired + page_size - 1) / page_size,
                               10000 * page_size);
        
        cerr << "expanding from " << arenas.back().length
             << " to " << newLength << endl;

        int res = ::ftruncate(fd, currentlyAllocated + newLength - arenas.back().length);
        if (res == -1) {
            throw HttpReturnException
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
                throw HttpReturnException(500, "Ftruncate failed: " + string(strerror(errno)));
            }
            verifyLength();
            return false;
        }

        currentlyAllocated += newLength - arenas.back().length;
        arenas.back().length = newLength;

        verifyLength();

        return true;
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


/*****************************************************************************/
/* ZIP STRUCTURED SERIALIZER                                                 */
/*****************************************************************************/

struct ZipStructuredSerializer::Itl {
    virtual ~Itl()
    {
    }

    virtual void commit() = 0;

    virtual Path path() const = 0;

    virtual BaseItl * base() const = 0;
};

struct ZipStructuredSerializer::BaseItl: public Itl {
    BaseItl(Utf8String filename)
    {
        stream.open(filename.rawString());
        a.reset(archive_write_new(),
                [] (struct archive * a) { archive_write_free(a); });
        if (!a.get()) {
            throw HttpReturnException
                (500, "Couldn't create archive object");
        }
        archive_op(archive_write_set_format_zip);

        // We compress each one individually using zstandard or not
        // at all if we want to mmap.
        archive_op(archive_write_zip_set_compression_store);
        archive_op(archive_write_open, this,
                   &BaseItl::openCallback,
                   &BaseItl::writeCallback,
                   &BaseItl::closeCallback);
    }

    // Perform a libarchive operation
    template<typename Fn, typename... Args>
    void archive_op(Fn&& op, Args&&... args)
    {
        int res = op(a.get(), std::forward<Args>(args)...);
        if (res != ARCHIVE_OK) {
            throw HttpReturnException
                (500, string("Error writing zip file: ")
                 + archive_error_string(a.get()));
        }
    }

    struct Entry {
        Entry()
        {
            entry.reset(archive_entry_new(),
                        [] (archive_entry * e) { archive_entry_free(e); });
            if (!entry.get()) {
                throw HttpReturnException
                    (500, "Couldn't create archive entry");
            }
        }

        template<typename Fn, typename... Args>
        void op(Fn&& op, Args&&... args)
        {
            /*int res =*/ op(entry.get(), std::forward<Args>(args)...);
            /*
            if (res != ARCHIVE_OK) {
                throw HttpReturnException
                    (500, string("Error writing zip file: ")
                     + archive_error_string(entry.get()));
            }
            */
        }
        
        std::shared_ptr<struct archive_entry> entry;
    };

    void writeEntry(Path name, FrozenMemoryRegion region)
    {
        Entry entry;
        entry.op(archive_entry_set_pathname, name.toUtf8String().rawData());
        entry.op(archive_entry_set_size, region.length());
        entry.op(archive_entry_set_filetype, AE_IFREG);
#if 0
        for (auto & a: attrs) {
            entry.op(archive_entry_xattr_add_entry, a.first.c_str(),
                     a.second.data(), a.second.length());
        }
#endif
        archive_op(archive_write_header, entry.entry.get());
        auto written = archive_write_data(a.get(), region.data(), region.length());
        if (written != region.length()) {
            throw HttpReturnException(500, "Not all data written");
        }
    }

    virtual void commit()
    {
    }

    virtual Path path() const
    {
        return Path();
    }

    virtual BaseItl * base() const
    {
        return const_cast<BaseItl *>(this);
    }
    
    static int openCallback(struct archive * a, void * voidThis)

    {
        // Nothing to do here
        return ARCHIVE_OK;
    }

    static int closeCallback(struct archive * a, void * voidThis)
    {
        // Nothing to do here
        return ARCHIVE_OK;
    }

    /** Callback from libarchive when it needs to write some data to the
        output file.  This simply hooks it into the filter ostream.
    */
    static __LA_SSIZE_T	writeCallback(struct archive * a,
                                      void * voidThis,
                                      const void * buffer,
                                      size_t length)
    {
        BaseItl * that = reinterpret_cast<BaseItl *>(voidThis);
        that->stream.write((const char *)buffer, length);
        if (!that->stream)
            return -1;
        return length;
    }

    filter_ostream stream;
    std::shared_ptr<struct archive> a;
};

struct ZipStructuredSerializer::RelativeItl: public Itl {
    RelativeItl(Itl * parent,
                PathElement relativePath)
        : base_(parent->base()), parent(parent), relativePath(relativePath)
    {
    }

    virtual void commit()
    {
        cerr << "commiting " << path() << endl;
        // nothing to do; each entry will have been committed as it
        // was written
    }

    virtual Path path() const
    {
        return parent->path() + relativePath;
    }

    virtual BaseItl * base() const
    {
        return base_;
    }

    BaseItl * base_;
    Itl * parent;
    PathElement relativePath;
};

struct ZipStructuredSerializer::EntrySerializer: public MemorySerializer {
    EntrySerializer(Itl * itl, PathElement entryName)
        : itl(itl), entryName(std::move(entryName))
    {
    }

    virtual ~EntrySerializer()
    {
        Path name = itl->path() + entryName;
        //cerr << "finishing entry " << name << " with "
        //     << frozen.length() << " bytes" << endl;
        itl->base()->writeEntry(name, std::move(frozen));
    }

    virtual void commit() override
    {
    }

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region) override
    {
        return frozen = MemorySerializer::freeze(region);
    }

    Itl * itl;
    PathElement entryName;
    FrozenMemoryRegion frozen;
};

ZipStructuredSerializer::
ZipStructuredSerializer(Utf8String filename)
    : itl(new BaseItl(filename))
{
}

ZipStructuredSerializer::
ZipStructuredSerializer(ZipStructuredSerializer * parent,
                        PathElement relativePath)
    : itl(new RelativeItl(parent->itl.get(), relativePath))
{
}

ZipStructuredSerializer::
~ZipStructuredSerializer()
{
}

std::shared_ptr<StructuredSerializer>
ZipStructuredSerializer::
newStructure(const PathElement & name)
{
    return std::make_shared<ZipStructuredSerializer>(this, name);
}

std::shared_ptr<MappedSerializer>
ZipStructuredSerializer::
newEntry(const PathElement & name)
{
    return std::make_shared<EntrySerializer>(itl.get(), name);
}

filter_ostream
ZipStructuredSerializer::
newStream(const PathElement & name)
{
    auto entry = newEntry(name);

    auto handler = std::make_shared<SerializerStreamHandler>();
    handler->owner = entry.get();
    handler->baggage = entry;

    filter_ostream result;
    result.openFromStreambuf(handler->stream.rdbuf(), handler);
    
    return result;
}

void
ZipStructuredSerializer::
commit()
{
    itl->commit();
}

} // namespace MLDB
