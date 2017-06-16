/** frozen_column.cc                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "frozen_column.h"
#include "tabular_dataset_column.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/utils/compact_vector.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/atomic_shared_ptr.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/arch/vm.h"
#include "mldb/arch/endian.h"
#include "mldb/vfs/filter_streams.h"
#include "frozen_tables.h"
#include <mutex>
#include <string.h>
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
                Utf8String relativePath)
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
    Utf8String relativePath;
};

struct ZipStructuredSerializer::EntrySerializer: public MemorySerializer {
    EntrySerializer(Itl * itl, Utf8String entryName)
        : itl(itl), entryName(std::move(entryName))
    {
    }

    virtual ~EntrySerializer()
    {
        Path name = itl->path() + entryName;
        cerr << "finishing entry " << name << " with "
             << frozen.length() << " bytes" << endl;
        itl->base()->writeEntry(name, frozen);
    }

    virtual void commit() override
    {
    }

    virtual FrozenMemoryRegion freeze(MutableMemoryRegion & region) override
    {
        return frozen = MemorySerializer::freeze(region);
    }

    Itl * itl;
    Utf8String entryName;
    FrozenMemoryRegion frozen;
};

ZipStructuredSerializer::
ZipStructuredSerializer(Utf8String filename)
    : itl(new BaseItl(filename))
{
}

ZipStructuredSerializer::
ZipStructuredSerializer(ZipStructuredSerializer * parent,
                        Utf8String relativePath)
    : itl(new RelativeItl(parent->itl.get(), relativePath))
{
}

ZipStructuredSerializer::
~ZipStructuredSerializer()
{
}

std::shared_ptr<StructuredSerializer>
ZipStructuredSerializer::
newStructure(const Utf8String & name)
{
    return std::make_shared<ZipStructuredSerializer>(this, name);
}

std::shared_ptr<MappedSerializer>
ZipStructuredSerializer::
newEntry(const Utf8String & name)
{
    return std::make_shared<EntrySerializer>(itl.get(), name);
}

filter_ostream
ZipStructuredSerializer::
newStream(const Utf8String & name)
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


/*****************************************************************************/
/* DIRECT FROZEN COLUMN                                                      */
/*****************************************************************************/

/// Frozen column that simply stores the values directly

struct DirectFrozenColumn: public FrozenColumn {
    DirectFrozenColumn(TabularDatasetColumn & column,
                      MappedSerializer & serializer)
        : columnTypes(std::move(column.columnTypes))
    {
        firstEntry = column.minRowNumber;
        numEntries = column.maxRowNumber - column.minRowNumber + 1;

        MutableCellValueTable mutableValues;
        mutableValues.reserve(column.sparseIndexes.size());

        for (auto & v: column.sparseIndexes) {
            mutableValues.set(v.first, column.indexedVals[v.second]);
        }
        numNonNullEntries = column.sparseIndexes.size();
        values = mutableValues.freeze(serializer);
    }

    virtual std::string format() const
    {
        return "d";
    }

    bool forEachImpl(const ForEachRowFn & onRow, bool keepNulls) const
    {
        for (size_t i = 0;  i < values.size();  ++i) {
            if (keepNulls || !values[i].empty()) {
                if (!onRow(i + firstEntry, values[i]))
                    return false;
            }
        }

        // Do any trailing nulls
        for (size_t i = values.size();  i < numEntries && keepNulls; ++i) {
            if (!onRow(i + firstEntry, CellValue()))
                return false;
        }

        return true;
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= values.size())
            return result; // nulls at the end
        ExcAssertLess(rowIndex, numEntries);
        return values[rowIndex];
    }

    virtual size_t size() const
    {
        return numEntries;
    }

    virtual size_t memusage() const
    {
        size_t result
            = sizeof(*this);
        
        result += values.memusage();
        
        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        bool doneNull = false;
        auto fn2 = [&] (const CellValue & val)
            {
                if (val.empty())
                    doneNull = true;
                return fn(val);
            };
        if (!values.forEachDistinctValue(fn))
            return false;

        // Trailing nulls?
        if (values.size() < numEntries && !doneNull) {
            return fn(CellValue());
        }
       
        return true;
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullEntries;
    }

    uint32_t numEntries;
    uint64_t firstEntry;
    uint32_t numNonNullEntries = 0;
    FrozenCellValueTable values;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer) const
    {
        // TODO: finish
        values.serialize(serializer);
    }
};

struct DirectFrozenColumnFormat: public FrozenColumnFormat {

    virtual ~DirectFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "d";
    }

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        return true;
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        size_t numEntries = column.maxRowNumber - column.minRowNumber + 1;
        size_t result = sizeof(DirectFrozenColumn);

        // How many times does each value occur?
        std::vector<size_t> valueCounts(column.indexedVals.size());

        for (auto & v: column.sparseIndexes) {
            valueCounts[v.second] += 1;
        }

        for (size_t i = 0;  i < column.indexedVals.size();  ++i) {
            size_t count = valueCounts[i];
            result += count * column.indexedVals[i].memusage();
        }

        // Nulls are stored explicitly...
        result += (numEntries - column.sparseIndexes.size()) * sizeof(CellValue);

        return result;
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new DirectFrozenColumn(column, serializer);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

RegisterFrozenColumnFormatT<DirectFrozenColumnFormat> regDirect;



/*****************************************************************************/
/* TABLE FROZEN COLUMN                                                       */
/*****************************************************************************/

/// Frozen column that finds each value in a lookup table
/// Useful when there are lots of duplicates
struct TableFrozenColumn: public FrozenColumn {
    TableFrozenColumn(TabularDatasetColumn & column,
                      MappedSerializer & serializer)
        : columnTypes(std::move(column.columnTypes))
    {
        MutableCellValueTable mutableTable
            (std::make_move_iterator(column.indexedVals.begin()),
             std::make_move_iterator(column.indexedVals.end()));
        table = mutableTable.freeze(serializer);

        firstEntry = column.minRowNumber;
        numEntries = column.maxRowNumber - column.minRowNumber + 1;
        hasNulls = column.sparseIndexes.size() < numEntries;

        MutableIntegerTable mutableIndexes;

        if (!hasNulls) {
            // Contiguous rows
            for (size_t i = 0;  i < column.sparseIndexes.size();  ++i) {
                ExcAssertEqual(column.sparseIndexes[i].first, i);
                mutableIndexes.add(column.sparseIndexes[i].second);
            }
        }
        else {
            // Non-contiguous; leave gaps with a zero (null) value
            size_t index = 0;
            for (auto & r_i: column.sparseIndexes) {
                while (index < r_i.first) {
                    mutableIndexes.add(0);
                    ++index;
                }
                mutableIndexes.add(r_i.second + 1);
                ++index;
            }
        }

        numNonNullEntries = column.sparseIndexes.size();
        indexes = mutableIndexes.freeze(serializer);
    }

    virtual std::string format() const
    {
        return "T";
    }

    virtual bool forEachImpl(const ForEachRowFn & onRow,
                             bool keepNulls) const
    {
        for (size_t i = 0;  i < numEntries;  ++i) {
            uint64_t index = indexes.get(i);

            CellValue val;
            if (hasNulls) {
                if (index > 0)
                    val = table[index - 1];
                else if (!keepNulls)
                    continue;  // skip nulls
            }
            else {
                val = table[index];
            }

            if (!onRow(i + firstEntry, val))
                return false;
        }

        return true;
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= indexes.size())
            return result;
        ExcAssertLess(rowIndex, numEntries);
        uint64_t index = indexes.get(rowIndex);
        if (hasNulls) {
            if (index == 0)
                return result;
            else return result = table[index - 1];
        }
        else {
            return result = table[index];
        }
    }

    virtual size_t size() const
    {
        return numEntries;
    }

    virtual size_t memusage() const
    {
        size_t result = sizeof(*this);
        result += table.memusage();
        result += indexes.memusage();

        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        if (hasNulls) {
            if (!fn(CellValue()))
                return false;
        }
        for (size_t i = 0;  i < table.size();  ++i) {
            if (!fn(table[i]))
                return false;
        }
        
        return true;
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullEntries;
    }

    FrozenIntegerTable indexes;
    uint32_t numEntries;
    uint64_t firstEntry;
    uint32_t numNonNullEntries = 0;
    
    bool hasNulls;
    FrozenCellValueTable table;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer) const
    {
        // TODO: finish
        indexes.serialize(serializer);
        table.serialize(serializer);
    }
};

struct TableFrozenColumnFormat: public FrozenColumnFormat {

    virtual ~TableFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "T";
    }

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        return true;
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        size_t numEntries = column.maxRowNumber - column.minRowNumber + 1;
        size_t hasNulls = column.sparseIndexes.size() < numEntries;
        int indexBits = bitsToHoldCount(column.indexedVals.size() + hasNulls);
        size_t result
            = sizeof(TableFrozenColumn)
            + (indexBits * numEntries + 31) / 8;

        for (auto & v: column.indexedVals)
            result += v.memusage();
        
        return result;
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new TableFrozenColumn(column, serializer);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

RegisterFrozenColumnFormatT<TableFrozenColumnFormat> regTable;


/*****************************************************************************/
/* SPARSE FROZEN COLUMN                                                      */
/*****************************************************************************/

/// Sparse frozen column that finds each value in a lookup table
struct SparseTableFrozenColumn: public FrozenColumn {
    SparseTableFrozenColumn(TabularDatasetColumn & column,
                            MappedSerializer & serializer)
        : columnTypes(std::move(column.columnTypes))
    {
        firstEntry = column.minRowNumber;
        lastEntry = column.maxRowNumber;

        MutableCellValueTable mutableTable;
        mutableTable.reserve(column.indexedVals.size());

        for (auto & v: column.indexedVals) {
            mutableTable.add(v);
        }

        this->table = mutableTable.freeze(serializer);

        MutableIntegerTable mutableRowNum, mutableIndex;
        mutableRowNum.reserve(column.sparseIndexes.size());
        mutableIndex.reserve(column.sparseIndexes.size());

        for (auto & i: column.sparseIndexes) {
            mutableRowNum.add(i.first);
            ExcAssertLess(i.second, table.size());
            mutableIndex.add(i.second);
        }

        rowNum = mutableRowNum.freeze(serializer);
        index = mutableIndex.freeze(serializer);

        if (false) {
            size_t mem = memusage();
            if (mem > 30000) {
                using namespace std;
                cerr << "table with " << column.sparseIndexes.size()
                     << " entries from "
                     << column.minRowNumber << " to " << column.maxRowNumber
                     << " and " << table.size()
                     << " uniques takes " << mem << " memory" << endl;
                
                for (unsigned i = 0;  i < 5 && i < table.size();  ++i) {
                    cerr << "  " << table[i];
                }
                cerr << endl;
            }
        }
    }

    virtual std::string format() const
    {
        return "ST";
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        for (size_t i = 0;  i < numEntries();  ++i) {
            auto rowNum = this->rowNum.get(i);
            auto index = this->index.get(i);
            if (!onRow(rowNum + firstEntry, table[index]))
                return false;
        }
        
        return true;
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        size_t lastRowNum = 0;
        for (size_t i = 0;  i < numEntries();  ++i) {
            auto rowNum = this->rowNum.get(i);
            auto index = this->index.get(i);

            while (lastRowNum < rowNum) {
                if (!onRow(firstEntry + lastRowNum, CellValue()))
                    return false;
                ++lastRowNum;
            }

            if (!onRow(firstEntry + rowNum, table[index]))
                return false;
            ++lastRowNum;
        }

        while (firstEntry + lastRowNum <= lastEntry) {
            if (!onRow(firstEntry + lastRowNum, CellValue()))
                return false;
            ++lastRowNum;
        }
        
        return true;
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;

        auto getAtIndex = [&] (uint32_t n)
            {
                auto rowNum = this->rowNum.get(n);
                auto index = this->index.get(n);
                return std::make_pair(rowNum, index);
            };

        uint32_t first = 0;
        uint32_t last  = numEntries();

        while (first != last) {
            uint32_t middle = (first + last) / 2;
            //cerr << "first = " << first << " middle = " << middle
            //     << " last = " << last << endl;
            uint32_t rowNum, index;
            std::tie(rowNum, index) = getAtIndex(middle);

#if 0
            cerr << "first = " << first << " middle = " << middle
                 << " last = " << last << " rowNum = " << rowNum
                 << " looking for " << rowIndex;
#endif

            if (rowNum == rowIndex) {
                ExcAssertLess(index, table.size());
                return result = table[index];
            }

            // Break out if the element isn't there
            if (first + 1 == last)
                break;

            if (rowNum < rowIndex) {
                first = middle;
            }
            else {
                last = middle;
            }

        }
        
        return result;
    }

    virtual size_t size() const
    {
        return lastEntry - firstEntry + 1;
    }

    virtual size_t memusage() const
    {
        size_t result
            = sizeof(*this);

        result += table.memusage();
        result += index.memusage();
        result += rowNum.memusage();
        
        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        // Detect nulls which implicitly means a gap in the indexes
        if (firstEntry + numEntries() != lastEntry + 1) {
            if (!fn(CellValue()))
                return false;
        }

        return table.forEachDistinctValue(fn);
    }

    virtual size_t nonNullRowCount() const override
    {
        return numEntries();
    }

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer) const
    {
        table.serialize(serializer);

        rowNum.serialize(serializer);
        index.serialize(serializer);
    }

    /// Set of distinct values in the column chunk
    FrozenCellValueTable table;

    /// Row numbers (in increasing order) per non-null cell
    FrozenIntegerTable rowNum;

    /// Table index per non-null cell, corresponding to entries in
    /// rowNum
    FrozenIntegerTable index;

    uint32_t numEntries() const { return rowNum.size(); }
    size_t firstEntry;
    size_t lastEntry;  // WARNING: this is the number, not number + 1
    ColumnTypes columnTypes;
};

struct SparseTableFrozenColumnFormat: public FrozenColumnFormat {

    virtual ~SparseTableFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "ST";
    }

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        return true;
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        int indexBits = bitsToHoldCount(column.indexedVals.size());
        int rowNumBits = bitsToHoldCount(column.maxRowNumber - column.minRowNumber);
        size_t numEntries = column.sparseIndexes.size();

        size_t result
            = sizeof(SparseTableFrozenColumn)
            + ((indexBits + rowNumBits) * numEntries + 31) / 8;

        for (auto & v: column.indexedVals)
            result += v.memusage();

        return result;
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new SparseTableFrozenColumn(column, serializer);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

RegisterFrozenColumnFormatT<SparseTableFrozenColumnFormat> regSparseTable;


/*****************************************************************************/
/* INTEGER FROZEN COLUMN                                                     */
/*****************************************************************************/

/// Frozen column that stores each value as a signed 64 bit integer
struct IntegerFrozenColumn: public FrozenColumn {

    struct SizingInfo {
        SizingInfo(const TabularDatasetColumn & column)
        {
            if (!column.columnTypes.onlyIntegersAndNulls()) {
#if 0
                cerr << "non-integer/nulls" << endl;
                cerr << "numReals = " << column.columnTypes.numReals << endl;
                cerr << "numStrings = " << column.columnTypes.numStrings << endl;
                cerr << "numBlobs = " << column.columnTypes.numBlobs << endl;
                cerr << "numPaths = " << column.columnTypes.numPaths << endl;
                cerr << "numOther = " << column.columnTypes.numOther << endl;
#endif
                return;  // can't use this column type
            }
            if (column.columnTypes.maxPositiveInteger
                > (uint64_t)std::numeric_limits<int64_t>::max()) {
                cerr << "out of range" << endl;
                return;  // out of range
            }

            if (column.columnTypes.hasPositiveIntegers()
                && column.columnTypes.hasNegativeIntegers()) {
                range = column.columnTypes.maxPositiveInteger
                    - column.columnTypes.minNegativeInteger;
                offset = column.columnTypes.minNegativeInteger;
            }
            else if (column.columnTypes.hasPositiveIntegers()) {
                range = column.columnTypes.maxPositiveInteger
                    - column.columnTypes.minPositiveInteger;
                offset = column.columnTypes.minPositiveInteger;
            }
            else if (column.columnTypes.hasNegativeIntegers()) {
                range = column.columnTypes.maxNegativeInteger
                    - column.columnTypes.minNegativeInteger;
                offset = column.columnTypes.minNegativeInteger;
            }
            else {
                // only nulls or empty column; we can store another way
                return;
            }

            numEntries = column.maxRowNumber - column.minRowNumber + 1;
            hasNulls = column.sparseIndexes.size() < numEntries;

            // If we have too much range to represent nulls then we can't
            // use this kind of column.
            if (range == -1 && hasNulls)
                return;

            uint64_t doneRows = 0;
            for (auto & v: column.sparseIndexes) {
                uint32_t rowNumber = v.first;
                const CellValue & val = column.indexedVals[v.second];
                uint64_t intVal = 0;
                if (!val.empty()) {
                    intVal = val.toInt() - offset + hasNulls;
                    ++numNonNullRows;
                }
                while (rowNumber < doneRows) {
                    table.add(0);  // for the null
                    ++doneRows;
                }
                table.add(intVal);
                ++doneRows;
            }

            // Handle nulls at the end
            while (doneRows < numEntries) {
                table.add(0);  // for the null
                ++doneRows;
            }

            this->bytesRequired = table.bytesRequired() + sizeof(IntegerFrozenColumn);

#if 0
            cerr << "table.size() = " << table.size() << endl;
            cerr << "hasNulls = " << hasNulls << endl;
            cerr << "numEntries = " << numEntries << endl;
            cerr << "bytes required = " << this->bytesRequired << endl;
#endif

            return;

#if 0 // later on... we should look for a common multiple to reduce bits used
   
            // Check for common multiple
            std::vector<int64_t> offsets;
            offsets.reserve(column.indexedVals.size());
            for (auto & v: column.indexedVals) {
                if (!v.empty())
                    offsets.emplace_back(v.toInt());
            }

            std::sort(offsets.begin(), offsets.end());
        
            // Find the multiple
            for (size_t i = 0;  i < offsets.size() - 1;  ++i) {
                offsets[i] = offsets[i + 1] - offsets[i];
            }
            if (!offsets.empty())
                offsets.pop_back();

            // Uniquify
            std::sort(offsets.begin(), offsets.end());
            offsets.erase(std::unique(offsets.begin(), offsets.end()),
                          offsets.end());
        
            static std::mutex mutex;
            std::unique_lock<std::mutex> guard(mutex);

            TRACE_MSG(logger) << "got " << offsets.size() << " unique offsets starting at "
                              << offsets.front();

            for (size_t i = 0;  i < 100 && i < offsets.size() - 1;  ++i) {
                TRACE_MSG(logger) << "  " << offsets[i];
            }
#endif

            entryBits = bitsToHoldCount(range + hasNulls);
            cerr << "entryBits = " << entryBits << endl;
            numWords = (entryBits * numEntries + 63) / 64;
            cerr << "numWords = " << numWords << endl;
            bytesRequired = sizeof(IntegerFrozenColumn) + numWords * 8;
            cerr << "sizeof(IntegerFrozenColumn) = "
                 << sizeof(IntegerFrozenColumn) << endl;
            cerr << "sizeof(FrozenColumn) = " << sizeof(FrozenColumn) << endl;
            cerr << "sizeof(ColumnTypes) = " << sizeof(ColumnTypes) << endl;
            cerr << "bytesReqired = " << bytesRequired << endl;
        }

        operator ssize_t () const
        {
            return bytesRequired;
        }

        ssize_t bytesRequired = -1;
        uint64_t range;
        int64_t offset;
        size_t numEntries;
        bool hasNulls;
        size_t numWords;
        int entryBits;
        uint32_t numNonNullRows = 0;

        MutableIntegerTable table;
    };
    
    IntegerFrozenColumn(TabularDatasetColumn & column,
                        SizingInfo & info,
                        MappedSerializer & serializer)
        : columnTypes(std::move(column.columnTypes))
    {
        ExcAssertNotEqual(info.bytesRequired, -1);

        this->firstEntry = column.minRowNumber;
        this->hasNulls = info.hasNulls;

        this->table = info.table.freeze(serializer);
        this->offset = info.offset;
        this->numNonNullRows = info.numNonNullRows;
    }
    
    CellValue decode(uint64_t val) const
    {
        return (val == 0 && hasNulls)
            ? CellValue()
            : CellValue(int64_t(val) + offset - hasNulls);
            
    }

    bool forEachImpl(const ForEachRowFn & onRow, bool keepNulls) const
    {
        auto onRow2 = [&] (size_t i, uint64_t val) -> bool
            {
                CellValue decoded = decode(val);
                //cerr << "decoding " << val << " at entry " << i << " gave "
                //     << decoded << endl;
                if (decoded.empty() && !keepNulls)
                    return true;
                return onRow(i + firstEntry, decoded);
            };

        return table.forEach(onRow2);
    }
    
    virtual std::string format() const
    {
        return "I";
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= table.size())
            return result;
        return decode(table.get(rowIndex));
    }

    virtual size_t size() const
    {
        return table.size();
    }

    virtual size_t memusage() const
    {
        return sizeof(*this) + table.memusage();
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        auto onVal = [&] (uint64_t val) -> bool
            {
                return fn(decode(val));
            };

        return table.forEachDistinctValue(onVal);
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullRows;
    }

    FrozenIntegerTable table;
    bool hasNulls = false;
    uint64_t firstEntry = 0;
    int64_t offset = 0;
    uint32_t numNonNullRows = 0;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer) const
    {
        // TODO: finish
        table.serialize(serializer);
    }
};

struct IntegerFrozenColumnFormat: public FrozenColumnFormat {
    
    virtual ~IntegerFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "I";
    }

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        return column.columnTypes.onlyIntegersAndNulls()
            && column.columnTypes.maxPositiveInteger
            <= (uint64_t)std::numeric_limits<int64_t>::max();
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        auto info = std::make_shared<IntegerFrozenColumn::SizingInfo>(column);
        size_t result = info->bytesRequired;
        cachedInfo = info;
        return result;
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        auto infoCast
            = std::static_pointer_cast<IntegerFrozenColumn::SizingInfo>
            (std::move(cachedInfo));
        return new IntegerFrozenColumn(column, *infoCast, serializer);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

RegisterFrozenColumnFormatT<IntegerFrozenColumnFormat> regInteger;


/*****************************************************************************/
/* DOUBLE FROZEN COLUMN                                                     */
/*****************************************************************************/

/// Frozen column that stores each value as a signed 64 bit double
struct DoubleFrozenColumn: public FrozenColumn {

    struct SizingInfo {
        SizingInfo(const TabularDatasetColumn & column)
        {
            if (!column.columnTypes.onlyDoublesAndNulls())
                return;  // can't use this column type
            numEntries = column.maxRowNumber - column.minRowNumber + 1;
            hasNulls = column.sparseIndexes.size() < numEntries;

            bytesRequired = sizeof(DoubleFrozenColumn) + numEntries * sizeof(Entry);
        }

        operator ssize_t () const
        {
            return bytesRequired;
        }

        ssize_t bytesRequired = -1;
        size_t numEntries;
        bool hasNulls;
    };
    
    struct Entry {
        
        Entry()
            : val(NULL_BITS)
        {
        }

        Entry(double d)
        {
            U u { d: d };
            val = u.bits;
        }

        uint64_le val;

        static const uint64_t NULL_BITS
            = 0ULL  << 63 // sign
            | (0x7ffULL << 53) // exponent is all 1s for NaN
            | (0xe1a1ULL); // mantissa

        // Type-punning union declared once here so we don't need to
        // do so everywhere else anonymously.
        union U {
            double d;
            uint64_t bits;
        };

        bool isNull() const
        {
            return val == NULL_BITS;
        }

        double value() const
        {
            U u { bits: val };
            return u.d;
        }

        operator CellValue() const
        {
            return isNull() ? CellValue() : value();
        }
    };

    DoubleFrozenColumn(TabularDatasetColumn & column,
                       MappedSerializer & serializer)
        : columnTypes(column.columnTypes)
    {
        SizingInfo info(column);
        ExcAssertNotEqual(info.bytesRequired, -1);

        firstEntry = column.minRowNumber;
        numEntries = info.numEntries;

        MutableMemoryRegionT<Entry> mutableData
            = serializer.allocateWritableT<Entry>(info.numEntries);

        // Check it's really feasible
        ExcAssert(column.columnTypes.onlyDoublesAndNulls());
        Entry * data = mutableData.data();
        
        std::fill(data, data + info.numEntries, Entry());

        for (auto & r_i: column.sparseIndexes) {
            const CellValue & v = column.indexedVals[r_i.second];
            if (!v.empty()) {
                ++numNonNullRows;
                data[r_i.first] = v.toDouble();
            }
        }

        this->storage = mutableData.freeze();
    }

    bool forEachImpl(const ForEachRowFn & onRow, bool keepNulls) const
    {
        for (size_t i = 0;  i < numEntries;  ++i) {
            const Entry & entry = storage[i];
            if (!keepNulls && entry.isNull())
                continue;
            if (!onRow(i + firstEntry, entry))
                return false;
        }

        return true;
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= numEntries)
            return result;
        return storage[rowIndex];
    }

    virtual size_t size() const
    {
        return numEntries;
    }

    virtual size_t memusage() const
    {
        size_t result
            = sizeof(*this)
            + (sizeof(Entry) * numEntries);

        return result;
    }

    template<typename Float>
    struct safe_less {
        bool operator () (Float v1, Float v2) const
        {
            bool nan1 = std::isnan(v1), nan2 = std::isnan(v2);
            return (nan1 > nan2)
                || ((nan1 == nan2) && v1 < v2);
        }
    };

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        bool hasNulls = false;

        std::vector<double> allVals;
        allVals.reserve(numEntries);

        for (size_t i = 0;  i < numEntries;  ++i) {
            const Entry & entry = storage[i];
            if (entry.isNull())
                hasNulls = true;
            else {
                allVals.emplace_back(entry.value());
            }
        }

        // Handle nulls first so we don't have to do them later
        if (hasNulls && !fn(CellValue()))
            return false;

        /** Like std::less<Float>, but has a well defined order for nan
            values, which allows us to sort ranges that might contain
            nan values without crashing.
        */
        std::sort(allVals.begin(), allVals.end(), safe_less<double>());
        auto endIt = std::unique(allVals.begin(), allVals.end());
        
        for (auto it = allVals.begin();  it != endIt;  ++it) {
            if (!fn(*it))
                return false;
        }
        
        return true;
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullRows;
    }

    FrozenMemoryRegionT<Entry> storage;

    uint32_t numEntries = 0;
    uint64_t firstEntry = 0;
    uint32_t numNonNullRows = 0;

    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    static ssize_t bytesRequired(const TabularDatasetColumn & column)
    {
        return SizingInfo(column);
    }

    virtual std::string format() const
    {
        return "D";
    }

    virtual void serialize(MappedSerializer & serializer) const
    {
        throw HttpReturnException(600, "DoubleFrozenColumn::serialize()");
    }
};

struct DoubleFrozenColumnFormat: public FrozenColumnFormat {
    
    virtual ~DoubleFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "D";
    }

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        return column.columnTypes.onlyDoublesAndNulls();
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        return DoubleFrozenColumn::bytesRequired(column);
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new DoubleFrozenColumn(column, serializer);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

RegisterFrozenColumnFormatT<DoubleFrozenColumnFormat> regDouble;


/*****************************************************************************/
/* TIMESTAMP FROZEN COLUMN                                                   */
/*****************************************************************************/

/// Frozen column that stores each value as a timestamp
struct TimestampFrozenColumn: public FrozenColumn {

    // This stores the underlying doubles or CellValues 
    std::shared_ptr<const FrozenColumn> unwrapped;

    TimestampFrozenColumn(TabularDatasetColumn & column,
                          MappedSerializer & serializer,
                          const ColumnFreezeParameters & params)
        : columnTypes(column.columnTypes)
    {
        ExcAssert(!column.isFrozen);
        // Convert the values to unwrapped doubles
        column.valueIndex.clear();
        size_t numNulls = column.columnTypes.numNulls;
        column.columnTypes = ColumnTypes();
        for (auto & v: column.indexedVals) {
            v = v.coerceToNumber();
            column.columnTypes.update(v);
        }
        column.columnTypes.numNulls = numNulls;

        unwrapped = column.freeze(serializer, params);
    }

    // Wrap a double (or null) into a timestamp (or null)
    static CellValue wrap(CellValue val)
    {
        if (val.empty())
            return val;
        return val.coerceToTimestamp();
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        auto onRow2 = [&] (size_t rowNum, const CellValue & val)
            {
                return onRow(rowNum, wrap(val));
            };

        return unwrapped->forEach(onRow2);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        auto onRow2 = [&] (size_t rowNum, const CellValue & val)
            {
                return onRow(rowNum, wrap(val));
            };

        return unwrapped->forEachDense(onRow2);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        return wrap(unwrapped->get(rowIndex));
    }

    virtual size_t size() const
    {
        return unwrapped->size();
    }

    virtual size_t memusage() const
    {
        return sizeof(*this)
            + unwrapped->memusage();
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        auto fn2 = [&] (const CellValue & v)
            {
                return fn(wrap(v));
            };

        return unwrapped->forEachDistinctValue(fn2);
    }

    virtual size_t nonNullRowCount() const override
    {
        return unwrapped->nonNullRowCount();
    }

    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual std::string format() const
    {
        return "T";
    }

    virtual void serialize(MappedSerializer & serializer) const
    {
        unwrapped->serialize(serializer);
    }
};

struct TimestampFrozenColumnFormat: public FrozenColumnFormat {
    
    virtual ~TimestampFrozenColumnFormat()
    {
    }

    virtual std::string format() const override
    {
        return "Timestamp";
    }

    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const override
    {
        return column.columnTypes.numTimestamps
            && column.columnTypes.onlyTimestampsAndNulls();
    }

    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const override
    {
        // Worst case is 8 bytes per timestamp for a double column
        return sizeof(TimestampFrozenColumn) + 8 * (column.maxRowNumber - column.minRowNumber);
    }
    
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new TimestampFrozenColumn(column, serializer, params);
    }

    virtual FrozenColumn *
    reconstitute(MappedReconstituter & reconstituter) const override
    {
        throw HttpReturnException(600, "Tabular reconstitution not finished");
    }
};

RegisterFrozenColumnFormatT<TimestampFrozenColumnFormat> regTimestamp;


/*****************************************************************************/
/* FROZEN COLUMN FORMAT                                                      */
/*****************************************************************************/

namespace {

typedef std::map<std::string, std::shared_ptr<FrozenColumnFormat> > Formats;

atomic_shared_ptr<const Formats> & getFormats()
{
    static atomic_shared_ptr<const Formats> formats
        (std::make_shared<Formats>());
    return formats;
}


} // file scope

FrozenColumnFormat::
~FrozenColumnFormat()
{
}

std::shared_ptr<void>
FrozenColumnFormat::
registerFormat(std::shared_ptr<FrozenColumnFormat> format)
{
    std::string name = format->format();
    auto & formats = getFormats();
    for (;;) {
        auto ptr = formats.load();
        if (ptr->count(name)) {
            throw HttpReturnException
                (500, "Attempt to double-register frozen column format "
                 + name);
        }
        auto newFormats = *ptr;
        newFormats.emplace(name, format);
        auto newFormatsPtr
            = std::make_shared<Formats>(std::move(newFormats));
        if (formats.compare_exchange_strong(ptr, newFormatsPtr)) {

            auto deregister = [name] (void *)
                {
                    auto & formats = getFormats();
                    for (;;) {
                        auto ptr = formats.load();
                        auto newFormats = *ptr;
                        newFormats.erase(name);
                        auto newFormatsPtr
                            = std::make_shared<Formats>(std::move(newFormats));
                        if (formats.compare_exchange_strong(ptr, newFormatsPtr))
                            break;
                    }
                };

            return std::shared_ptr<void>(format.get(), deregister);
        }
    }
}


/*****************************************************************************/
/* FROZEN COLUMN                                                             */
/*****************************************************************************/

FrozenColumn::
FrozenColumn()
{
}

std::pair<ssize_t, std::function<std::shared_ptr<FrozenColumn>
                                 (TabularDatasetColumn & column,
                                  MappedSerializer & Serializer)> >
FrozenColumnFormat::
preFreeze(const TabularDatasetColumn & column,
          const ColumnFreezeParameters & params)
{
    // Get the current list of formats
    auto formats = getFormats().load();
    
    ssize_t bestBytes = FrozenColumnFormat::NOT_BEST;
    const FrozenColumnFormat * bestFormat = nullptr;
    std::shared_ptr<void> bestData;

#if 0
    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);
#endif

    for (auto & f: *formats) {
        std::shared_ptr<void> data;
        if (f.second->isFeasible(column, params, data)) {
            ssize_t bytes = f.second->columnSize(column, params, bestBytes,
                                                 data);
            //cerr << "format " << f.first << " took " << bytes << endl;

            if (bytes >= 0 && (bestBytes < 0 || bytes < bestBytes)) {
                bestFormat = f.second.get();
                bestData = std::move(data);
                bestBytes = bytes;
            }
        }
    }

#if 0
    cerr << "chose format " << bestFormat->format() << " with "
         << column.indexedVals.size() << " unique and "
         << column.sparseIndexes.size() << " populated" << endl;
    for (size_t i = 0;  i < column.indexedVals.size() && i < 10;  ++i) {
        cerr << " " << column.indexedVals[i];
    }
    cerr << "...";
    for (ssize_t i = std::max<ssize_t>(10, column.indexedVals.size() - 10);
         i < column.indexedVals.size();
         ++i) {
        cerr << " " << column.indexedVals[i];
    }
    cerr << endl;
#endif

    if (bestFormat) {
        return std::make_pair(bestBytes,
                              [=] (TabularDatasetColumn & column,
                                   MappedSerializer & serializer)
                              {
                                  return std::shared_ptr<FrozenColumn>
                                      (bestFormat->freeze(column, serializer, params, bestData));
                              }
                              );
    }
    
    return std::make_pair(FrozenColumnFormat::NOT_BEST, nullptr);

}

std::shared_ptr<FrozenColumn>
FrozenColumn::
freeze(TabularDatasetColumn & column,
       MappedSerializer & serializer,
       const ColumnFreezeParameters & params)
{
    ExcAssert(!column.isFrozen);
    auto res = FrozenColumnFormat::preFreeze(column, params);
    if (!res.second) {
        throw HttpReturnException(500, "No column format found for column");
    }
    return res.second(column, serializer);
}


} // namespace MLDB


