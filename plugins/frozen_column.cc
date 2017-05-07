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
#include <mutex>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>


using namespace std;


namespace MLDB {

struct FrozenIntegerTable {
    int64_t getSigned(size_t i) const;
    uint64_t getUnsigned(size_t i) const;

    FrozenMemoryRegion data;

    size_t memusage() const
    {
        return data.memusage();
    }
};

struct MutableIntegerTable {
    uint64_t add(uint64_t val) const;
    uint64_t add(int64_t val) const;

    template<typename T>
    void add(T val, typename std::enable_if<std::is_unsigned<T>::value>::type * en
             = nullptr)
    {
        add((uint64_t)val);
    }

    template<typename T>
    void add(T val, typename std::enable_if<!std::is_unsigned<T>::value>::type * en
             = nullptr)
    {
        add((int64_t)val);
    }

    FrozenIntegerTable freeze(MappedSerializer & serializer);
};

struct FrozenBlobTable {
    uint64_t memusage() const
    {
        return mem.memusage() + offsets.memusage();
    }

    std::pair<const char *, size_t>
    operator [] (size_t index) const
    {
        uint64_t offset = offsets.getUnsigned(index);
        uint64_t length = offsets.getUnsigned(index + 1);
        return { mem.data() + offset, length };
    }

    FrozenMemoryRegion mem;
    FrozenIntegerTable offsets;
};

struct MutableBlobTable {
};

struct MutableStringTable {
    FrozenIntegerTable freeze(MappedSerializer & serializer);
};

struct FrozenCellValueTable {
#if 0 // for when it's really frozen...
    CellValue operator [] (size_t index) const
    {
        static uint8_t format = CellValue::serializationFormat(true /* known length */);
        const char * data;
        size_t len;
        std::tie(data, len) = blobs[index];
        return CellValue::reconstitute(data, len, format, true /* known length */).first;
    }

    uint64_t memusage() const
    {
        return blobs.memusage() + sizeof(*this);
    }

    FrozenBlobTable blobs;
#else
    CellValue operator [] (size_t index) const
    {
        return values.at(index);
    }

    uint64_t memusage() const
    {
        uint64_t result = 0;
        for (auto & v: values)
            result += v.memusage();
        return result;
    }
    
    std::vector<CellValue> values;
#endif
};

struct MutableCellValueTable {
    void reserve(size_t numValues)
    {
        values.reserve(numValues);
    }

    void add(CellValue val)
    {
        values.emplace_back(std::move(val));
    }

    void set(uint64_t index, CellValue val)
    {
        if (index >= values.size())
            values.resize(index + 1);
        values[index] = std::move(val);
    }

    FrozenCellValueTable
    freeze(MappedSerializer & serializer)
    {
        FrozenCellValueTable result;
        result.values = std::move(values);
        return result;
    }

    std::vector<CellValue> values;
};


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
    cerr << "allocate " << bytesRequired << " bytes" << endl;
        
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

        return std::shared_ptr<void>(allocated, [] (void *) {});
    }

    void createNewArena(size_t bytesRequired)
    {
        size_t newLength = std::max<size_t>
            ((bytesRequired + page_size - 1) / page_size,
             10000 * page_size);

        int res = ::ftruncate(fd, currentOffset + newLength);
        if (res == -1) {
            throw HttpReturnException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }

        void * addr = mmap(nullptr, newLength, PROT_READ | PROT_WRITE, MAP_SHARED,
                           fd, currentOffset);
        if (addr == nullptr) {
            throw HttpReturnException
                (400, "Failed to open memory map file: "
                 + string(strerror(errno)));
        }

        arenas.emplace_back(addr, currentOffset, newLength);

        currentOffset += newLength;
    }

    bool expandLastArena(size_t bytesRequired)
    {
        size_t newLength
            = arenas.back().length
            + std::max<size_t>((bytesRequired + page_size - 1) / page_size,
                               10000 * page_size);
        
        int res = ::ftruncate(fd, currentOffset + newLength);
        if (res == -1) {
            throw HttpReturnException
                (500, "ftruncate failed: " + string(strerror(errno)));
        }

        void * newAddr = mremap(arenas.back().addr,
                                arenas.back().length,
                                newLength,
                                0 /* flags */);

        if (newAddr != arenas.back().addr) {
            // undo the expansion
            ftruncate(fd, currentOffset);
            return false;
        }

        arenas.back().length = newLength;

        return true;
    }

    std::mutex mutex;
    Utf8String filename;
    int fd = -1;
    size_t mappedLength = 0;
    size_t currentOffset = 0;

    struct Arena {
        Arena(void * addr, size_t startOffset, size_t length)
            : addr(addr), startOffset(startOffset), length(length)
        {
        }

        ~Arena()
        {
            ::munmap(addr, length);
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

            char * data = reinterpret_cast<char *>(addr) + extraBytes + currentOffset;
            currentOffset += extraBytes + bytes;
            return data;
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

        values = mutableValues.freeze(serializer);
    }

    virtual std::string format() const
    {
        return "D";
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        for (size_t i = 0;  i < numEntries;  ++i) {
            if (!onRow(i + firstEntry, values[i]))
                return false;
        }

        return true;
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        return forEach(onRow);
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= numEntries)
            return result;
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
        throw HttpReturnException
            (600, "Tabular frozen column direct forEachDistinctValue");
    }

    uint32_t numEntries;
    uint64_t firstEntry;
    FrozenCellValueTable values;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer)
    {
        throw HttpReturnException(600, "DirectFrozenColumn::serialize()");
    }
};

struct DirectFrozenColumnFormat: public FrozenColumnFormat {

    virtual ~DirectFrozenColumnFormat()
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

/** How many bits are required to hold a number up from zero up to count - 1
    inclusive?
*/
static uint8_t bitsToHoldCount(size_t count)
{
    return ML::highest_bit(std::max<size_t>(count, 1) - 1, -1) + 1;
}


/*****************************************************************************/
/* TABLE FROZEN COLUMN                                                       */
/*****************************************************************************/

/// Frozen column that finds each value in a lookup table
/// Useful when there are lots of duplicates
struct TableFrozenColumn: public FrozenColumn {
    TableFrozenColumn(TabularDatasetColumn & column,
                      MappedSerializer & serializer)
        : table(std::move(column.indexedVals)),
          columnTypes(std::move(column.columnTypes))
    {
        firstEntry = column.minRowNumber;
        numEntries = column.maxRowNumber - column.minRowNumber + 1;
        hasNulls = column.sparseIndexes.size() < numEntries;
        indexBits = bitsToHoldCount(table.size() + hasNulls);
        size_t numWords = (indexBits * numEntries + 31) / 32;
        auto mutableStorage = serializer.allocateWritableT<uint32_t>(numWords);
        uint32_t * data = mutableStorage.data();

        cerr << "table: data is " << data << endl;

        if (!hasNulls) {
            // Contiguous rows
            ML::Bit_Writer<uint32_t> writer(data);
            for (size_t i = 0;  i < column.sparseIndexes.size();  ++i) {
                ExcAssertEqual(column.sparseIndexes[i].first, i);
                writer.write(column.sparseIndexes[i].second, indexBits);
            }
        }
        else {
            // Non-contiguous; leave gaps with a zero (null) value
            std::fill(data, data + numWords, 0);
            for (auto & r_i: column.sparseIndexes) {
                ML::Bit_Writer<uint32_t> writer(data);
                writer.skip(r_i.first * indexBits);
                writer.write(r_i.second + 1, indexBits);
            }
        }

        storage = mutableStorage.freeze();
    }

    virtual std::string format() const
    {
        return "T";
    }

    virtual bool forEachImpl(const ForEachRowFn & onRow,
                             bool keepNulls) const
    {
        ML::Bit_Extractor<uint32_t> bits(storage.data());

        for (size_t i = 0;  i < numEntries;  ++i) {
            int index = bits.extract<uint32_t>(indexBits);

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
        if (rowIndex >= numEntries)
            return result;
        ExcAssertLess(rowIndex, numEntries);
        ML::Bit_Extractor<uint32_t> bits(storage.data());
        bits.advance(rowIndex * indexBits);
        int index = bits.extract<uint32_t>(indexBits);
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
        size_t result
            = sizeof(*this)
            + (indexBits * numEntries + 31) / 8;

        for (auto & v: table)
            result += v.memusage();

        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        if (hasNulls) {
            if (!fn(CellValue()))
                return false;
        }
        for (auto & v: table) {
            if (!fn(v))
                return false;
        }

        return true;
    }

    FrozenMemoryRegionT<uint32_t> storage;
    uint32_t indexBits;
    uint32_t numEntries;
    uint64_t firstEntry;
    
    bool hasNulls;
    std::vector<CellValue> table;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer)
    {
        throw HttpReturnException(600, "TableFrozenColumn::serialize()");
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
        : table(column.indexedVals.size()),
          columnTypes(std::move(column.columnTypes))
    {
        firstEntry = column.minRowNumber;
        lastEntry = column.maxRowNumber;
        std::move(std::make_move_iterator(column.indexedVals.begin()),
                  std::make_move_iterator(column.indexedVals.end()),
                  table.begin());
        indexBits = bitsToHoldCount(table.size());
        rowNumBits = bitsToHoldCount(column.maxRowNumber - column.minRowNumber);
        numEntries = column.sparseIndexes.size();
        size_t numWords = ((indexBits + rowNumBits) * numEntries + 31) / 32;
        
        auto mutableStorage = serializer.allocateWritableT<uint32_t>(numWords);
        uint32_t * data = mutableStorage.data();
            
        ML::Bit_Writer<uint32_t> writer(data);
        for (auto & i: column.sparseIndexes) {
            writer.write(i.first, rowNumBits);
            ExcAssertLess(i.second, table.size());
            writer.write(i.second, indexBits);
        }

        if (logger->should_log(spdlog::level::debug)) {
            size_t mem = memusage();
            if (mem > 30000) {
                using namespace std;
                logger->debug() << "table with " << column.sparseIndexes.size()
                                << " entries from "
                                << column.minRowNumber << " to " << column.maxRowNumber
                                << " and " << table.size()
                                << " uniques takes " << mem << " memory";
                
                for (unsigned i = 0;  i < 5 && i < table.size();  ++i) {
                    logger->debug() << "  " << table[i];
                }
            }
        }

        storage = mutableStorage.freeze();
    }

    virtual std::string format() const
    {
        return "ST";
    }

    virtual bool forEach(const ForEachRowFn & onRow) const
    {
        ML::Bit_Extractor<uint32_t> bits(storage.data());

        for (size_t i = 0;  i < numEntries;  ++i) {
            uint32_t rowNum = bits.extract<uint32_t>(rowNumBits);
            uint32_t index = bits.extract<uint32_t>(indexBits);
            if (!onRow(rowNum + firstEntry, table[index]))
                return false;
        }
        
        return true;
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const
    {
        ML::Bit_Extractor<uint32_t> bits(storage.data());

        size_t lastRowNum = 0;
        for (size_t i = 0;  i < numEntries;  ++i) {
            uint32_t rowNum = bits.extract<uint32_t>(rowNumBits);
            uint32_t index = bits.extract<uint32_t>(indexBits);

            while (lastRowNum < rowNum) {
                if (!onRow(firstEntry + lastRowNum, CellValue()))
                    return false;
                ++lastRowNum;
            }

            if (!onRow(firstEntry + rowNum, table[index]))
                return false;
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
                ML::Bit_Extractor<uint32_t> bits(storage.data());
                bits.advance(n * (indexBits + rowNumBits));
                uint32_t rowNum = bits.extract<uint32_t>(rowNumBits);
                uint32_t index = bits.extract<uint32_t>(indexBits);
                return std::make_pair(rowNum, index);
            };

        uint32_t first = 0;
        uint32_t last  = numEntries;

        while (first != last) {
            uint32_t middle = (first + last) / 2;
            uint32_t rowNum, index;
            std::tie(rowNum, index) = getAtIndex(middle);

            TRACE_MSG(logger) << "first = " << first << " middle = " << middle
                              << " last = " << last << " rowNum = " << rowNum
                              << " looking for " << rowIndex;

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
        return numEntries;
    }

    virtual size_t memusage() const
    {
        size_t result
            = sizeof(*this)
            + ((indexBits + rowNumBits) * numEntries + 31) / 8;

        for (auto & v: table)
            result += v.memusage();

        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        if (!fn(CellValue()))
            return false;
        for (auto & v: table) {
            if (!fn(v))
                return false;
        }
        
        return true;
    }

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer)
    {
        throw HttpReturnException(600, "SparseTableFrozenColumn::serialize()");
    }

    FrozenMemoryRegionT<uint32_t> storage;
    compact_vector<CellValue, 0> table;
    uint8_t rowNumBits;
    uint8_t indexBits;
    uint32_t numEntries;
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
            numWords = (entryBits * numEntries + 63) / 64;
            bytesRequired = sizeof(IntegerFrozenColumn) + numWords * 8;
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
    };
    
    IntegerFrozenColumn(TabularDatasetColumn & column,
                        const SizingInfo & info,
                        MappedSerializer & serializer)
        : columnTypes(std::move(column.columnTypes))
    {
        ExcAssertNotEqual(info.bytesRequired, -1);

        firstEntry = column.minRowNumber;
        numEntries = info.numEntries;

        // Check it's really feasible
        ExcAssert(column.columnTypes.onlyIntegersAndNulls());
        ExcAssertLessEqual(column.columnTypes.maxPositiveInteger,
                           (uint64_t)std::numeric_limits<int64_t>::max());

        hasNulls = info.hasNulls;
        entryBits = info.entryBits;
        offset = info.offset;

        auto mutableStorage = serializer.allocateWritableT<uint64_t>(info.numWords);
        uint64_t * data = mutableStorage.data();

        if (!hasNulls) {
            // Contiguous rows
            DEBUG_MSG(logger) << "fill with contiguous";
            ML::Bit_Writer<uint64_t> writer(data);
            for (size_t i = 0;  i < column.sparseIndexes.size();  ++i) {
                ExcAssertEqual(column.sparseIndexes[i].first, i);
                int64_t val
                    = column.indexedVals[column.sparseIndexes[i].second].toInt();
                DEBUG_MSG(logger) << "writing " << val << " - " << offset << " = "
                                  << val - offset << " at " << i;
                writer.write(val - offset, entryBits);
            }
        }
        else {
            // Non-contiguous; leave gaps with a zero (null) value
            std::fill(data, data + info.numWords, 0);
            for (auto & r_i: column.sparseIndexes) {
                int64_t val
                    = column.indexedVals[r_i.second].toInt();
                ML::Bit_Writer<uint64_t> writer(data);
                writer.skip(r_i.first * entryBits);
                writer.write(val - offset + 1, entryBits);
            }
        }

        storage = mutableStorage.freeze();
#if 0
        // Check that we got the right thing
        for (auto & i: column.sparseIndexes) {
            DEBUG_MSG(logger) << "getting " << i.first << " with value "
                              << column.indexedVals.at(i.second);
            ExcAssertEqual(get(i.first + firstEntry),
                           column.indexedVals.at(i.second));
        }
#endif
    }

    bool forEachImpl(const ForEachRowFn & onRow, bool keepNulls) const
    {
        ML::Bit_Extractor<uint64_t> bits(storage.data());

        for (size_t i = 0;  i < numEntries;  ++i) {
            int64_t val = bits.extract<uint64_t>(entryBits);
            if (hasNulls) {
                if (val == 0) {
                    if (keepNulls && (!onRow(i + firstEntry, CellValue())))
                        return false;
                }
                else {
                    if (!onRow(i + firstEntry, val + offset - 1))
                        return false;
                }
            }
            else {
                if (!onRow(i + firstEntry, val + offset))
                    return false;
            }
        }

        return true;

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
        if (rowIndex >= numEntries)
            return result;
        ExcAssertLess(rowIndex, numEntries);
        ML::Bit_Extractor<uint64_t> bits(storage.data());
        bits.advance(rowIndex * entryBits);
        int64_t val = bits.extract<uint64_t>(entryBits);
        if (hasNulls) {
            if (val == 0)
                return result;
            else return result = val + offset - 1;
        }
        else {
            return result = val + offset;
        }
    }

    virtual size_t size() const
    {
        return numEntries;
    }

    virtual size_t memusage() const
    {
        size_t result
            = sizeof(*this)
            + (entryBits * numEntries + 63) / 8;

        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const
    {
        // Handle nulls first so we don't have to do them later
        if (hasNulls && !fn(CellValue()))
            return false;

        std::vector<int64_t> allVals;
        allVals.reserve(numEntries);

        ML::Bit_Extractor<uint64_t> bits(storage.data());
        
        for (size_t i = 0;  i < numEntries;  ++i) {
            int64_t val = bits.extract<uint64_t>(entryBits);
            if (val == 0 && hasNulls)
                continue;
            allVals.push_back(val);
        }

        std::sort(allVals.begin(), allVals.end());
        auto endIt = std::unique(allVals.begin(), allVals.end());

        for (auto it = allVals.begin();  it != endIt;  ++it) {
            if (!fn(*it + offset - hasNulls))
                return false;
        }

        return true;
    }

    FrozenMemoryRegionT<uint64_t> storage;
    uint32_t entryBits;
    uint32_t numEntries;
    uint64_t firstEntry;
    int64_t offset;

    bool hasNulls;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual void serialize(MappedSerializer & serializer)
    {
        throw HttpReturnException(600, "IntegerFrozenColumn::serialize()");
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

        uint64_t val;

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

    DoubleFrozenColumn(TabularDatasetColumn & column)
        : columnTypes(column.columnTypes)
    {
        SizingInfo info(column);
        ExcAssertNotEqual(info.bytesRequired, -1);

        firstEntry = column.minRowNumber;
        numEntries = info.numEntries;

        // Check it's really feasible
        ExcAssert(column.columnTypes.onlyDoublesAndNulls());
        Entry * data = new Entry[info.numEntries];
        storage = std::shared_ptr<Entry>(data, [] (Entry * p) { delete[] p; });

        for (auto & r_i: column.sparseIndexes) {
            const CellValue & v = column.indexedVals[r_i.second];
            if (!v.empty()) {
                data[r_i.first] = v.toDouble();
            }
        }
    }

    bool forEachImpl(const ForEachRowFn & onRow, bool keepNulls) const
    {
        for (size_t i = 0;  i < numEntries;  ++i) {
            const Entry & entry = storage.get()[i];
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
        return storage.get()[rowIndex];
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
            const Entry & entry = storage.get()[i];
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

    std::shared_ptr<const Entry> storage;
    uint32_t numEntries;
    uint64_t firstEntry;

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

    virtual void serialize(MappedSerializer & serializer)
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
        return "Double";
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
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new DoubleFrozenColumn(column);
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
        
        unwrapped = column.freeze(params);
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

    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }

    virtual std::string format() const
    {
        return "T";
    }

    virtual void serialize(MappedSerializer & serializer)
    {
        throw HttpReturnException(600, "TimestampFrozenColumn::serialize()");
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
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new TimestampFrozenColumn(column, params);
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
    : logger(getMldbLog<TabularDataset>()) // this class is only used by the tabular dataset
{
}

std::pair<ssize_t, std::function<std::shared_ptr<FrozenColumn> (TabularDatasetColumn & column)> >
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
                              [=] (TabularDatasetColumn & column)
                              {
                                  return std::shared_ptr<FrozenColumn>
                                      (bestFormat->freeze(column, params, bestData));
                              }
                              );
    }
    
    return std::make_pair(FrozenColumnFormat::NOT_BEST, nullptr);

}

std::shared_ptr<FrozenColumn>
FrozenColumn::
freeze(TabularDatasetColumn & column,
       const ColumnFreezeParameters & params)
{
    ExcAssert(!column.isFrozen);
    auto res = FrozenColumnFormat::preFreeze(column, params);
    if (!res.second) {
        throw HttpReturnException(500, "No column format found for column");
    }
    return res.second(column);
}


} // namespace MLDB


