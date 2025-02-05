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
#include "mldb/utils/lightweight_hash.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/utils/atomic_shared_ptr.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/arch/vm.h"
#include "mldb/arch/endian.h"
#include "mldb/vfs/filter_streams.h"
#include "frozen_tables.h"
#include <mutex>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* FROZEN COLUMN                                                             */
/*****************************************************************************/

void
FrozenColumn::
reconstituteMetadataHelper(StructuredReconstituter & reconstituter,
                           void * md,
                           const std::type_info & mdType)
{
    FrozenMemoryRegion mdRegion = reconstituter.getRegion("md");

    Utf8StringJsonParsingContext context
        (mdRegion.data(), mdRegion.length(),
         (reconstituter.getContext() + "/md").rawString());
        
    std::shared_ptr<const ValueDescription> desc
        = ValueDescription::get(mdType);
    ExcAssert(desc);

    int version = 0;
    std::shared_ptr<void> mdObject;
    
    auto onMember = [&] ()
        {
            if (context.inField("fmt")) {
                context.skip();
            }
            else if (context.inField("type")) {
                context.skip();
                //std::string structType = context.expectStringAscii();
                // TODO: make sure it matches
                //desc = ValueDescription::getType(structType);
                // TODO: get the correct version too...
            }
            else if (context.inField("ver")) {
                version = context.expectInt();
            }
            else if (context.inField("data")) {
                desc->parseJson(md, context);
            }
        };
        
    context.forEachMember(onMember);
}


/*****************************************************************************/
/* DIRECT FROZEN COLUMN                                                      */
/*****************************************************************************/

/// Frozen column that simply stores the values directly
/// No deduplication is done

struct DirectFrozenColumnMetadata {
    uint32_t numEntries = 0;
    uint64_t firstEntry = 0;
    uint32_t numNonNullEntries = 0;
    ColumnTypes columnTypes;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(DirectFrozenColumnMetadata)
{
    setVersion(1);
    addField("numEntries", &DirectFrozenColumnMetadata::numEntries, "");
    addField("firstEntry", &DirectFrozenColumnMetadata::firstEntry, "");
    addField("numNonNullEntries", &DirectFrozenColumnMetadata::numNonNullEntries, "");
    addField("columnTypes", &DirectFrozenColumnMetadata::columnTypes, "");
}

struct DirectFrozenColumn
    : public FrozenColumn,
      public DirectFrozenColumnMetadata {
    DirectFrozenColumn(TabularDatasetColumn & column,
                       MappedSerializer & serializer)
    {
        this->columnTypes = std::move(column.columnTypes);
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

    virtual std::string format() const override
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

    virtual bool forEach(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const override
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

    virtual size_t size() const override
    {
        return numEntries;
    }

    virtual size_t memusage() const override
    {
        size_t result
            = sizeof(*this);

        result += values.memusage();

        //cerr << "Direct memusage is " << result << " for " 
        //     << numEntries << " entries at "
        //     << 1.0 * values.memusage() / numEntries << " per entry"
        //     << endl;
        
        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const override
    {
        bool doneNull = false;
        auto fn2 = [&] (const CellValue & val)
            {
                if (val.empty())
                    doneNull = true;
                return fn(val);
            };
        if (!values.forEachDistinctValue(fn2))
            return false;

        // Trailing nulls?
        if (values.size() < numEntries && !doneNull) {
            return fn(CellValue());
        }
       
        return true;
    }

    virtual bool
    forEachDistinctValueWithRowCount(std::function<bool (const CellValue &, size_t)> fn) const override
    {
        auto fn2 = [&] (const CellValue & val, size_t n)
            {
                if (val.empty())
                    return true;
                return fn(val, n);
            };

        return values.forEachDistinctValueWithRowCount(fn2);
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullEntries;
    }

    FrozenCellValueTable values;

    virtual ColumnTypes getColumnTypes() const override
    {
        return columnTypes;
    }

    virtual void serialize(StructuredSerializer & serializer) const override
    {
        serializeMetadataT<DirectFrozenColumnMetadata>(serializer, *this);
        values.serialize(*serializer.newStructure("values"));
    }

    DirectFrozenColumn(StructuredReconstituter & reconstituter)
    {
        reconstituteMetadataT<DirectFrozenColumnMetadata>(reconstituter, *this);
        values.reconstitute(*reconstituter.getStructure("values"));
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
    
    virtual DirectFrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new DirectFrozenColumn(column, serializer);
    }

    virtual DirectFrozenColumn *
    reconstitute(StructuredReconstituter & reconstituter) const override
    {
        return new DirectFrozenColumn(reconstituter); 
    }
};

RegisterFrozenColumnFormatT<DirectFrozenColumnFormat> regDirect;



/*****************************************************************************/
/* TABLE FROZEN COLUMN                                                       */
/*****************************************************************************/

struct TableFrozenColumnMetadata {
    uint32_t numEntries = 0;
    uint64_t firstEntry = 0;
    uint32_t numNonNullEntries = 0;
    bool hasNulls = false;
    ColumnTypes columnTypes;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(TableFrozenColumnMetadata)
{
    setVersion(1);
    addField("numEntries", &TableFrozenColumnMetadata::numEntries, "");
    addField("firstEntry", &TableFrozenColumnMetadata::firstEntry, "");
    addField("numNonNullEntries", &TableFrozenColumnMetadata::numNonNullEntries, "");
    addField("hasNulls", &TableFrozenColumnMetadata::hasNulls, "");
    addField("columnTypes", &TableFrozenColumnMetadata::columnTypes, "");
}

/// Frozen column that finds each value in a lookup table
/// Useful when there are lots of duplicates
struct TableFrozenColumn
    : public FrozenColumn,
      public TableFrozenColumnMetadata {
    TableFrozenColumn(TabularDatasetColumn & column,
                      MappedSerializer & serializer)
    {
        this->columnTypes = std::move(column.columnTypes);
        MutableCellValueSet mutableTable
            (std::make_move_iterator(column.indexedVals.begin()),
             std::make_move_iterator(column.indexedVals.end()));

        std::vector<uint32_t> remapping;

        // Freezing is allowed to reorder them for efficiency, so we
        // need to also keep a table remapping old indexes to new ones
        std::tie(table, remapping)
            = mutableTable.freeze(serializer);

        firstEntry = column.minRowNumber;
        numEntries = column.maxRowNumber - column.minRowNumber + 1;
        hasNulls = column.sparseIndexes.size() < numEntries;

        MutableIntegerTable mutableIndexes;
        mutableIndexes.reserve(numEntries);
        
        if (!hasNulls) {
            // Contiguous rows
            for (size_t i = 0;  i < column.sparseIndexes.size();  ++i) {
                ExcAssertEqual(column.sparseIndexes[i].first, i);
                mutableIndexes.add(remapping.at(column.sparseIndexes[i].second));
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
                mutableIndexes.add(remapping.at(r_i.second) + 1);
                ++index;
            }
        }

        numNonNullEntries = column.sparseIndexes.size();
        indexes = mutableIndexes.freeze(serializer);
    }

    virtual std::string format() const override
    {
        return "T";
    }

    virtual bool forEachImpl(const ForEachRowFn & onRow,
                             bool keepNulls) const
    {
        for (size_t i = 0;  i < numEntries;  ++i) {
            uint64_t index = indexes.getDefault(i, 0);

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

    virtual bool forEach(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const override
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

    virtual size_t size() const override
    {
        return numEntries;
    }

    virtual size_t memusage() const override
    {
        size_t result = sizeof(*this);
        result += table.memusage();
        result += indexes.memusage();

        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const override
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

    virtual bool
    forEachDistinctValueWithRowCount(std::function<bool (const CellValue &, size_t)> fn) const override
    {
        auto onValue = [&] (auto index, size_t count)
        {
            if (hasNulls) {
                if (index == 0)
                    return true;  // null
                index -= 1;
            }
            return fn(table[index], count);
        };

        return indexes.forEachDistinctValueWithRowCount(onValue);
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullEntries;
    }

    FrozenIntegerTable indexes;
    FrozenCellValueSet table;

    virtual ColumnTypes getColumnTypes() const override
    {
        return columnTypes;
    }

    virtual void serialize(StructuredSerializer & serializer) const override
    {
        serializeMetadataT<TableFrozenColumnMetadata>(serializer, *this);
        indexes.serialize(*serializer.newStructure("ix"));
        table.serialize(*serializer.newStructure("t"));
    }

    /// Reconstitute constructor
    TableFrozenColumn(StructuredReconstituter & reconstituter)
    {
        reconstituteMetadataT<TableFrozenColumnMetadata>(reconstituter, *this);
        indexes.reconstitute(*reconstituter.getStructure("ix"));
        table.reconstitute(*reconstituter.getStructure("t"));
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
    
    virtual TableFrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new TableFrozenColumn(column, serializer);
    }

    virtual TableFrozenColumn *
    reconstitute(StructuredReconstituter & reconstituter) const override
    {
        return new TableFrozenColumn(reconstituter);
    }
};

RegisterFrozenColumnFormatT<TableFrozenColumnFormat> regTable;


/*****************************************************************************/
/* SPARSE TABLE FROZEN COLUMN                                                */
/*****************************************************************************/

struct SparseTableFrozenColumnMetadata {
    size_t firstEntry = 0;
    size_t lastEntry = 0;  // WARNING: this is the number, not number + 1
    ColumnTypes columnTypes;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(SparseTableFrozenColumnMetadata)
{
    setVersion(1);
    addField("firstEntry", &SparseTableFrozenColumnMetadata::firstEntry, "");
    addField("lastEntry", &SparseTableFrozenColumnMetadata::lastEntry, "");
    addField("columnTypes", &SparseTableFrozenColumnMetadata::columnTypes, "");
}

/// Sparse frozen column that finds each value in a lookup table
struct SparseTableFrozenColumn
    : public FrozenColumn,
      public SparseTableFrozenColumnMetadata {

    SparseTableFrozenColumn(TabularDatasetColumn & column,
                            MappedSerializer & serializer)
    {
        columnTypes = std::move(column.columnTypes);
        firstEntry = column.minRowNumber;
        lastEntry = column.maxRowNumber;

        MutableCellValueSet mutableTable;
        mutableTable.reserve(column.indexedVals.size());

        for (auto & v: column.indexedVals) {
            mutableTable.add(v);
        }

        std::vector<uint32_t> remapping;
        std::tie(this->table, remapping)
            = mutableTable.freeze(serializer);

        MutableIntegerTable mutableRowNum, mutableIndex;
        mutableRowNum.reserve(column.sparseIndexes.size());
        mutableIndex.reserve(column.sparseIndexes.size());

        for (auto & i: column.sparseIndexes) {
            mutableRowNum.add(i.first);
            ExcAssertLess(i.second, table.size());
            mutableIndex.add(remapping.at(i.second));
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

    virtual std::string format() const override
    {
        return "ST";
    }

    virtual bool forEach(const ForEachRowFn & onRow) const override
    {
        for (size_t i = 0;  i < numEntries();  ++i) {
            auto rowNum = this->rowNum.get(i);
            auto index = this->index.get(i);
            if (!onRow(rowNum + firstEntry, table[index]))
                return false;
        }
        
        return true;
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const override
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

    virtual CellValue get(uint32_t rowIndex) const override
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

    virtual size_t size() const override
    {
        return lastEntry - firstEntry + 1;
    }

    virtual size_t memusage() const override
    {
        size_t result
            = sizeof(*this);

        result += table.memusage();
        result += index.memusage();
        result += rowNum.memusage();
        
        return result;
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const override
    {
        // Detect nulls which implicitly means a gap in the indexes
        if (firstEntry + numEntries() != lastEntry + 1) {
            if (!fn(CellValue()))
                return false;
        }

        return table.forEachDistinctValue(fn);
    }

    virtual bool
    forEachDistinctValueWithRowCount(std::function<bool (const CellValue &, size_t)> fn) const override
    {
        auto onValue = [&] (auto value, auto count)
        {
            return fn(table[value], count);
        };
        return index.forEachDistinctValueWithRowCount(onValue);
    }

    virtual size_t nonNullRowCount() const override
    {
        return numEntries();
    }

    virtual ColumnTypes getColumnTypes() const override
    {
        return columnTypes;
    }

    virtual void serialize(StructuredSerializer & serializer) const override
    {
        serializeMetadataT<SparseTableFrozenColumnMetadata>(serializer, *this);
        table.serialize(*serializer.newStructure("t"));
        rowNum.serialize(*serializer.newStructure("rn"));
        index.serialize(*serializer.newStructure("ix"));
    }

    /// Reconstitute constructor
    SparseTableFrozenColumn(StructuredReconstituter & reconstituter)
    {
        reconstituteMetadataT<SparseTableFrozenColumnMetadata>
            (reconstituter, *this);
        table.reconstitute(*reconstituter.getStructure("t"));
        rowNum.reconstitute(*reconstituter.getStructure("rn"));
        index.reconstitute(*reconstituter.getStructure("ix"));
    }
    
    /// Set of distinct values in the column chunk
    FrozenCellValueSet table;

    /// Row numbers (in increasing order) per non-null cell
    FrozenIntegerTable rowNum;

    /// Table index per non-null cell, corresponding to entries in
    /// rowNum
    FrozenIntegerTable index;

    uint32_t numEntries() const { return rowNum.size(); }
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
    
    virtual SparseTableFrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new SparseTableFrozenColumn(column, serializer);
    }

    virtual SparseTableFrozenColumn *
    reconstitute(StructuredReconstituter & reconstituter) const override
    {
        return new SparseTableFrozenColumn(reconstituter);
    }
};

RegisterFrozenColumnFormatT<SparseTableFrozenColumnFormat> regSparseTable;


/*****************************************************************************/
/* INTEGER FROZEN COLUMN                                                     */
/*****************************************************************************/

struct IntegerFrozenColumnMetadata {
    bool hasNulls = false;
    uint64_t firstEntry = 0;
    int64_t offset = 0;
    uint32_t numNonNullRows = 0;
    ColumnTypes columnTypes;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(IntegerFrozenColumnMetadata)
{
    setVersion(1);
    addField("hasNulls", &IntegerFrozenColumnMetadata::hasNulls, "");
    addField("firstEntry", &IntegerFrozenColumnMetadata::firstEntry, "");
    addField("offset", &IntegerFrozenColumnMetadata::offset, "");
    addField("nonNumNullRows",
             &IntegerFrozenColumnMetadata::numNonNullRows, "");
    addField("columnTypes", &IntegerFrozenColumnMetadata::columnTypes, "");
}

/// Frozen column that stores each value as a signed 64 bit integer
struct IntegerFrozenColumn
    : public FrozenColumn,
      public IntegerFrozenColumnMetadata {

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

            table.reserve(numEntries);
            
            uint64_t doneRows = 0;
            for (auto & v: column.sparseIndexes) {
                uint32_t rowNumber = v.first;
                const CellValue & val = column.indexedVals[v.second];
                uint64_t intVal = 0;
                if (!val.empty()) {
                    // Need to do arithmetic in unsigned mode as otherwise wrapping
                    // is undefined behavior
                    intVal = (uint64_t)val.toInt() - (uint64_t)offset + hasNulls;
                    ++numNonNullRows;
                }
                while (doneRows < rowNumber) {
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
    {
        this->columnTypes = std::move(column.columnTypes);
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
            : CellValue(int64_t(val + (uint64_t)offset - (uint64_t)!!hasNulls));
            
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
    
    virtual std::string format() const override
    {
        return "I";
    }

    virtual bool forEach(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const override
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= table.size())
            return result;
        return decode(table.get(rowIndex));
    }

    virtual size_t size() const override
    {
        return table.size();
    }

    virtual size_t memusage() const override
    {
        return sizeof(*this) + table.memusage();
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const override
    {
        auto onVal = [&] (uint64_t val) -> bool
            {
                return fn(decode(val));
            };

        return table.forEachDistinctValue(onVal);
    }

    virtual bool
    forEachDistinctValueWithRowCount(std::function<bool (const CellValue &, size_t)> fn) const override
    {
        auto onVal = [&] (uint64_t val, size_t count) -> bool
            {
                return fn(decode(val), count);
            };

        return table.forEachDistinctValueWithRowCount(onVal);
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullRows;
    }

    FrozenIntegerTable table;

    virtual ColumnTypes getColumnTypes() const override
    {
        return columnTypes;
    }

    virtual void serialize(StructuredSerializer & serializer) const override
    {
        serializeMetadataT<IntegerFrozenColumnMetadata>(serializer, *this);
        table.serialize(*serializer.newStructure("t"));
    }

    /// Reconstitute constructor
    IntegerFrozenColumn(StructuredReconstituter & reconstituter)
    {
        reconstituteMetadataT<IntegerFrozenColumnMetadata>
            (reconstituter, *this);
        table.reconstitute(*reconstituter.getStructure("t"));
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
    
    virtual IntegerFrozenColumn *
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

    virtual IntegerFrozenColumn *
    reconstitute(StructuredReconstituter & reconstituter) const override
    {
        return new IntegerFrozenColumn(reconstituter);
    }
};

RegisterFrozenColumnFormatT<IntegerFrozenColumnFormat> regInteger;


/*****************************************************************************/
/* DOUBLE FROZEN COLUMN                                                     */
/*****************************************************************************/

struct DoubleFrozenColumnMetadata {
    uint32_t numEntries = 0;
    uint64_t firstEntry = 0;
    uint32_t numNonNullRows = 0;
    ColumnTypes columnTypes;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(DoubleFrozenColumnMetadata)
{
    setVersion(1);
    addField("numEntries", &DoubleFrozenColumnMetadata::numEntries, "");
    addField("firstEntry", &DoubleFrozenColumnMetadata::firstEntry, "");
    addField("numNonNullRows",
             &DoubleFrozenColumnMetadata::numNonNullRows, "");
    addField("columnTypes", &DoubleFrozenColumnMetadata::columnTypes, "");
}

/// Frozen column that stores each value as a signed 64 bit double
struct DoubleFrozenColumn
    : public FrozenColumn,
      public DoubleFrozenColumnMetadata {

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

    typedef FrozenDoubleTable::Entry Entry;
    
    DoubleFrozenColumn(TabularDatasetColumn & column,
                       MappedSerializer & serializer)
    {
        this->columnTypes = column.columnTypes;
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

    virtual bool forEach(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, false /* keep nulls */);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const override
    {
        return forEachImpl(onRow, true /* keep nulls */);
    }

    virtual CellValue get(uint32_t rowIndex) const override
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;
        if (rowIndex >= numEntries)
            return result;
        return storage[rowIndex];
    }

    virtual size_t size() const override
    {
        return numEntries;
    }

    virtual size_t memusage() const override
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
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const override
    {
        bool hasNulls = false;

        PossiblyDynamicBuffer<double> allVals(numEntries);
        size_t n = 0;

        auto eq = [] (double d1, double d2)
        {
            return memcmp(&d1, &d2, sizeof(double)) == 0;
        };

        auto addEntry = [&] (double val)
        {
            if (n == 0 || !eq(val, allVals[n-1])) {
                allVals[n++] = val;
            }
        };

        for (size_t i = 0;  i < numEntries;  ++i) {
            const Entry & entry = storage[i];
            if (entry.isNull())
                hasNulls = true;
            else
                addEntry(entry.value());
        }

        // Handle nulls first so we don't have to do them later
        if (hasNulls && !fn(CellValue()))
            return false;

        /** Like std::less<Float>, but has a well defined order for nan
            values, which allows us to sort ranges that might contain
            nan values without crashing.
        */
        std::sort(allVals.data(), allVals.data() + n, safe_less<double>());
        
        for (auto it = allVals.data(), endIt = allVals.data() + n;  it != endIt;  /* no inc */) {
            auto it2 = it;
            ++it2;
            while (it2 != endIt && eq(*it, *it2)) {
                ++it2;
            }
            if (!fn(*it))
                return false;
            it = it2;
        }

        return true;
    }

    virtual bool
    forEachDistinctValueWithRowCount(std::function<bool (const CellValue &, size_t)> fn) const override
    {
        PossiblyDynamicBuffer<double> allVals(numEntries);
        size_t n = 0;
        size_t numNulls = 0;

        auto eq = [] (double d1, double d2)
        {
            return memcmp(&d1, &d2, sizeof(double)) == 0;
        };

        auto addEntry = [&] (double val)
        {
            allVals[n++] = val;
        };

        for (size_t i = 0;  i < numEntries;  ++i) {
            const Entry & entry = storage[i];
            if (entry.isNull())
                ++numNulls;
            else
                addEntry(entry.value());
        }

        if (numNulls > 0 && !fn(CellValue(), numNulls))
            return false;

        /** Like std::less<Float>, but has a well defined order for nan
            values, which allows us to sort ranges that might contain
            nan values without crashing.
        */
        std::sort(allVals.data(), allVals.data() + n, safe_less<double>());
        
        for (auto it = allVals.data(), endIt = allVals.data() + n;  it != endIt;  /* no inc */) {
            auto it2 = it;
            ++it2;
            while (it2 != endIt && eq(*it, *it2)) {
                ++it2;
            }
            if (!fn(*it, it2 - it))
                return false;
            it = it2;
        }
        
        return true;
    }

    virtual size_t nonNullRowCount() const override
    {
        return numNonNullRows;
    }

    FrozenMemoryRegionT<Entry> storage;

    virtual ColumnTypes getColumnTypes() const override
    {
        return columnTypes;
    }

    static ssize_t bytesRequired(const TabularDatasetColumn & column)
    {
        return SizingInfo(column);
    }

    virtual std::string format() const override
    {
        return "D";
    }

    virtual void serialize(StructuredSerializer & serializer) const override
    {
        serializeMetadataT<DoubleFrozenColumnMetadata>(serializer, *this);
        serializer.addRegion(storage, "d");
    }

    /// Reconstitute constructor
    DoubleFrozenColumn(StructuredReconstituter & reconstituter)
    {
        reconstituteMetadataT<DoubleFrozenColumnMetadata>(reconstituter, *this);
        storage = reconstituter.getRegion("d");
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
    
    virtual DoubleFrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new DoubleFrozenColumn(column, serializer);
    }

    virtual DoubleFrozenColumn *
    reconstitute(StructuredReconstituter & reconstituter) const override
    {
        return new DoubleFrozenColumn(reconstituter);
    }
};

RegisterFrozenColumnFormatT<DoubleFrozenColumnFormat> regDouble;


/*****************************************************************************/
/* TIMESTAMP FROZEN COLUMN                                                   */
/*****************************************************************************/

struct TimestampFrozenColumnMetadata {
    ColumnTypes columnTypes;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(TimestampFrozenColumnMetadata)
{
    setVersion(1);
    addField("columnTypes", &TimestampFrozenColumnMetadata::columnTypes, "");
}

/// Frozen column that stores each value as a timestamp
struct TimestampFrozenColumn
    : public FrozenColumn,
      public TimestampFrozenColumnMetadata {

    // This stores the underlying doubles or CellValues 
    std::shared_ptr<const FrozenColumn> unwrapped;

    TimestampFrozenColumn(TabularDatasetColumn & column,
                          MappedSerializer & serializer,
                          const ColumnFreezeParameters & params)
    {
        this->columnTypes = column.columnTypes;
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

    virtual bool forEach(const ForEachRowFn & onRow) const override
    {
        auto onRow2 = [&] (size_t rowNum, const CellValue & val)
            {
                return onRow(rowNum, wrap(val));
            };

        return unwrapped->forEach(onRow2);
    }

    virtual bool forEachDense(const ForEachRowFn & onRow) const override
    {
        auto onRow2 = [&] (size_t rowNum, const CellValue & val)
            {
                return onRow(rowNum, wrap(val));
            };

        return unwrapped->forEachDense(onRow2);
    }

    virtual CellValue get(uint32_t rowIndex) const override
    {
        return wrap(unwrapped->get(rowIndex));
    }

    virtual size_t size() const override
    {
        return unwrapped->size();
    }

    virtual size_t memusage() const override
    {
        return sizeof(*this)
            + unwrapped->memusage();
    }

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn) const override
    {
        auto fn2 = [&] (const CellValue & v)
            {
                return fn(wrap(v));
            };

        return unwrapped->forEachDistinctValue(fn2);
    }

    virtual bool
    forEachDistinctValueWithRowCount(std::function<bool (const CellValue &, size_t)> fn) const override
    {
        auto fn2 = [&] (const CellValue & v, size_t n)
            {
                return fn(wrap(v), n);
            };

        return unwrapped->forEachDistinctValueWithRowCount(fn2);
    }

    virtual size_t nonNullRowCount() const override
    {
        return unwrapped->nonNullRowCount();
    }

    virtual ColumnTypes getColumnTypes() const override
    {
        return columnTypes;
    }

    virtual std::string format() const override
    {
        return "Timestamp";
    }

    virtual void serialize(StructuredSerializer & serializer) const override
    {
        serializeMetadataT<TimestampFrozenColumnMetadata>(serializer, *this);
        unwrapped->serialize(*serializer.newStructure("ul"));
    }

    /// Reconstitute constructor
    TimestampFrozenColumn(StructuredReconstituter & reconstituter)
    {
        reconstituteMetadataT<TimestampFrozenColumnMetadata>
            (reconstituter, *this);
        unwrapped.reset(FrozenColumnFormat::thaw
                        (*reconstituter.getStructure("ul")));
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
    
    virtual TimestampFrozenColumn *
    freeze(TabularDatasetColumn & column,
           MappedSerializer & serializer,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const override
    {
        return new TimestampFrozenColumn(column, serializer, params);
    }

    virtual TimestampFrozenColumn *
    reconstitute(StructuredReconstituter & reconstituter) const override
    {
        return new TimestampFrozenColumn(reconstituter);
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
            throw AnnotatedException
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

std::shared_ptr<const FrozenColumnFormat>
FrozenColumnFormat::
getFormat(const std::string & formatName)
{
    auto formats = getFormats().load();

    auto it = formats->find(formatName);
    if (it == formats->end()) {
        throw AnnotatedException
            (400, "Frozen column format " + formatName + " not found");
    }

    return it->second;
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
#if 0
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
        throw AnnotatedException(500, "No column format found for column");
    }
    return res.second(column, serializer);
}

void
FrozenColumn::
serializeMetadata(StructuredSerializer & serializer,
                  const void * md,
                  const ValueDescription * desc) const
{
    Utf8String printed;
    {
        Utf8StringJsonPrintingContext context(printed);
        context.startObject();
        context.startMember("fmt");
        context.writeString(format());
        if (md) {
            context.startMember("type");
            context.writeString(desc->typeName);
            context.startMember("ver");
            context.writeInt(desc->getVersion());
            context.startMember("data");
            ExcAssert(desc);
            desc->printJson(md, context);
        }
        context.endObject();
    }

    //cerr << "got metadata " << printed << endl;

    auto entry = serializer.newEntry("md");
    auto serializeTo = entry->allocateWritable(printed.rawLength(),
                                               1 /* alignment */);
    
    std::memcpy(serializeTo.data(), printed.rawData(), printed.rawLength());
    serializeTo.freeze();
}

FrozenColumn *
FrozenColumnFormat::
thaw(StructuredReconstituter & reconstituter)
{
    // TODO: this is user generated data; we can't trust it.  We need to
    // ensure that things align properly between the types

    // 1.  Get the metadata
    FrozenMemoryRegion md = reconstituter.getRegion("md");

    //cerr << "md = " << std::string(md.data(), md.data() + md.length())
    //     << endl;
    
    Utf8StringJsonParsingContext context
        (md.data(), md.length(),
         (reconstituter.getContext() + "/md").rawString());

    std::string fmt;

    auto onMember = [&] ()
        {
            if (context.inField("fmt")) {
                fmt = context.expectStringAscii();
            }
            else {
                context.skip();
            }
        };

    context.forEachMember(onMember);

    std::shared_ptr<const FrozenColumnFormat> format
        = FrozenColumnFormat::getFormat(fmt);

    return format->reconstitute(reconstituter);

#if 0
    std::shared_ptr<const ValueDescription> desc;
    int version = 0;
    std::shared_ptr<void> mdObject;
    
    auto onMember = [&] ()
        {
            if (context.fieldNameView() == "fmt") {
                fmt = context.expectStringAscii();
            }
            else if (context.fieldNameView() == "type") {
                std::string structType = context.expectStringAscii();
                desc = ValueDescription::getType(structType);
                // TODO: get the correct version too...
            }
            else if (context.fieldNameView() == "ver") {
                version = context.expectInt();
            }
            else if (context.fieldNameView() == "data") {
                ExcAssert(desc);
                mdObject.reset(desc->constructDefault(),
                               [=] (void * obj) { desc->destroy(obj); });
                desc->parseJson(mdObject.get(), context);
            }
        };
    
    context.forEachMember(onMember);
#endif
    
    
}


} // namespace MLDB


