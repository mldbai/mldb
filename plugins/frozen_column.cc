/** frozen_column.cc                                               -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "frozen_column.h"
#include "tabular_dataset_column.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/jml/utils/compact_vector.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/http/http_exception.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

/// Frozen column that finds each value in a lookup table
struct TableFrozenColumn: public FrozenColumn {
    TableFrozenColumn(TabularDatasetColumn & column)
        : table(std::move(column.indexedVals)),
          columnTypes(column.columnTypes)
    {
        firstEntry = column.minRowNumber;
        numEntries = column.maxRowNumber - column.minRowNumber + 1;
        hasNulls = column.sparseIndexes.size() < numEntries;
        indexBits = ML::highest_bit(table.size() + hasNulls) + 1;
        size_t numWords = (indexBits * numEntries + 31) / 32;
        uint32_t * data = new uint32_t[numWords];
        storage = std::shared_ptr<uint32_t>(data, [] (uint32_t * p) { delete[] p; });

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
        ML::Bit_Extractor<uint32_t> bits(storage.get());
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

    std::shared_ptr<const uint32_t> storage;
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

    static size_t bytesRequired(const TabularDatasetColumn & column)
    {
        size_t numEntries = column.maxRowNumber - column.minRowNumber + 1;
        size_t hasNulls = column.sparseIndexes.size() < numEntries;
        int indexBits = ML::highest_bit(column.indexedVals.size() + hasNulls) + 1;
        size_t result
            = sizeof(TableFrozenColumn)
            + (indexBits * numEntries + 31) / 8;

        for (auto & v: column.indexedVals)
            result += v.memusage();

        return result;
    }
};

/// Sparse frozen column that finds each value in a lookup table
struct SparseTableFrozenColumn: public FrozenColumn {
    SparseTableFrozenColumn(TabularDatasetColumn & column)
        : table(column.indexedVals.size()), columnTypes(column.columnTypes)
    {
        firstEntry = column.minRowNumber;
        std::move(std::make_move_iterator(column.indexedVals.begin()),
                  std::make_move_iterator(column.indexedVals.end()),
                  table.begin());
        indexBits = ML::highest_bit(table.size()) + 1;
        rowNumBits = ML::highest_bit(column.maxRowNumber - column.minRowNumber) + 1;
        numEntries = column.sparseIndexes.size();
        size_t numWords = ((indexBits + rowNumBits) * numEntries + 31) / 32;
        uint32_t * data = new uint32_t[numWords];
        storage = std::shared_ptr<uint32_t>(data, [] (uint32_t * p) { delete[] p; });
            
        ML::Bit_Writer<uint32_t> writer(data);
        for (auto & i: column.sparseIndexes) {
            writer.write(i.first, rowNumBits);
            ExcAssertLess(i.second, table.size());
            writer.write(i.second, indexBits);
        }

#if 0
        size_t mem = memusage();
        if (mem > 30000) {
            using namespace std;
            cerr << "table with " << column.sparseIndexes.size()
                 << " entries from "
                 << column.minRowNumber << " to " << column.maxRowNumber
                 << " and " << table.size()
                 << " uniques takes " << mem << " memory" << endl;

            for (unsigned i = 0;  i < 5 && i < table.size();  ++i) {
                cerr << "  " << table[i] << endl;
            }
        }
#endif
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        CellValue result;
        if (rowIndex < firstEntry)
            return result;
        rowIndex -= firstEntry;

        auto getAtIndex = [&] (uint32_t n)
            {
                ML::Bit_Extractor<uint32_t> bits(storage.get());
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

            //cerr << "first = " << first << " middle = " << middle
            //     << " last = " << last << " rowNum = " << rowNum
            //     << " looking for " << rowIndex << endl;

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

    static size_t bytesRequired(const TabularDatasetColumn & column)
    {
        int indexBits = ML::highest_bit(column.indexedVals.size()) + 1;
        int rowNumBits = ML::highest_bit(column.maxRowNumber - column.minRowNumber) + 1;
        size_t numEntries = column.sparseIndexes.size();

        size_t result
            = sizeof(SparseTableFrozenColumn)
            + ((indexBits + rowNumBits) * numEntries + 31) / 8;

        for (auto & v: column.indexedVals)
            result += v.memusage();

        return result;
    }

    std::shared_ptr<const uint32_t> storage;
    ML::compact_vector<CellValue, 0> table;
    uint8_t rowNumBits;
    uint8_t indexBits;
    uint32_t numEntries;
    size_t firstEntry;
    ColumnTypes columnTypes;
};

std::shared_ptr<FrozenColumn>
FrozenColumn::
freeze(TabularDatasetColumn & column)
{
    size_t required1 = TableFrozenColumn::bytesRequired(column);
    size_t required2 = SparseTableFrozenColumn::bytesRequired(column);

    if (required1 <= required2)
        return std::make_shared<TableFrozenColumn>(column);
    else return std::make_shared<SparseTableFrozenColumn>(column);
}

} // namespace MLDB
} // namespace Datacratic

