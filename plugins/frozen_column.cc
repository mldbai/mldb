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


namespace Datacratic {
namespace MLDB {

/// Frozen column that finds each value in a lookup table
struct TableFrozenColumn: public FrozenColumn {
    TableFrozenColumn(TabularDatasetColumn & column)
        : table(std::move(column.indexedVals)),
          columnTypes(column.columnTypes)
    {
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
        //cerr << "getting " << rowIndex << " of " << numEntries << endl;
        ExcAssertLess(rowIndex, numEntries);
        ML::Bit_Extractor<uint32_t> bits(storage.get());
        bits.advance(rowIndex * indexBits);
        int index = bits.extract<uint32_t>(indexBits);
        if (hasNulls) {
            if (index == 0)
                return CellValue();
            else return table[index - 1];
        }
        else {
            return table[index];
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

    virtual bool forEachDistinctValue(std::function<bool (const CellValue &, size_t)> fn) const
    {
        if (hasNulls) {
            if (!fn(CellValue(), columnTypes.numNulls))
                return false;
        }
        for (auto & v: table) {
            if (!fn(v, 1 /* todo: real count */))
                return false;
        }

        return true;
    }

    std::shared_ptr<const uint32_t> storage;
    uint32_t indexBits;
    uint32_t numEntries;

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
            writer.write(i.second, indexBits);
        }

        size_t mem = memusage();
        if (mem > 30000) {
            using namespace std;
            cerr << "table with " << column.sparseIndexes.size()
                 << " entries from "
                 << column.minRowNumber << " to " << column.maxRowNumber
                 << " and " << table.size()
                 << " uniques takes " << mem << " memory" << endl;

            for (unsigned i = 0;  i < 5;  ++i) {
                cerr << "  " << table[i] << endl;
            }
        }
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        throw HttpReturnException(500, "TODO: SparseTableFrozenColumn get");
        //cerr << "getting " << rowIndex << " of " << numEntries << endl;
        ML::Bit_Extractor<uint32_t> bits(storage.get());
        bits.advance(rowIndex * indexBits);
        return table[bits.extract<uint32_t>(indexBits)];
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

    virtual bool forEachDistinctValue(std::function<bool (const CellValue &, size_t)> fn) const
    {
        for (auto & v: table) {
            if (!fn(v, 1 /* todo: real count */))
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
    ColumnTypes columnTypes;
};

std::shared_ptr<FrozenColumn>
FrozenColumn::
freeze(TabularDatasetColumn & column)
{
    return std::make_shared<TableFrozenColumn>(column);

    size_t required1 = TableFrozenColumn::bytesRequired(column);
    size_t required2 = SparseTableFrozenColumn::bytesRequired(column);

    if (required1 <= required2)
        return std::make_shared<TableFrozenColumn>(column);
    else return std::make_shared<SparseTableFrozenColumn>(column);
}

} // namespace MLDB
} // namespace Datacratic

