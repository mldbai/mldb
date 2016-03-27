/** frozen_column.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Frozen (immutable), compressed column representations and methods
    to operate on them.
*/

#pragma once

#include "column_types.h"
#include "mldb/sql/cell_value.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/jml/utils/compact_vector.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/http/http_exception.h"

namespace Datacratic {
namespace MLDB {

/// Base class for a frozen column
struct FrozenColumn {
    virtual ~FrozenColumn()
    {
    }

    virtual CellValue get(uint32_t rowIndex) const = 0;

    virtual size_t size() const = 0;

    virtual size_t memusage() const = 0;

    virtual bool forEachDistinctValue(std::function<bool (const CellValue &, size_t)> fn) const = 0;

    CellValue operator [] (size_t index) const
    {
        return this->get(index);
    }

    template<typename Fn>
    bool forEach(Fn && fn) const
    {
        // TODO: sparse columns have nulls...
        size_t sz = this->size();
        for (size_t i = 0;  i < sz;  ++i) {
            if (!fn(i, std::move(this->get(i))))
                return false;
        } 
        return true;
    }

    virtual ColumnTypes getColumnTypes() const = 0;
};

/// Naive frozen column that gets its values from a pure
/// vector.
struct NaiveFrozenColumn: public FrozenColumn {
    NaiveFrozenColumn(std::vector<CellValue> vals)
        : vals(std::move(vals))
    {
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
        return vals.at(rowIndex);
    }
        
    std::vector<CellValue> vals;
    ColumnTypes columnTypes;

    virtual size_t size() const
    {
        return vals.capacity();
    }

    virtual size_t memusage() const
    {
        return sizeof(*this)
            + vals.capacity() * sizeof(CellValue);  // todo: usage of each
    }

    virtual bool forEachDistinctValue(std::function<bool (const CellValue &, size_t)> fn) const
    {
        for (auto & v: vals) {
            if (!fn(v, 1))
                return false;
        }

        return true;
    }

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }
};

/// Frozen column that finds each value in a lookup table
struct TableFrozenColumn: public FrozenColumn {
    TableFrozenColumn(const std::vector<int> & indexes,
                      std::vector<CellValue> table_,
                      const ColumnTypes & columnTypes)
        : table(std::move(table_)), columnTypes(columnTypes)
    {
        indexBits = ML::highest_bit(table.size()) + 1;
        numEntries = indexes.size();
        size_t numWords = (indexBits * indexes.size() + 31) / 32;
        uint32_t * data = new uint32_t[numWords];
        storage = std::shared_ptr<uint32_t>(data, [] (uint32_t * p) { delete[] p; });
            
        ML::Bit_Writer<uint32_t> writer(data);
        for (unsigned i = 0;  i < indexes.size();  ++i) {
            writer.write(indexes[i], indexBits);
        }
    }

    virtual CellValue get(uint32_t rowIndex) const
    {
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
            + (indexBits * numEntries + 31) / 8;

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

    std::shared_ptr<const uint32_t> storage;
    uint32_t indexBits;
    uint32_t numEntries;

    std::vector<CellValue> table;
    ColumnTypes columnTypes;

    virtual ColumnTypes getColumnTypes() const
    {
        return columnTypes;
    }
};

/// Sparse frozen column that finds each value in a lookup table
struct SparseTableFrozenColumn: public FrozenColumn {
    SparseTableFrozenColumn(uint64_t minRow,
                            uint64_t maxRow,
                            const ML::Lightweight_Hash<uint32_t, int> & indexes,
                            std::vector<CellValue> table_,
                            const ColumnTypes & columnTypes)
        : table(table_.size()), columnTypes(columnTypes)
    {
        std::move(std::make_move_iterator(table_.begin()),
                  std::make_move_iterator(table_.end()),
                  table.begin());
        indexBits = ML::highest_bit(table.size()) + 1;
        rowNumBits = ML::highest_bit(maxRow - minRow) + 1;
        numEntries = indexes.size();
        size_t numWords = ((indexBits + rowNumBits) * numEntries + 31) / 32;
        uint32_t * data = new uint32_t[numWords];
        storage = std::shared_ptr<uint32_t>(data, [] (uint32_t * p) { delete[] p; });
            
        ML::Bit_Writer<uint32_t> writer(data);
        for (auto & i: indexes) {
            writer.write(i.first, rowNumBits);
            writer.write(i.second, indexBits);
        }

        size_t mem = memusage();
        if (mem > 30000) {
            using namespace std;
            cerr << "table with " << indexes.size() << " entries from "
                 << minRow << " to " << maxRow << " and " << table.size()
                 << "uniques takes " << mem << " memory" << endl;
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

    std::shared_ptr<const uint32_t> storage;
    ML::compact_vector<CellValue, 0> table;
    uint8_t rowNumBits;
    uint8_t indexBits;
    uint32_t numEntries;
    ColumnTypes columnTypes;
};
    


} // namespace MLDB
} // namespace Datacratic
