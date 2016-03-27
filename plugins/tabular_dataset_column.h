/** tabular_dataset_column.h                                       -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Self-contained, recordable chunk of a tabular dataset.
    
*/

#pragma once

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* TABULAR DATASET COLUMN                                                    */
/*****************************************************************************/

struct TabularDatasetColumn {
    TabularDatasetColumn()
        : sparseRowOffset(0), maxRowNumber(0), isFrozen(false)
    {
    }

    /** Add a value for a dense column, ie one where we know we will call
        add() on every row, in order.
    */
    void add(CellValue val)
    {
        indexes.push_back(getIndex(val));
    }

    /** Add a value that doesn't occur on every row, for the given row number
        (which must be greater than the previous one) and the given cell
        value.

        This does NOT support multiple values of the same column per row.
    */
    void addSparse(size_t rowNumber, CellValue val)
    {
        if (val.empty())
            return;

        using namespace std;
        int index = getIndex(val);
        if (sparseIndexes.empty()) {
            sparseRowOffset = rowNumber;
            maxRowNumber = rowNumber;
        }
        else {
            ExcAssertGreaterEqual(rowNumber, sparseRowOffset);
            maxRowNumber = rowNumber;
            if (rowNumber == sparseRowOffset) {
                // We have two values for this column in this row.  If they're equal,
                // we're OK.  Otherwise we take the lowest value.
                if (index == sparseIndexes[rowNumber - sparseRowOffset]) ;
                else {
                    ExcAssert(false);
                }
            }
        }
        sparseIndexes[rowNumber - sparseRowOffset] = index;
    }

    /** Return the value index for this value.  This is the integer we store
        that indexes into the array of distinct values.

        This will destroy val, so don't re-use it afterwards (we don't
        take it as a by-value parameter to avoid having to call the move
        constructor, which is non-trivial).
    */
    int getIndex(CellValue & val)
    {
        ExcAssert(!isFrozen);
        // Optimization: if we're recording the same value as
        // the last column, then we don't need to do anything
        if (!indexes.empty() && val == lastValue) {
            return indexes.back();
        }

        // Optimization: if there are only a few values, do a
        // linear search and don't bother with the hashing

        if (indexedVals.size() < 8) {
            for (unsigned i = 0;  i < indexedVals.size();  ++i) {
                if (val == indexedVals[i]) {
                    lastValue = std::move(val);
                    return i;
                }
            }
        }

        // Look up the hash of the value we're looking for
        size_t hash = val.hash();
        auto it = valueIndex.find(hash);
        int index = -1;
        if (it == valueIndex.end()) {
            columnTypes.update(val);
            index = indexedVals.size();
            lastValue = val;
            valueIndex[hash] = index;
            indexedVals.emplace_back(std::move(val));
        }
        else {
            lastValue = std::move(val);
            index = it->second;
        }

        return index;
    }

    /** Reserve space for the given number of rows, for when we know that we
        will have a given number.  This saves on vector resizes during
        insertions.
    */
    void reserve(size_t sz)
    {
        indexes.reserve(sz);
    }

    std::vector<int> indexes;
    std::vector<CellValue> indexedVals;
    ML::Lightweight_Hash<uint64_t, int> valueIndex;
    CellValue lastValue;
    ML::Lightweight_Hash<uint32_t, int> sparseIndexes;
    size_t sparseRowOffset;
    size_t maxRowNumber;
    ColumnTypes columnTypes;
    bool isFrozen;

    std::shared_ptr<FrozenColumn> freeze()
    {
        ExcAssert(!isFrozen);
        isFrozen = true;
        if (!indexes.empty())
            return std::make_shared<TableFrozenColumn>(indexes, std::move(indexedVals), columnTypes);
        else return std::make_shared<SparseTableFrozenColumn>(sparseRowOffset, maxRowNumber, sparseIndexes, std::move(indexedVals), columnTypes);
    }

    size_t memusage() const
    {
        return sizeof(*this)
            + indexes.capacity() * sizeof(int)
            + indexedVals.capacity() * sizeof(CellValue)
            + valueIndex.capacity() * 16;
    }
};

} // namespace MLDB
} // namespace Datacratic
