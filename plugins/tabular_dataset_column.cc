/** tabular_dataset_column.cc                                      -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Implementation of code to freeze columns into a binary format.
*/

#include "tabular_dataset_column.h"



namespace MLDB {


/*****************************************************************************/
/* TABULAR DATASET COLUMN                                                    */
/*****************************************************************************/

TabularDatasetColumn::
TabularDatasetColumn()
    : minRowNumber(-1), maxRowNumber(-1), isFrozen(false)
{
}

void
TabularDatasetColumn::
add(size_t rowNumber, CellValue val)
{
    if (minRowNumber == -1) {
        minRowNumber = rowNumber;
    }
    else {
        ExcAssertGreaterEqual(rowNumber, maxRowNumber);
    }

    if (val.empty()) {
        maxRowNumber = rowNumber;
        return;
    }

    using namespace std;
    int index = getIndex(val);
    if (rowNumber == maxRowNumber) {
        // We have two values for this column in this row.  If they're
        // equal, we're OK.  Otherwise we take the lowest value.
        //ExcAssertEqual(index, sparseIndexes.back().first);
        return;  // ignore second value
    }
    maxRowNumber = rowNumber;

    sparseIndexes.emplace_back(rowNumber - minRowNumber, index);
}

int
TabularDatasetColumn::
getIndex(CellValue & val)
{
    ExcAssert(!isFrozen);
    // Optimization: if we're recording the same value as
    // the last column, then we don't need to do anything
    if (!sparseIndexes.empty() && val == lastValue) {
        return sparseIndexes.back().second;
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

void
TabularDatasetColumn::
reserve(size_t sz)
{
    sparseIndexes.reserve(sz);
}

std::shared_ptr<FrozenColumn>
TabularDatasetColumn::
freeze(const ColumnFreezeParameters & params)
{
    ExcAssert(!isFrozen);
    isFrozen = true;

    return FrozenColumn::freeze(*this, params);
}

#if 0
size_t
TabularDatasetColumn::
memusage() const
{
    return sizeof(*this)
        + sparseIndexes.capacity() * 8
        + indexedVals.capacity() * sizeof(CellValue)
        + valueIndex.capacity() * 16;
}
#endif

} // namespace MLDB

