/** tabular_dataset_column.h                                       -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Self-contained, recordable chunk of a tabular dataset.
    
*/

#pragma once

#include "frozen_column.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/sql/cell_value.h"


namespace MLDB {


/*****************************************************************************/
/* TABULAR DATASET COLUMN                                                    */
/*****************************************************************************/

/** Represents a column of a tabular dataset that's currently being
    constructed as the inversion of a row by row insert.
*/

struct TabularDatasetColumn {
    TabularDatasetColumn();

    /** Add a value for the given row number (which must be greater than
        the previous one) and the given cell value.

        This does NOT support multiple values of the same column per row.
    */
    void add(size_t rowNumber, CellValue val);

    /** Return the value index for this value.  This is the integer we store
        that indexes into the array of distinct values.

        This will destroy val, so don't re-use it afterwards (we don't
        take it as a by-value parameter to avoid having to call the move
        constructor, which is non-trivial).
    */
    int getIndex(CellValue & val);

    /** Reserve space for the given number of rows, for when we know that we
        will have a given number.  This saves on vector resizes during
        insertions.
    */
    void reserve(size_t sz);

    std::vector<CellValue> indexedVals;
    Lightweight_Hash<uint64_t, int> valueIndex;
    CellValue lastValue;
    std::vector<std::pair<uint32_t, int> > sparseIndexes;
    int64_t minRowNumber;  ///< Including null values not in sparseIndexes
    int64_t maxRowNumber;  ///< Including null values not in sparseIndexes
    ColumnTypes columnTypes;
    bool isFrozen;

    std::shared_ptr<FrozenColumn>
    freeze(const ColumnFreezeParameters & params);

    size_t memusage() const;
};

} // namespace MLDB

