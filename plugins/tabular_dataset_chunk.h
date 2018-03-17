/** tabular_dataset_chunk.h                                        -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Self-contained, recordable chunk of a tabular dataset.
*/

#pragma once

#include <unordered_map>
#include "frozen_column.h"
#include "mldb/types/path.h"
#include "mldb/types/date.h"
#include "tabular_dataset_column.h"
#include "tabular_dataset.h"
#include <mutex>


namespace MLDB {

struct PathElement;
struct Path;
struct ExpressionValue;


/*****************************************************************************/
/* TABULAR DATASET CHUNK                                                     */
/*****************************************************************************/

/** This represents a frozen chunk of a dataset: a fixed number of rows,
    each named, with a set of columns (either dense or sparse) attached.
*/

struct TabularDatasetChunk: public MappedObject {

    TabularDatasetChunk(size_t numColumns = 0)
        : columns(numColumns)
    {
    }

    TabularDatasetChunk(TabularDatasetChunk && other) noexcept
    {
        swap(other);
    }

    TabularDatasetChunk & operator = (TabularDatasetChunk && other) noexcept
    {
        swap(other);
        return *this;
    }

    void swap(TabularDatasetChunk & other) noexcept
    {
        columns.swap(other.columns);
        sparseColumns.swap(other.sparseColumns);
        rowNames.swap(other.rowNames);
        std::swap(timestamps, other.timestamps);
    }

    size_t rowCount() const
    {
        return rowNames->size();
    }

    size_t memusage() const;

    /// Return an owned version of the rowname
    Path getRowPath(size_t index) const;

    /// Return a reference to the rowName, stored in storage if it's a temp
    const Path & getRowPath(size_t index, RowPath & storage) const;

    const FrozenColumn *
    maybeGetColumn(size_t columnIndex, const PathElement & columnName) const;

    const FrozenColumn *
    maybeGetColumn(size_t columnIndex, const Path & columnName) const;

    const FrozenColumn & getColumnByIndex(size_t columnIndex) const
    {
        return *columns.at(columnIndex);
    }

    /// Get the row with the given index
    std::vector<std::tuple<ColumnPath, CellValue, Date> >
    getRow(size_t index, const std::vector<Path> & fixedColumnNames) const;

    /// Get the row with the given index
    ExpressionValue
    getRowExpr(size_t index, const std::vector<Path> & fixedColumnNames) const;

    /// Add the given column to the column with the given index
    void addToColumn(int columnIndex,
                     const Path & colName,
                     std::vector<std::tuple<Path, CellValue, Date> > & rows,
                     bool dense) const;

    size_t fixedColumnCount() const
    {
        return columns.size();
    }

    size_t sparseColumnCount() const
    {
        return sparseColumns.size();
    }

    size_t columnCount() const
    {
        return columns.size() + sparseColumns.size();
    }

    /** Serialize to the given serializer. */
    void serialize(StructuredSerializer & serializer) const;

private:
    std::vector<std::shared_ptr<FrozenColumn> > columns;
    std::unordered_map<Path, std::shared_ptr<FrozenColumn>, PathNewHasher> sparseColumns;
    std::shared_ptr<FrozenColumn> rowNames;
    std::shared_ptr<FrozenColumn> timestamps;

    friend class TabularDataset;
    friend class MutableTabularDatasetChunk;
};


/*****************************************************************************/
/* MUTABLE TABULAR DATASET CHUNK                                             */
/*****************************************************************************/

/** This is a mutable chunk of a tabular dataset.  It's not frozen, and so
    the storage is less efficient, but it can have rows added to it.
*/

struct MutableTabularDatasetChunk {

    MutableTabularDatasetChunk(size_t numColumns, size_t maxSize);

    MutableTabularDatasetChunk(MutableTabularDatasetChunk && other) noexcept = delete;
    MutableTabularDatasetChunk & operator = (MutableTabularDatasetChunk && other) noexcept = delete;

    TabularDatasetChunk freeze(MappedSerializer & serializer,
                               const ColumnFreezeParameters & params);

    /// Protect access in a multithreaded context
    mutable std::mutex mutex;

    /// Maximum size, in rows
    size_t maxSize;

    /// Number of rows added so far
    size_t rowCount_;

    size_t rowCount() const
    {
        return rowCount_;
    }

    /// Set of known, dense valued columns
    std::vector<TabularDatasetColumn> columns;

    bool isFrozen;

    /// Set of sparse columns
    std::unordered_map<Path, TabularDatasetColumn> sparseColumns;

    /// Ordered list of row names
    TabularDatasetColumn rowNames;

    /// One per row; however these are all date valued
    TabularDatasetColumn timestamps;

    /// Has add() failure been notified?  This selects whether we
    /// return 0 or -1 from add().
    bool addFailureNotified;

    static constexpr int ADD_SUCCEEDED = 1;
    static constexpr int ADD_PERFORM_ROTATION = 0;
    static constexpr int ADD_AWAIT_ROTATION = -1;

    /** Add the given values to this chunk.  Arguments are:
        - rowName: the name of the new row.  It must be unique (and this
          is not checked).
        - ts: the timestamp to be given to all values of this row
        - vals: the values of all cells at this row, for dense values
        - numVals: the number of dense values.  Used to verify that the
          right number were passed.
        - extra: extra columns and their values, for when we accept an open
          schema.  These will be stored less efficiently and will normally
          be sparse.  It takes a reference as the operation can fail and we
          may need to retry.  If it returns false, extra is untouched,
          otherwise it is destroyed.

        Returns:
          - ADD_SUCCEEDED         if it was added
          - ADD_PERFORM_ROTATION  if it wasn't added, and this is the first
                                  failed attempt (this thread should rotate)
          - ADD_AWAIT_ROTATION    if it wasn't added, and another thread has
                                  already received ADD_PERFORM_ROTATION
    */
    int add(Path & rowName,
            Date ts,
            CellValue * vals,
            size_t numVals,
            std::vector<std::pair<Path, CellValue> > & extra)
        __attribute__((warn_unused_result));
};

} // namespace MLDB

