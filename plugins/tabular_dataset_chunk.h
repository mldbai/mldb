/** tabular_dataset_chunk.h                                        -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Self-contained, recordable chunk of a tabular dataset.
*/

#pragma once

#include <unordered_map>
#include "frozen_column.h"
#include <mutex>

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* TABULAR DATASET CHUNK                                                     */
/*****************************************************************************/

/** This represents a frozen chunk of a dataset: a fixed number of rows,
    each named, with a set of columns (either dense or sparse) attached.
*/

struct TabularDatasetChunk {

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
        return rowNames.size();
    }

    size_t memusage() const
    {
        using namespace std;
        size_t result = sizeof(*this);
        size_t before = result;
        for (auto & c: columns)
            result += c->memusage();
        
        //cerr << columns.size() << " columns took " << result - before << endl;
        before = result;
        
        for (auto & c: sparseColumns)
            result += c.first.memusage() + c.second->memusage();

        //cerr << sparseColumns.size() << " sparse columns took "
        //     << result - before << endl;
        before = result;

        for (auto & r: rowNames)
            result += r.memusage();

        //cerr << rowNames.size() << " row names took "
        //     << result - before << endl;
        before = result;

        result += timestamps->memusage();

        //cerr << "timestamps took "
        //     << result - before << endl;

        //cerr << "total memory is " << result << endl;
        return result;
    }

    const FrozenColumn *
    maybeGetColumn(size_t columnIndex, const PathElement & columnName) const
    {
        if (columnIndex < columns.size()) {
            return columns[columnIndex].get();
        }
        else {
            auto it = sparseColumns.find(columnName);
            if (it == sparseColumns.end())
                return nullptr;
            return it->second.get();
        }
    }

    std::vector<std::shared_ptr<FrozenColumn> > columns;
    std::unordered_map<ColumnName, std::shared_ptr<FrozenColumn>, PathNewHasher> sparseColumns;
    std::vector<RowName> rowNames;
    std::shared_ptr<FrozenColumn> timestamps;

    /// Get the row with the given index
    std::vector<std::tuple<ColumnName, CellValue, Date> >
    getRow(size_t index, const std::vector<ColumnName> & fixedColumnNames) const
    {
        ExcAssertLess(index, rowNames.size());
        std::vector<std::tuple<ColumnName, CellValue, Date> > result;
        result.reserve(columns.size());
        Date ts = timestamps->get(index).toTimestamp();
        for (size_t i = 0;  i < columns.size();  ++i) {
            CellValue val = columns[i]->get(index);
            if (val.empty())
                continue;
            result.emplace_back(fixedColumnNames[i], std::move(val), ts);
        }

        for (auto & c: sparseColumns) {
            CellValue val = c.second->get(index);
            if (val.empty())
                continue;
            result.emplace_back(c.first, std::move(val), ts);

        }
        return result;
    }

    /// Add the given column to the column with the given index
    void addToColumn(int columnIndex,
                     const ColumnName & colName,
                     std::vector<std::tuple<RowName, CellValue, Date> > & rows,
                     bool dense) const
    {
        const FrozenColumn * col = nullptr;
        if (columnIndex < columns.size())
            col = columns[columnIndex].get();
        else {
            auto it = sparseColumns.find(colName);
            if (it == sparseColumns.end()) {
                if (dense) {
                    for (unsigned i = 0;  i < rowNames.size();  ++i) {
                        rows.emplace_back(rowNames[i],
                                          CellValue(),
                                          timestamps->get(i).toTimestamp());
                    }
                }
                return;
            }
            col = it->second.get();
        }

        for (unsigned i = 0;  i < rowNames.size();  ++i) {
            CellValue val = col->get(i);
            if (dense || !val.empty()) {
                rows.emplace_back(rowNames[i],
                                  std::move(val),
                                  timestamps->get(i).toTimestamp());
            }
        }
    }
};

struct MutableTabularDatasetChunk {

    MutableTabularDatasetChunk(size_t numColumns, size_t maxSize)
        : maxSize(maxSize), columns(numColumns), isFrozen(false),
          addFailureNotified(false)
    {
        rowNames.reserve(maxSize);
        timestamps.reserve(maxSize);
        for (unsigned i = 0;  i < numColumns;  ++i)
            columns[i].reserve(maxSize);
    }

    MutableTabularDatasetChunk(MutableTabularDatasetChunk && other) noexcept = delete;
    MutableTabularDatasetChunk & operator = (MutableTabularDatasetChunk && other) noexcept = delete;

    TabularDatasetChunk freeze()
    {
        std::unique_lock<std::mutex> guard(mutex);

        ExcAssert(!isFrozen);

        TabularDatasetChunk result;
        result.columns.resize(columns.size());
        result.sparseColumns.reserve(sparseColumns.size());

        for (unsigned i = 0;  i < columns.size();  ++i)
            result.columns[i] = columns[i].freeze();
        for (auto & c: sparseColumns)
            result.sparseColumns.emplace(c.first, c.second.freeze());

        result.timestamps = timestamps.freeze();
        result.rowNames = std::move(rowNames);

        isFrozen = true;

        return result;
    }

    /// Protect access in a multithreaded context
    mutable std::mutex mutex;

    /// Maximum size
    size_t maxSize;

    size_t rowCount() const
    {
        return rowNames.size();
    }

    /// Set of known, dense valued columns
    std::vector<TabularDatasetColumn> columns;

    bool isFrozen;

    /// Set of sparse columns
    std::unordered_map<ColumnName, TabularDatasetColumn> sparseColumns;

    /// One per row
    std::vector<RowName> rowNames;

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
    int add(RowName & rowName,
            Date ts,
            CellValue * vals,
            size_t numVals,
            std::vector<std::pair<ColumnName, CellValue> > & extra)
        __attribute__((warn_unused_result))
    {
        std::unique_lock<std::mutex> guard(mutex);
        if (isFrozen)
            return ADD_AWAIT_ROTATION;
        size_t numRows = rowNames.size();

        if (numRows == maxSize) {
            if (addFailureNotified)
                return ADD_AWAIT_ROTATION;
            else {
                addFailureNotified = true;
                return ADD_PERFORM_ROTATION;
            }
        }

        ExcAssertEqual(columns.size(), numVals);

        rowNames.emplace_back(std::move(rowName));
        timestamps.add(numRows, ts);

        for (unsigned i = 0;  i < columns.size();  ++i) {
            columns[i].add(numRows, std::move(vals[i]));
        }

        for (auto & e: extra) {
            auto it = sparseColumns.emplace(std::move(e.first), TabularDatasetColumn()).first;
            it->second.add(numRows, std::move(e.second));
        }

        return ADD_SUCCEEDED;
    }
};

} // namespace MLDB
} // namespace Datacratic
