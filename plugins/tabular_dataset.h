/* tabular_dataset.h                                               -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Tabular dataset: one timestamp per row, dense values, known columns.

   An example is a CSV file or a relational database.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include <memory>
#include "mldb/arch/bit_range_ops.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/jml/utils/compact_vector.h"
#include <mutex>

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* TABULAR DATA STORE UTILITIES                                              */
/*****************************************************************************/

/// Tells us which types it could be
struct ColumnTypes {
    ColumnTypes()
        : numNulls(false), numIntegers(false),
          minNegativeInteger(0), maxPositiveInteger(0),
          numReals(false), numStrings(false), numBlobs(false),
          numOther(false)
    {
    }

    void update(const CellValue & val)
    {
        // Record the type
        switch (val.cellType()) {
        case CellValue::EMPTY:
            numNulls += 1;  break;
        case CellValue::FLOAT:
            numReals += 1;  break;

        case CellValue::INTEGER:
            numIntegers += 1;
            if (val.isUInt64()) {
                maxPositiveInteger = std::max(maxPositiveInteger, val.toUInt());
            }
            else {
                minNegativeInteger = std::min(minNegativeInteger, val.toInt());
            }
            break;
        case CellValue::ASCII_STRING:
        case CellValue::UTF8_STRING:
            numStrings += 1;  break;
        case CellValue::BLOB:
            numBlobs += 1;  break;
        default:
            numOther += 1;  break;
        }
    }

    void update(const ColumnTypes & other)
    {
        numNulls = numNulls + other.numNulls;
        numIntegers = numIntegers + other.numIntegers;
        minNegativeInteger
            = std::min(minNegativeInteger, other.minNegativeInteger);
        maxPositiveInteger
            = std::max(maxPositiveInteger, other.maxPositiveInteger);
        numReals = numReals + other.numReals;
        numStrings = numStrings + other.numStrings;
        numBlobs = numBlobs + other.numBlobs;
        numOther = numOther + other.numOther;
    }

    std::shared_ptr<ExpressionValueInfo>
    getExpressionValueInfo() const
    {
        if (!numNulls && !numReals && !numStrings && !numOther) {
            // Integers only
            if (minNegativeInteger == 0) {
                // All positive
                return std::make_shared<Uint64ValueInfo>();
            }
            else if (maxPositiveInteger <= (1ULL << 63)) {
                // Fits in a 64 bit integer
                return std::make_shared<IntegerValueInfo>();
            }
            else {
                // Out of range of either positive or negative integers
                // only.  We say it's an atom.
                return std::make_shared<AtomValueInfo>();
            }
        }
        else if (!numNulls && !numStrings && !numOther) {
            // Reals and integers.  If all integers are representable as
            // doubles, in other words a maximum of 53 bits, then we're all
            // doubles.
            if (maxPositiveInteger < (1ULL << 53)
                && minNegativeInteger > -(1LL << 53)) {
                return std::make_shared<Float64ValueInfo>();
            }
            // Doubles would lose precision.  It's an atom.
            return std::make_shared<AtomValueInfo>();
        }
        else if (!numNulls && !numIntegers && !numReals && !numOther) {
            return std::make_shared<Utf8StringValueInfo>();
        }
        else {
            return std::make_shared<AtomValueInfo>();
        }
    }
    uint64_t numNulls;

    uint64_t numIntegers;
    int64_t minNegativeInteger;
    uint64_t maxPositiveInteger;

    uint64_t numReals;
    uint64_t numStrings;
    uint64_t numBlobs;
    uint64_t numOther;  // timestamps, intervals
};

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
        
        cerr << columns.size() << " columns took " << result - before << endl;
        before = result;
        
        for (auto & c: sparseColumns)
            result += c.first.memusage() + c.second->memusage();

        cerr << sparseColumns.size() << " sparse columns took "
             << result - before << endl;
        before = result;

        for (auto & r: rowNames)
            result += r.memusage();

        cerr << rowNames.size() << " row names took "
             << result - before << endl;
        before = result;

        result += timestamps->memusage();

        cerr << "timestamps took "
             << result - before << endl;

        cerr << "total memory is " << result << endl;
        return result;
    }

    std::vector<std::shared_ptr<FrozenColumn> > columns;
    std::unordered_map<ColumnName, std::shared_ptr<FrozenColumn>, CoordNewHasher> sparseColumns;
    std::vector<RowName> rowNames;
    std::shared_ptr<FrozenColumn> timestamps;

    /// Add the given column to the column with the given index
    void addToColumn(int columnIndex,
                     std::vector<std::tuple<RowName, CellValue, Date> > & rows) const
    {
        for (unsigned i = 0;  i < rowNames.size();  ++i) {
            rows.emplace_back(rowNames[i],
                              columns[columnIndex]->get(i),
                              timestamps->get(i).toTimestamp());
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
    int add(RowName & rowName, Date ts, CellValue * vals,
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

        rowNames.emplace_back(std::move(rowName));
        timestamps.add(ts);

        for (unsigned i = 0;  i < columns.size();  ++i) {
            columns[i].add(std::move(vals[i]));
        }

        for (auto & e: extra) {
            auto it = sparseColumns.emplace(std::move(e.first), TabularDatasetColumn()).first;
            it->second.addSparse(numRows, std::move(e.second));
        }

        return ADD_SUCCEEDED;
    }
};

/*****************************************************************************/
/* TABULAR DATASET                                                           */
/*****************************************************************************/

enum UnknownColumnAction {
    UC_IGNORE,   ///< Ignore unknown columns
    UC_ERROR,    ///< Unknown columns are an error
    UC_ADD       ///< Add unknown columns as a new column
};

DECLARE_ENUM_DESCRIPTION(UnknownColumnAction);

struct TabularDatasetConfig {
    TabularDatasetConfig();

    UnknownColumnAction unknownColumns;
};

DECLARE_STRUCTURE_DESCRIPTION(TabularDatasetConfig);

struct TabularDataset : public Dataset {

    TabularDataset(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);

    virtual ~TabularDataset();
    
    virtual Any getStatus() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual std::shared_ptr<RowStream> getRowStream() const;
    
    virtual std::pair<Date, Date> getTimestampRange() const;

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const Utf8String& alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const;

    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const;

    /** Commit changes to the database. */
    virtual void commit();

    virtual MultiChunkRecorder getChunkRecorder();

    void recordRowItl(const RowName & rowName, const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals);

    void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows);

protected:
    // To initialize from a subclass
    TabularDataset(MldbServer * owner);

    struct TabularDataStore;
    std::shared_ptr<TabularDataStore> itl;
};



} // namespace MLDB
} // namespace Datacratic
