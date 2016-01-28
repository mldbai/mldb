/* tabular_dataset.h                                               -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Tabular dataset: one timestamp per row, dense values, known columns.

   An example is a CSV file or a relational database.
*/

#pragma once

#include <memory>
#include "mldb/arch/bit_range_ops.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/sql/cell_value_impl.h"

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* TABULAR DATA STORE UTILITIES                                              */
/*****************************************************************************/

/// Tells us which types it could be
struct ColumnTypes {
    ColumnTypes()
        : hasNulls(false), hasIntegers(false),
          minNegativeInteger(0), maxPositiveInteger(0),
          hasReals(false), hasStrings(false), hasOther(false)
    {
    }

    void update(const CellValue & val)
    {
        // Record the type
        switch (val.cellType()) {
        case CellValue::EMPTY:
            hasNulls = true;  break;
        case CellValue::FLOAT:
            hasReals = true;  break;

        case CellValue::INTEGER:
            hasIntegers = true;
            if (val.isUInt64()) {
                maxPositiveInteger = std::max(maxPositiveInteger, val.toUInt());
            }
            else {
                minNegativeInteger = std::min(minNegativeInteger, val.toInt());
            }
            break;
        case CellValue::ASCII_STRING:
        case CellValue::UTF8_STRING:
            hasStrings = true;  break;
        default:
            hasOther = true;  break;
        }
    }

    void update(const ColumnTypes & other)
    {
        hasNulls = hasNulls || other.hasNulls;
        hasIntegers = hasIntegers || other.hasIntegers;
        minNegativeInteger
            = std::min(minNegativeInteger, other.minNegativeInteger);
        maxPositiveInteger
            = std::max(maxPositiveInteger, other.maxPositiveInteger);
        hasReals = hasReals || other.hasReals;
        hasStrings = hasStrings || other.hasStrings;
        hasOther = hasOther || other.hasOther;
    }

    std::shared_ptr<ExpressionValueInfo>
    getExpressionValueInfo() const
    {
        if (!hasNulls && !hasReals && !hasStrings && !hasOther) {
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
        else if (!hasNulls && !hasStrings && !hasOther) {
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
        else if (!hasNulls && !hasIntegers && !hasReals && !hasOther) {
            return std::make_shared<Utf8StringValueInfo>();
        }
        else {
            return std::make_shared<AtomValueInfo>();
        }
    }
    bool hasNulls;

    bool hasIntegers;
    int64_t minNegativeInteger;
    uint64_t maxPositiveInteger;

    bool hasReals;
    bool hasStrings;
    bool hasOther;  // timestamps, intervals, blobs, etc
};

/// Base class for a frozen column
struct FrozenColumn {
    virtual ~FrozenColumn()
    {
    }

    virtual CellValue get(uint32_t rowIndex) const = 0;

    virtual size_t size() const = 0;

    virtual size_t getIndexBits() const = 0;

    virtual size_t memusage() const = 0;

    virtual bool forEachDistinctValue(std::function<bool (const CellValue &, size_t)> fn) const = 0;
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

    virtual size_t getIndexBits() const
    {
        return 32;
    }

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
};

/// Frozen column that finds each value in a lookup table
struct TableFrozenColumn: public FrozenColumn {
    TableFrozenColumn(const std::vector<int> & indexes,
                      std::vector<CellValue> table_)
        : table(std::move(table_))
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

    virtual size_t getIndexBits() const
    {
        return indexBits;
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
        return sizeof(*this)
            + (indexBits * numEntries + 31) / 8
            + table.capacity() * sizeof(CellValue);  // todo: usage of each
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
};
    

struct TabularDatasetColumn {
    //std::vector<CellValue> vals;

    void add(CellValue val)
    {
        // Optimization: if we're recording the same value as
        // the last column, then we don't need to do anything
        if (!indexes.empty() && val == lastValue) {
            // same as last one
            indexes.emplace_back(indexes.back());
            return;
        }

        // Optimization: if there are only a few values, do a
        // linear search and don't bother with the hashing
        if (indexedVals.size() < 8) {
            for (unsigned i = 0;  i < indexedVals.size();  ++i) {
                if (val == indexedVals[i]) {
                    indexes.emplace_back(i);
                    lastValue = std::move(val);
                    return;
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

        indexes.emplace_back(index);
    }

    void reserve(size_t sz)
    {
        indexes.reserve(sz);
    }

    std::vector<int> indexes;
    std::vector<CellValue> indexedVals;
    ML::Lightweight_Hash<uint64_t, int> valueIndex;
    CellValue lastValue;
    ColumnTypes columnTypes;
    std::shared_ptr<FrozenColumn> frozen;

    void freeze()
    {
        frozen.reset(new TableFrozenColumn(indexes, std::move(indexedVals)));
        indexes = std::vector<int>();
        indexedVals = std::vector<CellValue>();
        valueIndex = ML::Lightweight_Hash<uint64_t, int>();
        lastValue = CellValue();
    }

    size_t memusage() const
    {
        return sizeof(*this)
            + indexes.capacity() * sizeof(int)
            + indexedVals.capacity() * sizeof(CellValue)
            + valueIndex.capacity() * 16
            + (frozen ? frozen->memusage() : 0);
    }

    CellValue operator [] (size_t index) const
    {
        ExcAssert(frozen);
        return frozen->get(index);
    }

    template<typename Fn>
    bool forEach(Fn && fn) const
    {
        ExcAssert(frozen);
        size_t sz = frozen->size();
        for (size_t i = 0;  i < sz;  ++i) {
            if (!fn(i, std::move(frozen->get(i))))
                return false;
        } 
        return true;
    }

    template<typename Fn>
    bool forEachDistinctValue(Fn && fn) const
    {
        ExcAssert(frozen);
        return frozen->forEachDistinctValue(fn);
    }
};

struct TabularDatasetChunk {

    /// Not really required
    TabularDatasetChunk()
        : chunkNumber(-1), chunkLineNumber(-1), lineNumber(-1),
          numColumns(-1), numRows(0), numLines(0)
    {
        throw ML::Exception("Default constructor shouldn't be called");
    }

    TabularDatasetChunk(size_t numColumns, size_t reservedSize)
        : chunkNumber(-1), chunkLineNumber(-1), lineNumber(-1),
          numColumns(numColumns), numRows(0), numLines(0), columns(numColumns) 
    {
        rowNames.reserve(reservedSize);
        timestamps.reserve(reservedSize);
        for (unsigned i = 0;  i < numColumns;  ++i)
            columns[i].reserve(reservedSize);
    }

    TabularDatasetChunk(TabularDatasetChunk && other) noexcept
    : chunkNumber(-1), chunkLineNumber(-1), lineNumber(-1),
        numColumns(-1), numRows(0), numLines(0)
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
        std::swap(rowNames, other.rowNames);
        std::swap(timestamps, other.timestamps);
        std::swap(chunkNumber, other.chunkNumber);
        std::swap(chunkLineNumber, other.chunkLineNumber);
        std::swap(lineNumber, other.lineNumber);
        std::swap(numRows, other.numRows);
        std::swap(numLines, other.numLines);
        std::swap(numColumns, other.numColumns);
    }

    size_t rowCount() const
    {
        return numRows;
    }

    void freeze()
    {
        for (auto & c: columns)
            c.freeze();
        timestamps.freeze();
    }

    size_t memusage() const
    {
        size_t result = sizeof(*this);
        for (auto & c: columns)
            result += c.memusage();
        //result += rowNames.memusage();
        result += timestamps.memusage();
        return result;
    }

    /// Which chunk number is this associated with?
    int64_t chunkNumber;

    /// Which line number is this associated with?  Either in the 
    /// chunk or overall
    int64_t chunkLineNumber;

    /// Which absolute line number is this associated with?
    int64_t lineNumber;

    /// Number of columns in each line
    size_t numColumns;
            
    /// Number of rows we've added so far
    size_t numRows;

    /// Total number of lines that have been added
    size_t numLines;

    std::vector<TabularDatasetColumn> columns;
    std::vector<RowName> rowNames;
    TabularDatasetColumn timestamps;

    void add(int64_t lineNumber, RowName rowName, Date ts, CellValue * vals)
    {
        ++numRows;

        rowNames.emplace_back(std::move(rowName));
        timestamps.add(ts);

        for (unsigned i = 0;  i < numColumns;  ++i) {
            columns[i].add(std::move(vals[i]));
        }
    }

    /// Add the given column to the column with the given index
    void addToColumn(int columnIndex,
                     std::vector<std::tuple<RowName, CellValue, Date> > & rows) const
    {
        for (unsigned i = 0;  i < numRows;  ++i) {
            rows.emplace_back(rowNames[i],
                              columns[columnIndex][i],
                              timestamps[i].toTimestamp());
        }
    }
};


/*****************************************************************************/
/* TABULAR DATA STORE                                                        */
/*****************************************************************************/

/** Data store that can record tabular data and act as a matrix and
    column view to the underlying data.
*/

struct TabularDataStore: public ColumnIndex, public MatrixView {

    struct TabularDataStoreRowStream : public RowStream {

        TabularDataStoreRowStream(TabularDataStore * store) : store(store)
        {}

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<TabularDataStoreRowStream>(store);
            return ptr;
        }

        virtual void initAt(size_t start){
            size_t sum = 0;
            chunkiter = store->chunks.begin();
            while (chunkiter != store->chunks.end() && start > sum + chunkiter->rowNames.size())  {
                sum += chunkiter->rowNames.size();
                ++chunkiter;
            }

            if (chunkiter != store->chunks.end()) {
                rowiter = chunkiter->rowNames.begin() + (start - sum);
            }
        }

        virtual RowName next() {
            RowName row = *rowiter;
            rowiter++;
            if (rowiter == chunkiter->rowNames.end())
            {
                ++chunkiter;
                if (chunkiter != store->chunks.end())
                {
                    rowiter = chunkiter->rowNames.begin();
                    ExcAssert(rowiter != chunkiter->rowNames.end());
                }
            }
            return row;
        }

        TabularDataStore* store;
        std::vector<TabularDatasetChunk>::const_iterator chunkiter;
        std::vector<RowName>::const_iterator rowiter;

    };

    int64_t rowCount;

    std::vector<ColumnName> columnNames;
    std::vector<ColumnHash> columnHashes;
    ML::Lightweight_Hash<ColumnHash, int> columnIndex;
    
    std::vector<TabularDatasetChunk> chunks;

    /// Index from rowHash to (chunk, indexInChunk) when line number not used for rowName
    ML::Lightweight_Hash<RowHash, std::pair<int, int> > rowIndex;
    std::string filename;
    Date earliestTs, latestTs;

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        auto it = columnIndex.find(column);
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", columnNames);
        }

        MatrixColumn result;
        result.columnHash = result.columnName = column;

        for (unsigned i = 0;  i < chunks.size();  ++i) {
            chunks[i].addToColumn(it->second, result.rows);
        }
        
        return result;
    }

    virtual uint64_t getColumnRowCount(const ColumnName & column) const
    {
        return rowCount;
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        return columnIndex.count(column);
    }

    virtual std::vector<ColumnName> getColumnNames() const
    {
        return columnNames;
    }

    // TODO: we know more than this...
    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const
    {
        auto it = columnIndex.find(columnName);
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnName", columnName,
                                      "knownColumns", columnNames);
        }

        ColumnTypes types;
        for (auto & c: chunks) {
            types.update(c.columns[it->second].columnTypes);
        }

#if 0
        using namespace std;
        cerr << "knownColumnInfo for " << columnName << " is "
             << jsonEncodeStr(types.getExpressionValueInfo()) << endl;
        cerr << "hasNulls = " << types.hasNulls << endl;
        cerr << "hasIntegers = " << types.hasIntegers << endl;
        cerr << "minNegativeInteger = " << types.minNegativeInteger;
        cerr << "maxPositiveInteger = " << types.maxPositiveInteger;
        cerr << "hasReals = " << types.hasReals << endl;
        cerr << "hasStrings = " << types.hasStrings << endl;
        cerr << "hasOther = " << types.hasOther << endl;
#endif

        return KnownColumn(columnName, types.getExpressionValueInfo(),
                           COLUMN_IS_DENSE);
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        ExcAssertEqual(start, 0);
        ExcAssertEqual(limit, -1);

        std::vector<RowName> result;
        result.reserve(rowCount);

        for (auto & c: chunks) {
            result.insert(result.end(), c.rowNames.begin(), c.rowNames.end());
        }

        std::sort(result.begin(), result.end(),
                  [] (const RowName & n1,
                      const RowName & n2)
                  {
                      return n1.hash() < n2.hash();
                  });

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        ExcAssertEqual(start, 0);
        ExcAssertEqual(limit, -1);

        std::vector<RowHash> result;

        for (auto & i: rowIndex) {
            result.emplace_back(i.first);
        }

        return result;
    }

    std::pair<int, int> tryLookupRow(const RowName & rowName) const
    {
        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end())
            return { -1, -1 };
        return it->second;
    }
    
    std::pair<int, int> lookupRow(const RowName & rowName) const
    {
        auto result = tryLookupRow(rowName);
        if (result.first == -1)
            throw HttpReturnException(400, "Row not found in CSV dataset");
        return result;
    }

    virtual bool knownRow(const RowName & rowName) const
    {
        int chunkIndex;
        int rowIndex;

        std::tie(chunkIndex, rowIndex) = tryLookupRow(rowName);
        return chunkIndex > 0;
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        //cerr << "getting row " << rowName << endl;

        MatrixNamedRow result;
        result.rowHash = rowName;
        result.rowName = rowName;

        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in CSV dataset");
        }

        //cerr << "row is in chunk " << it->second.first << " offset "
        //     << it->second.second << endl;

        Date ts = chunks.at(it->second.first).timestamps[it->second.second].toTimestamp();

        for (unsigned i = 0;  i < columnNames.size();  ++i) {
            result.columns.emplace_back(columnNames[i], chunks.at(it->second.first).columns.at(i)[it->second.second], ts);
        }

        return result;
    }

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in CSV dataset");
        }

        return RowName(chunks.at(it->second.first).rowNames[it->second.second].toUtf8String());
    }

    virtual ColumnName getColumnName(ColumnHash column) const
    {
        auto it = columnIndex.find(column);
        if (it == columnIndex.end())
            throw HttpReturnException(400, "CSV dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", columnNames,
                                      "knownColumnHashes", columnHashes);
        return columnNames[it->second];
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnName & column, ColumnStats & stats) const
    {
        auto it = columnIndex.find(column);
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "CSV dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", columnNames,
                                      "knownColumnHashes", columnHashes);
        }

        stats = ColumnStats();

        bool isNumeric = true;

        for (auto & c: chunks) {

            auto onValue = [&] (const CellValue & value,
                                size_t rowCount)
                {
                    if (!value.isNumber())
                        isNumeric = false;
                    stats.values[value].rowCount_ += 1;
                    return true;
                };
                                
            c.columns[it->second].forEachDistinctValue(onValue);
        }

        stats.isNumeric_ = isNumeric && !chunks.empty();
        stats.rowCount_ = rowCount;
        return stats;
    }

    virtual size_t getRowCount() const
    {
        return rowCount;
    }

    virtual size_t getColumnCount() const
    {
        return columnNames.size();
    }

    virtual std::pair<Date, Date> getTimestampRange() const
    {
        return { earliestTs, latestTs };
    }

    GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const
    {
        GenerateRowsWhereFunction result;
        return result;
    }
};


} // namespace MLDB
} // namespace Datacratic
