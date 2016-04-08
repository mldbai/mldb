/** tabular_dataset.cc                                             -*- C++ -*-
    Jeremy Barnes, 26 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "tabular_dataset.h"
#include "column_types.h"
#include "frozen_column.h"
#include "tabular_dataset_column.h"
#include "tabular_dataset_chunk.h"
#include "mldb/arch/timers.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/jml/training_index_entry.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/scope.h"
#include "mldb/server/bucket.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/atomic_shared_ptr.h"
#include <mutex>

using namespace std;

namespace Datacratic {
namespace MLDB {

static constexpr size_t TABULAR_DATASET_DEFAULT_ROWS_PER_CHUNK=65536;
static constexpr size_t NUM_PARALLEL_CHUNKS=16;


/*****************************************************************************/
/* TABULAR DATA STORE                                                        */
/*****************************************************************************/

/** Data store that can record tabular data and act as a matrix and
    column view to the underlying data.
*/

struct TabularDataset::TabularDataStore: public ColumnIndex, public MatrixView {

    TabularDataStore(TabularDatasetConfig config)
        : rowCount(0), config(std::move(config)),
          backgroundJobsActive(0)
    {
    }

    /** A stream of row names used to incrementally query available rows
        without creating an entire list in memory.
    */
    struct TabularDataStoreRowStream : public RowStream {

        TabularDataStoreRowStream(TabularDataStore * store) : store(store)
        {
        }

        virtual std::shared_ptr<RowStream> clone() const
        {
            return std::make_shared<TabularDataStoreRowStream>(store);
        }

        virtual void initAt(size_t start)
        {
            size_t sum = 0;
            chunkiter = store->chunks.begin();
            while (chunkiter != store->chunks.end()
                   && start > sum + chunkiter->rowNames.size())  {
                sum += chunkiter->rowNames.size();
                ++chunkiter;
            }

            if (chunkiter != store->chunks.end()) {
                rowiter = chunkiter->rowNames.begin() + (start - sum);
            }
        }

        virtual RowName next()
        {
            ExcAssert(rowiter != chunkiter->rowNames.end());
            RowName row = *rowiter++;
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

    /// This indexes column names to their index, using new (fast) hash
    ML::Lightweight_Hash<uint64_t, int> columnIndex;

    /// Same index, but using the old (slow) hash.  Useful only for when
    /// we are forced to lookup on ColumnHash.
    ML::Lightweight_Hash<ColumnHash, int> columnHashIndex;

    struct ColumnEntry {
        ColumnEntry()
            : rowCount(0)
        {
        }

        ColumnName columnName;

        /// The number of non-null values of this row
        size_t rowCount;

        /// The set of chunks that contain the column.  This may not be all
        /// chunks for sparse columns.
        std::vector<std::pair<uint32_t, std::shared_ptr<const FrozenColumn> > > chunks;
    };

    /// List of all columns in the dataset
    std::vector<ColumnEntry> columns;
    
    /// List of the names of the fixed columns in the dataset
    std::vector<ColumnName> fixedColumns;

    /// Index of just the fixed columns
    ML::Lightweight_Hash<uint64_t, int> fixedColumnIndex;

    /// List of all chunks in the dataset
    std::vector<TabularDatasetChunk> chunks;

    /** This structure handles a list of chunks that allows for them to be
        recorded in parallel.  It's used for the old recordRow interface.
        Most datasets should instead use the chunk oriented interface, and
        at that point we can simplify the current code.

        It has atomic characteristics which allow for it to be used in a
        RCU-like situation without locking.
    */
    struct ChunkList {
        ChunkList(size_t n)
            : chunks(new atomic_shared_ptr<MutableTabularDatasetChunk>[n]),
              n(n)
        {
        }

        ~ChunkList()
        {
            delete[] chunks;
        }

        size_t size() const
        {
            return n;
        }

        std::shared_ptr<MutableTabularDatasetChunk>
        operator [] (size_t i) const
        {
            ExcAssertLess(i, n);
            return chunks[i].load();
        }

        const atomic_shared_ptr<MutableTabularDatasetChunk> * begin() const
        {
            return chunks;
        }

        const atomic_shared_ptr<MutableTabularDatasetChunk> * end() const
        {
            return chunks + n;
        }

        atomic_shared_ptr<MutableTabularDatasetChunk> * begin()
        {
            return chunks;
        }

        atomic_shared_ptr<MutableTabularDatasetChunk> * end()
        {
            return chunks + n;
        }
        
        atomic_shared_ptr<MutableTabularDatasetChunk> * chunks;
        size_t n;
    };

    // Reading of this data structure is not protected by any lock
    // Writing is protected by the dataset mutex
    atomic_shared_ptr<ChunkList> mutableChunks;

    // Everything below here is protected by the dataset lock
    std::vector<TabularDatasetChunk> frozenChunks;

    /// Index from rowHash to (chunk, indexInChunk) when line number not used for rowName
    ML::Lightweight_Hash<RowHash, std::pair<int, int> > rowIndex;
    std::string filename;
    Date earliestTs, latestTs;

    std::mutex datasetMutex;

    TabularDatasetConfig config;

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        auto it = columnIndex.find(column.newHash());
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", getColumnNames());
        }

        MatrixColumn result;
        result.columnHash = result.columnName = column;

        for (auto & c: columns[it->second].chunks) {
            chunks.at(c.first).addToColumn(it->second, column, result.rows,
                                           false /* dense */);
        }
        
        return result;
    }

    virtual std::vector<CellValue>
    getColumnDense(const ColumnName & column) const
    {
        auto it = columnIndex.find(column.newHash());
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given name",
                                      "columnName", column,
                                      "knownColumns", getColumnNames());
        }

        const ColumnEntry & entry = columns[it->second];

        std::vector<CellValue> result;
        result.reserve(entry.rowCount);

        // Go through each chunk with a non-null value
        for (auto & c: entry.chunks) {
            auto onValue = [&] (size_t n, CellValue val)
                {
                    if (!val.empty())
                        result.emplace_back(std::move(val));
                    return true;
                };
            
            c.second->forEach(onValue);
        }
        
        return result;
    }

    virtual std::tuple<BucketList, BucketDescriptions>
    getColumnBuckets(const ColumnName & column, int maxNumBuckets) const override
    {
        auto it = columnIndex.find(column.newHash());
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given name",
                                      "columnName", column,
                                      "knownColumns", getColumnNames());
        }

        std::unordered_map<CellValue, size_t> values;
        std::vector<CellValue> valueList;

        size_t totalRows = 0;

        for (unsigned i = 0;  i < chunks.size();  ++i) {
            auto onValue = [&] (const CellValue & val)
                {
                    if (values.insert({val,0}).second)
                        valueList.push_back(std::move(val));
                    return true;
                };

            chunks[i].columns[it->second]->forEachDistinctValue(onValue);
            totalRows += chunks[i].rowCount();
        }

        BucketDescriptions descriptions;
        descriptions.initialize(valueList, maxNumBuckets);

        for (auto & v: values) {
            v.second = descriptions.getBucket(v.first);            
        }
        
        // Finally, perform the bucketed lookup
        WritableBucketList buckets(totalRows, descriptions.numBuckets());

        for (unsigned i = 0;  i < chunks.size();  ++i) {
            auto onValue = [&] (size_t, const CellValue & val)
                {
                    uint32_t bucket = values[val];
                    buckets.write(bucket);
                    return true;
                };
            
            chunks[i].columns[it->second]->forEach(onValue);
        }

        return std::make_tuple(std::move(buckets), std::move(descriptions));
    }

    virtual uint64_t getColumnRowCount(const ColumnName & column) const
    {
        return rowCount;
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        return columnIndex.count(column.newHash());
    }

    virtual std::vector<ColumnName> getColumnNames() const
    {
        std::vector<ColumnName> result;
        result.reserve(columns.size());
        for (auto & c: columns)
            result.push_back(c.columnName);
        return result;
    }

    // TODO: we know more than this...
    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const
    {
        auto it = columnIndex.find(columnName.newHash());
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnName", columnName,
                                      "knownColumns", getColumnNames());
        }

        ColumnTypes types;

        const ColumnEntry & entry = columns.at(it->second);

        // Go through each chunk with a non-null value
        for (auto & c: entry.chunks) {
            types.update(c.second->getColumnTypes());
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
            throw HttpReturnException
                (400, "Row not found in tabular dataset: "
                 + rowName.toUtf8String(),
                 "rowName", rowName);
        return result;
    }

    virtual bool knownRow(const RowName & rowName) const
    {
        int chunkIndex;
        int rowIndex;

        std::tie(chunkIndex, rowIndex) = tryLookupRow(rowName);
        return chunkIndex >= 0;
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        MatrixNamedRow result;
        result.rowHash = rowName;
        result.rowName = rowName;

        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end()) {
            throw HttpReturnException
                (400, "Row not found in tabular dataset: "
                 + rowName.toUtf8String(),
                 "rowName", rowName);
        }

        result.columns
            = chunks.at(it->second.first)
            .getRow(it->second.second, fixedColumns);
        return result;
    }

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in tabular dataset");
        }

        return RowName(chunks.at(it->second.first).rowNames[it->second.second].toUtf8String());
    }

    virtual ColumnName getColumnName(ColumnHash column) const
    {
        auto it = columnHashIndex.find(column);
        if (it == columnHashIndex.end())
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", getColumnNames());
        return columns[it->second].columnName;
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnName & column, ColumnStats & stats) const
    {
        // WARNING: we don't calculate the correct value here; we don't
        // correctly record the row counts.  We should probably remove it
        // from the interface, since it's hard for any dataset to get it
        // right.
        auto it = columnIndex.find(column.newHash());
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", getColumnNames());
        }

        stats = ColumnStats();

        bool isNumeric = true;

        for (auto & c: columns.at(it->second).chunks) {

            auto onValue = [&] (const CellValue & value)
                {
                    if (!value.isNumber())
                        isNumeric = false;
                    stats.values[value].rowCount_ += 1;
                    return true;
                };
                                
            c.second->forEachDistinctValue(onValue);
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
        return columns.size();
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

    void finalize(std::vector<TabularDatasetChunk> & inputChunks,
                  uint64_t totalRows)
    {
        // NOTE: must be called with the lock held

        rowCount = totalRows;
        
        chunks.reserve(inputChunks.size());

        for (auto & c: inputChunks) {
            chunks.emplace_back(std::move(c));
        }

        columns.reserve(fixedColumns.size());
        for (size_t i = 0;  i < fixedColumns.size();  ++i) {
            const ColumnName & c = fixedColumns[i];
            ColumnEntry entry;
            entry.columnName = c;
            columns.emplace_back(entry);
            columnIndex[c.newHash()] = i;
            columnHashIndex[c] = i;
        }

        // Create the column index.  This should be rapid, as there shouldn't
        // be too many columns.
        for (size_t i = 0;  i < chunks.size();  ++i) {
            const TabularDatasetChunk & chunk = chunks[i];
            ExcAssertEqual(fixedColumns.size(), chunk.columns.size());
            for (size_t j = 0;  j < chunk.columns.size();  ++j) {
                columns[j].chunks.emplace_back(i, chunk.columns[j]);
            }
            for (auto & c: chunk.sparseColumns) {
                auto it = columnIndex.insert(make_pair(c.first.newHash(),
                                                       columns.size()))
                    .first;
                if (it->second == columns.size()) {
                    ColumnEntry entry;
                    entry.columnName = c.first;
                    columns.emplace_back(entry);
                    columnHashIndex[c.first] = it->second;
                }
                columns[it->second].chunks.emplace_back(i, c.second);
            }
        }

        ExcAssertEqual(columns.size(), columnIndex.size());
        ExcAssertEqual(columns.size(), columnHashIndex.size());

        ML::Timer rowIndexTimer;
        //cerr << "creating row index" << endl;
        rowIndex.reserve(4 * totalRows / 3);
        //cerr << "rowIndex capacity is " << rowIndex.capacity() << endl;
        for (unsigned i = 0;  i < chunks.size();  ++i) {
            for (unsigned j = 0;  j < chunks[i].rowNames.size();  ++j) {
                if (!rowIndex.insert({ chunks[i].rowNames[j], { i, j } }).second)
                    throw HttpReturnException(400, "Duplicate row name in tabular dataset",
                                              "rowName", chunks[i].rowNames[j]);
            }
        }
        //cerr << "done creating row index" << endl;
        cerr << "row index took " << rowIndexTimer.elapsed() << endl;

    }

    void initialize(vector<ColumnName> columnNames)
    {
        ExcAssert(this->fixedColumns.empty());
        this->fixedColumns = std::move(columnNames);

        for (size_t i = 0;  i < fixedColumns.size();  ++i) {
            if (!fixedColumnIndex.insert(make_pair(fixedColumns[i].newHash(), i))
                .second)
                throw HttpReturnException(500,
                                          "Duplicate column name in tabular dataset",
                                          "columnName", fixedColumns[i]);
        }
    }

    /** This is a recorder that allows parallel records from multiple
        threads. */
    struct BasicRecorder: public Recorder {
        BasicRecorder(TabularDataStore * store)
            : store(store)
        {
        }

        TabularDataStore * store;

        virtual void
        recordRowExpr(const RowName & rowName,
                      const ExpressionValue & expr) override
        {
            RowValue row;
            expr.appendToRow(ColumnName(), row);
            recordRowDestructive(rowName, std::move(row));
        }

        virtual void
        recordRowExprDestructive(RowName rowName,
                                 ExpressionValue expr) override
        {
            RowValue row;
            ColumnName columnName;
            expr.appendToRowDestructive(columnName, row);
            recordRowDestructive(std::move(rowName), std::move(row));
        }

        virtual void
        recordRow(const RowName & rowName,
                  const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) override
        {
            store->recordRow(rowName, vals);
        }

        virtual void
        recordRowDestructive(RowName rowName,
                             std::vector<std::tuple<ColumnName, CellValue, Date> > vals) override
        {
            store->recordRow(std::move(rowName), std::move(vals));
        }

        virtual void
        recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows) override
        {
            for (auto & r: rows)
                store->recordRow(r.first, r.second);
        }

        virtual void
        recordRowsDestructive(std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows) override
        {
            for (auto & r: rows)
                store->recordRow(std::move(r.first), std::move(r.second));
        }

        virtual void
        recordRowsExpr(const std::vector<std::pair<RowName, ExpressionValue > > & rows) override
        {
            for (auto & r: rows) {
                recordRowExpr(r.first, r.second);
            }
        }

        virtual void
        recordRowsExprDestructive(std::vector<std::pair<RowName, ExpressionValue > > rows) override
        {
            for (auto & r: rows) {
                recordRowExprDestructive(std::move(r.first), std::move(r.second));
            }
        }

        virtual void finishedChunk() override
        {
        }
        
    };


    /** This is a recorder that is designed to have each thread record
        chunks in a deterministic manner.
    */
    struct ChunkRecorder: public Recorder {
        ChunkRecorder(TabularDataStore * store)
            : store(store), doneFirst(store->mutableChunks.load())
        {
            // Note that this may return a null pointer, if nothing has
            // been loaded yet.
            chunk = store->createNewChunk(TABULAR_DATASET_DEFAULT_ROWS_PER_CHUNK);
        }

        TabularDataStore * store;
        bool doneFirst;

        std::shared_ptr<MutableTabularDatasetChunk> chunk;

        virtual void
        recordRowExpr(const RowName & rowName,
                      const ExpressionValue & expr) override
        {
            RowValue row;
            expr.appendToRow(ColumnName(), row);
            recordRowDestructive(rowName, std::move(row));
        }

        virtual void
        recordRowExprDestructive(RowName rowName,
                                 ExpressionValue expr) override
        {
            RowValue row;
            ColumnName columnName;
            expr.appendToRowDestructive(columnName, row);
            recordRowDestructive(std::move(rowName), std::move(row));
        }

        virtual void
        recordRow(const RowName & rowName,
                  const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) override
        {
            recordRowImpl(rowName, vals);
        }

        virtual void
        recordRowDestructive(RowName rowName,
                             std::vector<std::tuple<ColumnName, CellValue, Date> > vals) override
        {
            recordRowImpl(std::move(rowName), std::move(vals));
        }

        template<typename Vals>
        void recordRowImpl(RowName rowName, Vals&& vals)
        {
            if (!chunk) {
                {
                    std::unique_lock<std::mutex> guard(store->datasetMutex);
                    store->createFirstChunks(vals);
                }

                chunk = store->createNewChunk(TABULAR_DATASET_DEFAULT_ROWS_PER_CHUNK);
            }
            ExcAssert(chunk);


            // Prepare what we need to record
            auto rowVals = store->prepareRow(vals);

            std::vector<CellValue> & orderedVals = std::get<0>(rowVals);
            std::vector<std::pair<ColumnName, CellValue> > & newColumns
                = std::get<1>(rowVals);
            Date ts = std::get<2>(rowVals);

            for (;;) {
                int written = chunk->add(rowName, ts,
                                         orderedVals.data(),
                                         orderedVals.size(),
                                         newColumns);
                if (written == MutableTabularDatasetChunk::ADD_SUCCEEDED)
                    break;
                
                ExcAssertEqual(written,
                               MutableTabularDatasetChunk::ADD_PERFORM_ROTATION);
                finishedChunk();
                chunk.reset(new MutableTabularDatasetChunk(orderedVals.size(), 65536));
            }
        }

        virtual void
        recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows) override
        {
            for (auto & r: rows)
                recordRow(r.first, r.second);
        }

        virtual void
        recordRowsDestructive(std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows) override
        {
            for (auto & r: rows)
                recordRowDestructive(std::move(r.first), std::move(r.second));
        }

        virtual void
        recordRowsExpr(const std::vector<std::pair<RowName, ExpressionValue > > & rows) override
        {
            for (auto & r: rows) {
                recordRowExpr(r.first, r.second);
            }
        }

        virtual void
        recordRowsExprDestructive(std::vector<std::pair<RowName, ExpressionValue > > rows) override
        {
            for (auto & r: rows) {
                recordRowExprDestructive(std::move(r.first), std::move(r.second));
            }
        }

        virtual void finishedChunk() override
        {
            if (!chunk || chunk->rowCount() == 0)
                return;
            auto frozen = chunk->freeze();
            store->addFrozenChunk(std::move(frozen));
        }

        virtual
        std::function<void (RowName rowName,
                            Date timestamp,
                            CellValue * vals,
                            size_t numVals,
                            std::vector<std::pair<ColumnName, CellValue> > extra)>
        specializeRecordTabular(const std::vector<ColumnName> & columnNames) override
        {
            /* We return a function that knows it will always receive the same
               set of columns.  This allows us to directly record them without
               needing to do any manipulation of column names at all.
            */

            return [=] (RowName rowName, Date timestamp,
                        CellValue * vals, size_t numVals,
                        std::vector<std::pair<ColumnName, CellValue> > extra)
                {
                    if (!chunk) {
                        {
                            std::unique_lock<std::mutex> guard(store->datasetMutex);

                            // We create a sample set of values for the
                            // column to analyze, so it can identify the
                            // column names.
                            std::vector<std::tuple<ColumnName, CellValue, Date> > sampleVals;
                            for (unsigned i = 0;  i < columnNames.size();  ++i)
                                sampleVals.emplace_back(columnNames[i], vals[i], timestamp);
                   
                            store->createFirstChunks(sampleVals);
                        }

                        chunk = store->createNewChunk(TABULAR_DATASET_DEFAULT_ROWS_PER_CHUNK);
                    }
                    ExcAssert(chunk);


                    for (;;) {
                        int written = chunk->add(rowName, timestamp,
                                                 vals, numVals,
                                                 extra);
                        if (written == MutableTabularDatasetChunk::ADD_SUCCEEDED)
                            break;
                        
                        ExcAssertEqual(written,
                                       MutableTabularDatasetChunk::ADD_PERFORM_ROTATION);
                        finishedChunk();
                        chunk.reset(new MutableTabularDatasetChunk(columnNames.size(), 65536));
                    }
                };
        }
    };

    void commit()
    {
        // No mutable chunks anymore.  Atomically swap out the old pointer.
        auto oldMutableChunks = mutableChunks.exchange(nullptr);

        if (!oldMutableChunks)
            return;  // a parallel commit beat us to it

        for (auto & c: *oldMutableChunks) {
            // Wait for us to have the only reference to the chunk
            auto p = c.load();
            // We have one reference here
            // There is one reference inside oldMutableChunks
            while (p.use_count() != 2) ;

            if (p->rowCount() != 0)
                freezeChunkInBackground(p);

            c.store(nullptr);
        }

        // Wait for the background freeze events to finish.  We do it by
        // busy waiting while working in between, to ensure that we don't
        // deadlock if there are no other threads available to do the
        // work.
        while (backgroundJobsActive)
            ThreadPool::instance().work();

        // We can only take the mutex here, as the background threads need
        // to access it.
        std::unique_lock<std::mutex> guard(datasetMutex);

        // At this point, nobody can see oldMutableChunks or its contents
        // apart from this thread.  So we can perform operations unlocked
        // on it without any problem.

        // Freeze all of the uncommitted chunks
        std::atomic<size_t> totalRows(0);

        for (auto & c: frozenChunks)
            totalRows += c.rowCount();

        finalize(frozenChunks, totalRows);

        size_t mem = 0;
        for (auto & c: chunks) {
            mem += c.memusage();
        }

        cerr << "total mem usage is " << mem << " bytes" << " for "
             << 1.0 * mem / rowCount << " bytes/row" << endl;
    }

    /// The number of background jobs that we're currently waiting for
    std::atomic<size_t> backgroundJobsActive;

    // freezes a new chunk in the background, and adds it to frozenChunks.
    // Updates the number of background jobs atomically so that we can know
    // when everything is finished.
    void freezeChunkInBackground(std::shared_ptr<MutableTabularDatasetChunk> chunk)
    {
        if (chunk->rowCount() == 0)
            return;

        auto job = [=] ()
            {
                Scope_Exit(--this->backgroundJobsActive);
                auto frozen = chunk->freeze();
                addFrozenChunk(std::move(frozen));
            };
        
        ++backgroundJobsActive;
        try {
            ThreadPool::instance().add(std::move(job));
        } catch (...) {
            --backgroundJobsActive;
            throw;
        }
    }

    void addFrozenChunk(TabularDatasetChunk frozen)
    {
        ExcAssertNotEqual(frozen.rowCount(), 0);
        std::unique_lock<std::mutex> guard(datasetMutex);
        frozenChunks.emplace_back(std::move(frozen));
    }

    std::shared_ptr<MutableTabularDatasetChunk>
    createNewChunk(size_t expectedSize)
    {
        // Have we initialized things yet?
        bool mc = mutableChunks.load() != nullptr;
        if (!mc)
            return nullptr;

        return std::make_shared<MutableTabularDatasetChunk>
            (fixedColumns.size(), expectedSize);
    }

    /** Analyze the first row to know what the columns are. */
    void createFirstChunks(const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        // Must be done with the dataset lock held
        if (!mutableChunks.load()) {
            //need to create the mutable chunk
            vector<ColumnName> columnNames;

            //The first recorded row will determine the columns
            ML::Lightweight_Hash<uint64_t, int> inputColumnIndex;
            for (unsigned i = 0;  i < vals.size();  ++i) {
                const ColumnName & c = std::get<0>(vals[i]);
                uint64_t ch(c.newHash());
                if (!inputColumnIndex.insert(make_pair(ch, i)).second)
                    throw HttpReturnException(400, "Duplicate column name in tabular dataset entry",
                                              "columnName", c.toString());
                columnNames.push_back(c);
            }

            initialize(std::move(columnNames));

            auto newChunks = std::make_shared<ChunkList>(NUM_PARALLEL_CHUNKS);
            
            for (auto & c: *newChunks) {
                auto newChunk = std::make_shared<MutableTabularDatasetChunk>
                    (fixedColumns.size(),
                     TABULAR_DATASET_DEFAULT_ROWS_PER_CHUNK);
                c.store(std::move(newChunk));
            }
            
            auto old = mutableChunks.exchange(std::move(newChunks));
            ExcAssert(!old);
        }
    }

    // Vals is std::vector<std::tuple<ColumnName, CellValue, Date> >
    // either a const reference (in which case we copy), or a
    // rvalue or non-const reference (in which case we move)
    template<typename Vals>
    std::tuple<std::vector<CellValue>,
               std::vector<std::pair<ColumnName, CellValue> >,
               Date>
    prepareRow(Vals&& vals)
    {
        std::vector<CellValue> orderedVals(fixedColumns.size());
        Date ts = Date::negativeInfinity();

        std::vector<std::pair<ColumnName, CellValue> > newColumns;

        for (unsigned i = 0;  i < vals.size();  ++i) {
            const ColumnName & c = std::get<0>(vals[i]);
            auto iter = fixedColumnIndex.find(c.newHash());
            if (iter == fixedColumnIndex.end()) {
                switch (config.unknownColumns) {
                case UC_ERROR:
                    throw HttpReturnException(400, "New column name while recording row in tabular dataset", "columnName", c.toString());
                case UC_IGNORE:
                    continue;
                case UC_ADD:
                    newColumns.emplace_back(std::move(std::get<0>(vals[i])),
                                            std::move(std::get<1>(vals[i])));
                    continue;
                }
            }

            orderedVals[iter->second] = std::move(std::get<1>(vals[i]));

            ts = std::max(ts, std::get<2>(vals[i]));
        }

        return std::make_tuple(std::move(orderedVals),
                               std::move(newColumns),
                               std::move(ts));
    }

    // Vals is std::vector<std::tuple<ColumnName, CellValue, Date> >
    // Same const/non-const as is happening above
    template<typename Vals>
    void recordRow(RowName rowName,
                   Vals&& vals)
    {
        if (rowCount > 0)
            HttpReturnException(400, "Tabular dataset has already been committed, cannot add more rows");

        auto mc = mutableChunks.load();

        if (!mc) {
            std::unique_lock<std::mutex> guard(datasetMutex);
            createFirstChunks(vals);
            mc = mutableChunks.load();
        }

        ExcAssert(mc);

        // Prepare what we need to record
        auto rowVals = prepareRow(vals);

        std::vector<CellValue> & orderedVals = std::get<0>(rowVals);
        std::vector<std::pair<ColumnName, CellValue> > & newColumns
            = std::get<1>(rowVals);
        Date ts = std::get<2>(rowVals);

        int chunkNum = (rowName.hash() >> 32) % mc->n;

        for (int written = MutableTabularDatasetChunk::ADD_PERFORM_ROTATION;
             written != MutableTabularDatasetChunk::ADD_SUCCEEDED;) {
            auto chunkPtr = mc->chunks[chunkNum].load();
            ExcAssert(chunkPtr);
            written = chunkPtr->add(rowName, ts,
                                    orderedVals.data(),
                                    orderedVals.size(),
                                    newColumns);
            if (written == MutableTabularDatasetChunk::ADD_AWAIT_ROTATION)
                continue;  // busy wait until the rotation is done by another thread
            else if (written
                     == MutableTabularDatasetChunk::ADD_PERFORM_ROTATION) {
                // We need a rotation, and we've been selected to do it
                auto newChunk = std::make_shared<MutableTabularDatasetChunk>
                    (fixedColumns.size(), TABULAR_DATASET_DEFAULT_ROWS_PER_CHUNK);
                if (mc->chunks[chunkNum]
                    .compare_exchange_strong(chunkPtr, newChunk)) {
                    // Successful rotation.  First we background freeze
                    // the chunk.  Then the old one goes in the list of
                    // uncommitted chunks.

                    freezeChunkInBackground(std::move(chunkPtr));
                }
            }
        }
    }
};

TabularDataset::
TabularDataset(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    itl = make_shared<TabularDataStore>(config.params.convert<TabularDatasetConfig>());
}

TabularDataset::
~TabularDataset()
{
}

Any
TabularDataset::
getStatus() const
{
    Json::Value status;
    status["rowCount"] = itl->rowCount;
    status["columnCount"] = itl->columns.size();
    return status;
}

std::pair<Date, Date>
TabularDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
TabularDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
TabularDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
TabularDataset::
getRowStream() const 
{ 
    return std::make_shared<TabularDataStore::TabularDataStoreRowStream>
        (itl.get()); 
} 

GenerateRowsWhereFunction
TabularDataset::
generateRowsWhere(const SqlBindingScope & context,
                  const Utf8String& alias,
                  const SqlExpression & where,
                  ssize_t offset,
                  ssize_t limit) const
{
    GenerateRowsWhereFunction fn
        = itl->generateRowsWhere(context, where, offset, limit);
    if (!fn)
        fn = Dataset::generateRowsWhere(context, alias, where, offset, limit);
    return fn;
}

KnownColumn
TabularDataset::
getKnownColumnInfo(const ColumnName & columnName) const
{
    return itl->getKnownColumnInfo(columnName);
}

void
TabularDataset::
commit()
{
    return itl->commit();
}

Dataset::MultiChunkRecorder
TabularDataset::
getChunkRecorder()
{
    MultiChunkRecorder result;
    result.newChunk = [=] (size_t)
        {
            return std::unique_ptr<Recorder>
                (new TabularDataStore::ChunkRecorder(itl.get()));
        };

    result.commit = [=] () { this->commit(); };
    return result;
}

void
TabularDataset::
recordRowItl(const RowName & rowName,
             const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    validateNames(rowName, vals);
    itl->recordRow(rowName, vals);
}

void
TabularDataset::
recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
{
    for (auto & r: rows)
        itl->recordRow(r.first, r.second);
}

/*****************************************************************************/
/* TABULAR DATASET                                                           */
/*****************************************************************************/

TabularDatasetConfig::
TabularDatasetConfig()
{
    unknownColumns = UC_ERROR;
}

DEFINE_ENUM_DESCRIPTION(UnknownColumnAction);

UnknownColumnActionDescription::
UnknownColumnActionDescription()
{
    addValue("ignore", UC_IGNORE, "Unknown columns will be ignored");
    addValue("error", UC_ERROR, "Unknown columns will result in an error");
    addValue("add", UC_ADD, "Unknown columns will be added as a sparse column");
};

DEFINE_STRUCTURE_DESCRIPTION(TabularDatasetConfig);

TabularDatasetConfigDescription::
TabularDatasetConfigDescription()
{
    nullAccepted = true;

    addField("unknownColumns", &TabularDatasetConfig::unknownColumns,
             "Action to take on unknown columns.  Values are 'ignore', "
             "'error' (default), or 'add' which will allow an unlimited "
             "number of sparse columns to be added.",
             UC_ERROR);
}

namespace {

RegisterDatasetType<TabularDataset, TabularDatasetConfig>
regTabular(builtinPackage(),
           "tabular",
           "Dense dataset which can be recorded to",
           "datasets/TabularDataset.md.html");

} // file scope*/

} // MLDB
} // Datacratic
