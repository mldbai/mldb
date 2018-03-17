/** tabular_dataset.cc                                             -*- C++ -*-
    Jeremy Barnes, 26 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
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
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/types/set_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/atomic_shared_ptr.h"
#include "mldb/jml/utils/floating_point.h"
#include "mldb/utils/log.h"
#include "mldb/server/dataset_utils.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/dataset_utils.h"
#include "mldb/jml/db/persistent.h"
#include <mutex>


using namespace std;


namespace MLDB {

static constexpr size_t NUM_PARALLEL_CHUNKS=8;

struct PathIndex;


/*****************************************************************************/
/* MUTABLE PATH INDEX                                                        */
/*****************************************************************************/

struct MutablePathIndex {
    static constexpr size_t INDEX_SHARDS=32;

    struct Recorder {

        Recorder(uint32_t chunkNumber,
                 MutablePathIndex * owner)
            : chunkNumber(chunkNumber),
              owner(owner)
        {
        }

        void record(const Path & path,
                    uint32_t indexInChunk)
        {
            uint64_t hash = path.hash();
            int shard = getShard(hash);
            toInsert[shard].emplace_back(hash, indexInChunk);
            maxChunkIndex = std::max(maxChunkIndex, indexInChunk);
        }

        void commit()
        {
            for (size_t i = 0;  i < INDEX_SHARDS;  ++i) {
                size_t shard = (i + chunkNumber) % INDEX_SHARDS;
                std::unique_lock<std::mutex> guard(owner->indexLock[shard]);

                if (i == 0) {
                    owner->maxChunkIndex
                        = std::max(owner->maxChunkIndex, maxChunkIndex);
                    owner->maxChunkNumber
                        = std::max(owner->maxChunkNumber, chunkNumber);
                }

                for (auto & e: toInsert[shard]) {
                    uint64_t hash = e.first;
                    int32_t indexInChunk = e.second;
                    owner->index[shard]
                        .emplace_back(hash, chunkNumber, indexInChunk);
                }
            }
        }

        std::vector<std::pair<uint64_t, uint32_t> > toInsert[INDEX_SHARDS];
        uint32_t chunkNumber;
        MutablePathIndex * owner;
        uint32_t maxChunkIndex = 0;
    };

    Recorder getRecorder(int chunkNumber)
    {
        return Recorder(chunkNumber, this);
    }

    std::pair<PathIndex, std::vector<std::tuple<int, int, int, int> > >
    freeze(MappedSerializer & serializer);

    /// Index from hash to (chunk, indexInChunk)
    std::mutex indexLock[INDEX_SHARDS];
    std::vector<std::tuple<uint64_t, int, int> > index[INDEX_SHARDS];
    uint32_t maxChunkIndex = 0;
    uint32_t maxChunkNumber = 0;

    static int getShard(uint64_t hash)
    {
        return (hash >> 23) % INDEX_SHARDS;
    }
};


/*****************************************************************************/
/* PATH INDEX                                                                */
/*****************************************************************************/

struct PathIndexMetadata {
    // Bits required to serialize a chunk number
    uint8_t chunkBits = 0;

    // Bits required to serialize an offset
    uint8_t offsetBits = 0;

    // How many entries are in storage?
    uint64_t numEntries = 0;

    // Factor to multiply by to turn a hash value into an entry number
    double factor = 0.0;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(PathIndexMetadata)
{
    setVersion(1);
    addField("chunkBits", &PathIndexMetadata::chunkBits, "");
    addField("offsetBits", &PathIndexMetadata::offsetBits, "");
    addField("numEntries", &PathIndexMetadata::numEntries, "");
    addField("factor", &PathIndexMetadata::factor, "");
}

struct PathIndexShard: public PathIndexMetadata {
    
    size_t getBucket(uint64_t hash) const
    {
        size_t bucket = hash * factor;
        ExcAssertGreaterEqual(bucket, 0);
        ExcAssertLess(bucket, numEntries);
        return bucket;
    }

    /** Initialize the index in the shard.  Returns a list of possible
        collisions, with (shard1, offset1, shard2, offset2) as the
        format.
    */
    std::vector<std::tuple<int, int, int, int> >
    init(MappedSerializer & serializer,
         std::vector<std::tuple<uint64_t, int, int> > input,
         size_t numChunks,
         size_t maxChunkSize)
    {
        std::vector<std::tuple<int, int, int, int> > possibleCollisions;

        std::sort(input.begin(), input.end());

        chunkBits = ML::highest_bit(numChunks, -1) + 1;
        offsetBits = ML::highest_bit(maxChunkSize - 1, -1) + 1;

        // Create a hash that's 50% full at the end, by doubling the
        // size.  We want to leave plenty of space since we handle
        // collisions by simply advancing, and so a large number of
        // contiguous collisions can make lookups really slow.
        numEntries = input.size() * 2;

        size_t wordsRequired
            = (numEntries * (chunkBits + offsetBits) + 31) / 32;
        
        auto storage = serializer.allocateWritableT<uint32_t>(wordsRequired);
        std::fill(storage.data(), storage.data() + storage.length(), 0);

        // Expansion factor to turn a hash value into a position in the
        // bucket.  This is done linearly so that we access memory
        // sequentially.
        factor = 2.0 * input.size() / std::numeric_limits<uint64_t>::max();

        auto setEntry = [&] (size_t bucket, int chunkNumber, int indexInChunk)
            {
                ML::Bit_Writer<uint32_t> writer(storage.data());
                writer.skip((chunkBits + offsetBits) * bucket);
                writer.write(chunkNumber + 1, chunkBits);
                writer.write(indexInChunk, offsetBits);
            };

        auto entryIsOccupied = [&] (size_t bucket) -> bool
            {
                ML::Bit_Extractor<uint32_t> bits(storage.data());
                bits.advance((chunkBits + offsetBits) * bucket);
                uint32_t chunk = bits.extract<uint32_t>(chunkBits);
                return chunk > 0;
            };
     
        int collisions = 0;
        size_t maxOffset = 0;
        size_t totalOffset = 0;
        
        for (size_t idx = 0;  idx < input.size();  ++idx) {
            auto & i = input[idx];
            uint64_t hash = std::get<0>(i);
            int chunkNumber = std::get<1>(i);
            int indexInChunk = std::get<2>(i);

            if (idx > 0) {
                if (std::get<0>(input[idx - 1]) == hash) {
                    // potential collision
                    int lastChunkNumber = std::get<1>(input[idx - 1]);
                    int lastIndexInChunk = std::get<2>(input[idx - 1]);

                    possibleCollisions.emplace_back
                        (lastChunkNumber, lastIndexInChunk,
                         chunkNumber, indexInChunk);
                }
            }

            size_t bucket = getBucket(hash);

            collisions += entryIsOccupied(bucket);

            size_t offset = 0;

            for (int i = 0;  i < 1000;  ++i) /*while (true)*/ {
                if (!entryIsOccupied(bucket)) {
                    // empty
                    setEntry(bucket, chunkNumber, indexInChunk);
                    break;
                }
                else {
                    ++bucket;
                    ++offset;
                    if (bucket == numEntries)
                        bucket = 0;
                    if (i == 999) {
                        cerr << "Error with collisions" << endl;
                        cerr << "bucket = " << bucket << endl;
                        cerr << "offset = " << offset << endl;
                        cerr << "numEntries = " << numEntries << endl;
                        throw HttpReturnException(500, "Hash bucket error");
                    }
                }
            }

            maxOffset = std::max(maxOffset, offset);
            totalOffset += offset;
        }

        //cerr << "collision rate = " << 100.0 * collisions / input.size()
        //     << "%" << endl;
        //cerr << "max offset = " << maxOffset << endl;
        //cerr << "avg offset = " << 1.0 * totalOffset / input.size() << endl;

        this->storage = storage.freeze();

        return possibleCollisions;
    }

    compact_vector<std::pair<int, int>, 4>
    pathPossibleChunks(const Path & path) const
    {
        return pathPossibleChunks(path.hash());
    }

    compact_vector<std::pair<int, int>, 4>
    pathPossibleChunks(uint64_t hash) const
    {
        compact_vector<std::pair<int, int>, 4> result;
        if (numEntries == 0)
            return result;
        size_t bucket = hash * factor;
        while (entryIsOccupied(bucket)) {
            result.push_back(getEntry(bucket));
            ++bucket;
            if (bucket == numEntries)
                bucket = 0;
        }
        
        return result;
    }

    size_t memusage() const
    {
        return sizeof(*this) + storage.memusage();
    }

    std::pair<uint32_t, uint32_t>
    getEntry(size_t bucket) const
    {
        ML::Bit_Extractor<uint32_t> bits(storage.data());
        bits.advance((chunkBits + offsetBits) * bucket);
        uint32_t chunk = bits.extract<uint32_t>(chunkBits) - 1;
        uint32_t offset = bits.extract<uint32_t>(offsetBits);
        return {chunk, offset};
    }

    bool entryIsOccupied(size_t bucket) const
    {
        ML::Bit_Extractor<uint32_t> bits(storage.data());
        bits.advance((chunkBits + offsetBits) * bucket);
        uint32_t chunk = bits.extract<uint32_t>(chunkBits);
        return chunk > 0;
    }

    // Hash is implicit via position in the entry map (we take the top x bits)
    // It returns the chunk number that contains that hash portion
    // linear chaining

    // Actual storage for bit-packed values
    FrozenMemoryRegionT<uint32_t> storage;
};


struct PathIndex {
    static constexpr size_t INDEX_SHARDS=MutablePathIndex::INDEX_SHARDS;

    compact_vector<std::pair<int, int>, 4>
    pathPossibleChunks(const Path & path) const
    {
        return pathPossibleChunks(path.hash());
    }

    compact_vector<std::pair<int, int>, 4>
    pathPossibleChunks(uint64_t hash) const
    {
        int shard = MutablePathIndex::getShard(hash);
        return shards[shard].pathPossibleChunks(hash);
    }

    size_t memusage() const
    {
        size_t result = sizeof(*this);
        for (auto & shard: shards) {
            result += shard.memusage();
        }
        return result;
    }

    // Hash is implicit via position in the entry map (we rescale the
    // hash range)
    // It returns the chunk number that contains that hash portion
    // linear chaining
    PathIndexShard shards[INDEX_SHARDS];
};

std::pair<PathIndex, std::vector<std::tuple<int, int, int, int> > >
MutablePathIndex::
freeze(MappedSerializer & serializer)
{
    PathIndex result;
    std::vector<std::tuple<int, int, int, int> >
        possibleCollisions[INDEX_SHARDS];
    std::atomic<size_t> totalPossibleCollisions(0);
    
    // Index each shard in parallel
    auto onShard = [&] (int shardNumber)
        {
            possibleCollisions[shardNumber]
            = result.shards[shardNumber]
            .init(serializer,
                  index[shardNumber],
                  maxChunkNumber + 1, maxChunkIndex + 1);
            totalPossibleCollisions += possibleCollisions[shardNumber].size();
        };

    parallelMap(0, INDEX_SHARDS, onShard);

    std::vector<std::tuple<int, int, int, int> > collisions;
    collisions.reserve(totalPossibleCollisions);
    for (auto & c: possibleCollisions) {
        collisions.insert(collisions.end(), c.begin(), c.end());
    }
    
    return std::make_pair(result, collisions);
}


/*****************************************************************************/
/* TABULAR DATA STORE                                                        */
/*****************************************************************************/

/** Data store that can record tabular data and act as a matrix and
    column view to the underlying data.
*/

struct TabularDataset::TabularDataStore
    : public ColumnIndex, public MatrixView {

    // Find the optimal chunk size.  For narrow datasets, we want
    // big chunks because it means less overhead.  However, for
    // wide datasets we want a lot less as otherwise there is far too
    // much memory allocated and the TLB can't hold all of the entries
    // for all of the columns.
    static size_t chunkSizeForNumColumns(size_t numColumns)
    {
        if (numColumns == 0)
            numColumns = 1;
        size_t rowsPerChunk
            = std::min<size_t>(131072, 131072*numCpus()/numColumns);
        if (rowsPerChunk < 16)
            rowsPerChunk = 16;
        return rowsPerChunk;
    }

    TabularDataStore(MldbServer * server,
                     TabularDatasetConfig config,
                     shared_ptr<spdlog::logger> logger)
        : server(server),
          currentState(std::make_shared<CurrentState>(this, logger)),
          config(std::move(config)),
          backgroundJobsActive(0), logger(std::move(logger))
    {
        ExcAssert(this->logger);
    }

    MldbServer * server = nullptr;

    /// This is used to allocate mapped memory when chunks are frozen
    MemorySerializer serializer;

    /// Provides information about a column
    struct ColumnEntry {
        ColumnPath columnName;

        /// The number of non-null values of this row
        size_t nonNullRowCount = 0;

        /// The set of chunks that contain the column.  This may not be all
        /// chunks for sparse columns.
        std::vector<std::pair<uint32_t, std::shared_ptr<const FrozenColumn> > > chunks;
    };

    /// Holds the entire state of the dataset.  Each of these is
    /// immutable once committed.
    struct CurrentState
        : public ColumnIndex, public MatrixView, public Dataset,
          public std::enable_shared_from_this<CurrentState> {

        CurrentState(TabularDataStore * owner,
                     shared_ptr<spdlog::logger> logger)
            : Dataset(owner->server),
              owner(owner)
        {
            this->logger = logger;
            ExcAssert(this->logger);
        }

        /// Our owner
        TabularDataStore * owner;

        /// Lets us look up which chunk and row number contains a row
        PathIndex rowIndex;

        /// Total number of rows in the dataset
        int64_t rowCount = 0;

        /// This indexes column names to their index, using new (fast) hash
        Lightweight_Hash<uint64_t, int> columnIndex;

        /// Same index, but using the old (slow) hash.  Useful only for when
        /// we are forced to lookup on ColumnHash.
        Lightweight_Hash<ColumnHash, int> columnHashIndex;

        /// List of all columns in the dataset
        std::vector<ColumnEntry> columns;
    
        /// List of all chunks in the dataset
        std::vector<std::shared_ptr<const TabularDatasetChunk> > chunks;

        Date earliestTs = Date::positiveInfinity();
        Date  latestTs = Date::negativeInfinity();

        virtual Any getStatus() const override
        {
            Json::Value status;
            status["rowCount"] = rowCount;
            status["columnCount"] = columns.size();
            return status;
        }

        virtual std::shared_ptr<MatrixView>
        getMatrixView() const
        {
            auto thisPtr = this->shared_from_this();
            std::shared_ptr<const MatrixView> viewPtr
                = thisPtr;
           return std::const_pointer_cast<MatrixView>(viewPtr);
        }

        virtual std::shared_ptr<ColumnIndex>
        getColumnIndex() const
        {
            auto thisPtr = this->shared_from_this();
            std::shared_ptr<const ColumnIndex> indexPtr
                = thisPtr;
            return std::const_pointer_cast<ColumnIndex>(indexPtr);
        }

        // Return the value of the column for all rows
        virtual MatrixColumn getColumn(const ColumnPath & column) const override
        {
            auto it = columnIndex.find(column.oldHash());
            if (it == columnIndex.end()) {
                throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                          "columnHash", column,
                                          "knownColumns", getColumnPaths(0, -1));
            }
        
            MatrixColumn result;
            result.columnHash = result.columnName = column;

            for (auto & c: columns[it->second].chunks) {
                chunks.at(c.first)->addToColumn(it->second, column, result.rows,
                                                false /* dense */);
            }
        
            return result;
        }

        virtual std::vector<CellValue>
        getColumnDense(const ColumnPath & column) const override
        {
            auto it = columnIndex.find(column.oldHash());
            if (it == columnIndex.end()) {
                throw HttpReturnException(400, "Tabular dataset contains no column with given name",
                                          "columnName", column,
                                          "knownColumns", getColumnPaths(0, -1));
            }

            const ColumnEntry & entry = columns[it->second];

            std::vector<CellValue> result;
            result.reserve(entry.nonNullRowCount);

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
        getColumnBuckets(const ColumnPath & column, int maxNumBuckets) const override
        {
            auto it = columnIndex.find(column.oldHash());
            if (it == columnIndex.end()) {
                throw HttpReturnException(400, "Tabular dataset contains no column with given name",
                                          "columnName", column,
                                          "knownColumns", getColumnPaths(0, -1));
            }

            std::atomic<size_t> totalRows(0);

            std::vector<std::vector<double> > numerics(chunks.size());
            std::vector<std::vector<Utf8String> > strings(chunks.size());
            std::atomic<bool> hasNulls(false);

            auto onChunk = [&] (size_t i)
                {
                    auto onValue = [&] (const CellValue & val)
                    {
                        if (val.empty()) {
                            if (!hasNulls)
                                hasNulls = true;
                        }
                        else if (val.isNumber()) {
                            numerics[i].emplace_back(val.toDouble());
                        }
                        else if (val.isString()) {
                            strings[i].emplace_back(val.toUtf8String());
                        }
                        else {
                            throw HttpReturnException
                            (400, "Can only bucketize numbers and strings, not "
                             + jsonEncodeStr(val));
                        }
                        return true;
                    };

                    chunks[i]->getColumnByIndex(it->second).forEachDistinctValue(onValue);
                
                    totalRows += chunks[i]->rowCount();
                };
        
            parallelMap(0, chunks.size(), onChunk);

            DEBUG_MSG(logger) << chunks.size() << " chunks and " << totalRows << " rows";

            auto sortedNumerics = parallelMergeSortUnique(numerics, ML::safe_less<double>());
            auto sortedStrings = parallelMergeSortUnique(strings);

            BucketDescriptions desc;
            desc.initialize(hasNulls,
                            std::move(sortedNumerics),
                            std::move(sortedStrings),
                            maxNumBuckets);

            WritableBucketList buckets(totalRows, desc.numBuckets());

            size_t numWritten = 0;

            auto onChunk2 = [&] (size_t i)
                {

                    auto onRow = [&] (size_t rowNum, const CellValue & val)
                    {
                        uint32_t bucket = desc.getBucket(val);
                        buckets.write(bucket);
                        ++numWritten;
                        return true;
                    };
                
                    chunks[i]->columns[it->second]->forEachDense(onRow);
                };
        
            for (size_t i = 0;  i < chunks.size();  ++i)
                onChunk2(i);

            if (numWritten != totalRows) {
                throw HttpReturnException
                    (500, "Column " + column.toUtf8String()
                     + " had wrong number written ("
                     + to_string(numWritten) + " vs " + to_string(totalRows)
                     + "); internal error (contact support with your script and "
                     + "dataset if possible");
            }

            ExcAssertEqual(numWritten, totalRows);

            return std::make_tuple(std::move(buckets), std::move(desc));
        }

        virtual uint64_t getColumnRowCount(const ColumnPath & column) const override
        {
            return rowCount;
        }

        virtual bool knownColumn(const ColumnPath & column) const override
        {
            return columnIndex.count(column.oldHash());
        }

        virtual std::vector<ColumnPath>
        getColumnPaths(ssize_t offset, ssize_t limit) const override
        {
            std::vector<ColumnPath> result;
            result.reserve(columns.size());
            for (auto & c: columns)
                result.push_back(c.columnName);
            std::sort(result.begin(), result.end());
            return applyOffsetLimit(offset, limit, result);
        }

        // TODO: we know more than this...
        virtual KnownColumn
        getKnownColumnInfo(const ColumnPath & columnName) const
        {
            auto it = columnIndex.find(columnName.oldHash());
            if (it == columnIndex.end()) {
                throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                          "columnName", columnName,
                                          "knownColumns", getColumnPaths(0, -1));
            }

            ColumnTypes types;

            const ColumnEntry & entry = columns.at(it->second);

            // Go through each chunk with a non-null value
            for (auto & c: entry.chunks) {
                types.update(c.second->getColumnTypes());
            }

            ExcAssert(this->logger);

            DEBUG_MSG(logger) << "knownColumnInfo for " << columnName << " is "
                              << jsonEncodeStr(types.getExpressionValueInfo());
            //DEBUG_MSG(logger) << "hasNulls = " << types.hasNulls << endl;
            //DEBUG_MSG(logger) << "hasIntegers = " << types.hasIntegers << endl;
            DEBUG_MSG(logger) << "minNegativeInteger = " << types.minNegativeInteger;
            DEBUG_MSG(logger) << "maxPositiveInteger = " << types.maxPositiveInteger;
            //DEBUG_MSG(logger) << "hasReals = " << types.hasReals << endl;
            //DEBUG_MSG(logger) << "hasStrings = " << types.hasStrings << endl;
            //DEBUG_MSG(logger) << "hasOther = " << types.hasOther << endl;

            return KnownColumn(columnName, types.getExpressionValueInfo(),
                               COLUMN_IS_DENSE);
        }

        template<typename T>
        std::vector<T>
        getRowPathsT(ssize_t start, ssize_t limit) const
        {
            std::vector<T> result;
            if (limit == -1)
                result.reserve(std::min<ssize_t>(0, rowCount - start));
            else result.reserve(limit);

            size_t n = 0;
            for (size_t chunk = 0;  chunk < chunks.size();
                 n += chunks[chunk++]->rowCount()) {
                const TabularDatasetChunk & c = *chunks[chunk];

                if (limit != -1 && n >= start + limit)
                    break;
                if (n + c.rowCount() < start)
                    continue;

                size_t chunkStart = std::max<ssize_t>(0, start - n);
                size_t chunkEnd = c.rowCount();
                if (limit != -1)
                    chunkEnd = std::min<size_t>(chunkEnd, chunkStart + limit);

                for (size_t i = chunkStart;  i < chunkEnd;  ++i) {
                    result.emplace_back(c.getRowPath(i));
                }
            }

            return result;
        }

        virtual std::vector<RowPath>
        getRowPaths(ssize_t start = 0, ssize_t limit = -1) const override
        {
            return getRowPathsT<RowPath>(start, limit);
        }

        virtual std::vector<RowHash>
        getRowHashes(ssize_t start = 0, ssize_t limit = -1) const override
        {
            return getRowPathsT<RowHash>(start, limit);
        }

        std::pair<int, int> tryLookupRow(const RowPath & rowName) const
        {
            auto chunks = rowIndex.pathPossibleChunks(rowName);
            for (auto & c: chunks) {
                int chunkNumber = c.first;
                int indexInChunk = c.second;

                Path storage;
                if (indexInChunk < this->chunks[chunkNumber]->rowCount()
                    && this->chunks[chunkNumber]->getRowPath(indexInChunk, storage)
                    == rowName)
                    return {chunkNumber, indexInChunk};
            }
            return {-1,-1};
        }
    
        std::pair<int, int> lookupRow(const RowPath & rowName) const
        {
            auto result = tryLookupRow(rowName);
            if (result.first == -1)
                throw HttpReturnException
                    (400, "Row not found in tabular dataset: "
                     + rowName.toUtf8String(),
                     "rowName", rowName);
            return result;
        }

        std::pair<int, int> lookupRow(const RowHash & rowHash) const
        {
            auto chunks = rowIndex.pathPossibleChunks(rowHash);
            for (auto & c: chunks) {
                int chunkNumber = c.first;
                int indexInChunk = c.second;

                Path storage;
                if (indexInChunk < this->chunks[chunkNumber]->rowCount()
                    && this->chunks[chunkNumber]
                    ->getRowPath(indexInChunk, storage).hash()
                    == rowHash.hash())
                    return {chunkNumber, indexInChunk};
            }
            throw HttpReturnException
                (400, "Row not found in tabular dataset");
        }

        virtual bool knownRow(const RowPath & rowName) const override
        {
            int chunkIndex;
            int rowIndex;

            std::tie(chunkIndex, rowIndex) = tryLookupRow(rowName);
            return chunkIndex >= 0;
        }

        virtual MatrixNamedRow getRow(const RowPath & rowName) const override
        {
            MatrixNamedRow result;
            result.rowHash = rowName;
            result.rowName = rowName;

            int chunkNumber;
            int rowInChunk;
            std::tie(chunkNumber, rowInChunk)
                = lookupRow(rowName);

            result.columns
                = chunks.at(chunkNumber)
                ->getRow(rowInChunk, owner->fixedColumns);
            return result;
        }

        virtual ExpressionValue getRowExpr(const RowPath & rowName) const
        {
            int chunkNumber;
            int rowInChunk;
            std::tie(chunkNumber, rowInChunk)
                = lookupRow(rowName);

            return chunks.at(chunkNumber)
                ->getRowExpr(rowInChunk, owner->fixedColumns);
        }

        virtual RowPath getRowPath(const RowHash & rowHash) const override
        {
            int chunkNumber;
            int rowInChunk;
            std::tie(chunkNumber, rowInChunk)
                = lookupRow(rowHash);
            return chunks.at(chunkNumber)
                ->getRowPath(rowInChunk);
        }

        virtual ColumnPath getColumnPath(ColumnHash column) const override
        {
            auto it = columnHashIndex.find(column);
            if (it == columnHashIndex.end())
                throw HttpReturnException
                    (400, "Tabular dataset contains no column with given hash",
                     "columnHash", column,
                     "knownColumns", getColumnPaths(0, -1));
            return columns[it->second].columnName;
        }

        virtual const ColumnStats &
        getColumnStats(const ColumnPath & column, ColumnStats & stats) const override
        {
            // WARNING: we don't calculate the correct value here; we don't
            // correctly record the row counts.  We should probably remove it
            // from the interface, since it's hard for any dataset to get it
            // right.
            auto it = columnIndex.find(column.oldHash());
            if (it == columnIndex.end()) {
                throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                          "columnPath", column,
                                          "knownColumns", getColumnPaths(0, -1));
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

        virtual size_t getRowCount() const override
        {
            return rowCount;
        }

        virtual size_t getColumnCount() const override
        {
            return columns.size();
        }

        virtual std::pair<Date, Date> getTimestampRange() const
        {
            return { earliestTs, latestTs };
        }

        virtual GenerateRowsWhereFunction
        generateRowsWhere(const SqlBindingScope & context,
                          const Utf8String& alias,
                          const SqlExpression & where,
                          ssize_t offset,
                          ssize_t limit) const
        {
            GenerateRowsWhereFunction result;
            return result;
        }
    };

    /** A stream of row names used to incrementally query available rows
        without creating an entire list in memory.
    */
    struct TabularDataStoreRowStream : public RowStream {

        TabularDataStoreRowStream(std::shared_ptr<const CurrentState> state)
            : state(state)
        {
        }

        virtual std::shared_ptr<RowStream> clone() const override
        {
            return std::make_shared<TabularDataStoreRowStream>(state);
        }

        virtual void initAt(size_t start) override
        {
            size_t sum = 0;
            chunkiter = state->chunks.begin();
            while (chunkiter != state->chunks.end()
                   && start >= sum + (*chunkiter)->rowCount())  {
                sum += (*chunkiter)->rowCount();
                ++chunkiter;
            }

            if (chunkiter != state->chunks.end()) {
                rowIndex = (start - sum);
                rowCount = (*chunkiter)->rowCount();
            }
        }

        /// Parallelize chunk by chunk, which allows for natural
        /// boundaries.
        virtual std::vector<std::shared_ptr<RowStream> >
        parallelize(int64_t rowStreamTotalRows,
                    ssize_t approxNumberOfChildStreams,
                    std::vector<size_t> * streamOffsets) const override
        {
            // Always do the number of chunks
            std::vector<std::shared_ptr<RowStream> > streams;
            if (streamOffsets)
                streamOffsets->clear();

            ssize_t startAt = 0;
            for (auto it = state->chunks.begin();  it != state->chunks.end();
                 ++it) {
                if (streamOffsets)
                    streamOffsets->push_back(startAt);
                startAt += (*it)->rowCount();

                auto stream = std::make_shared<TabularDataStoreRowStream>(state);
                stream->chunkiter = it;
                stream->rowIndex = 0;
                stream->rowCount = (*it)->rowCount();

                streams.emplace_back(stream);
            }

            if (streamOffsets)
                streamOffsets->push_back(startAt);

            ExcAssertEqual(startAt, rowStreamTotalRows);

            return streams;
        }

        virtual bool supportsExtendedInterface() const override
        {
            return true;
        }

        virtual const RowPath & rowName(RowPath & storage) const override
        {
            return (*chunkiter)->getRowPath(rowIndex, storage);
        }

        virtual RowPath next() override
        {
            RowPath storage;
            const RowPath & row = rowName(storage);
            advance();
            if (&storage == &row)
                return storage;
            else return row;
        }

        virtual void advance() override
        {
            ExcAssert(rowIndex < rowCount);
            rowIndex++;
            if (rowIndex == rowCount) {
                ++chunkiter;
                if (chunkiter != state->chunks.end()) {
                    rowIndex = 0;
                    rowCount = (*chunkiter)->rowCount();
                    ExcAssertGreater(rowCount, 0);
                }
            }
        }

        static double extractVal(const CellValue & val, double *)
        {
            return val.toDouble();
        }

        static CellValue extractVal(CellValue val, CellValue *)
        {
            return val;
        }

        template<typename T>
        void extractT(size_t numValues,
                      const std::vector<ColumnPath> & columnNames,
                      T * output)
        {
            // 1.  Index each of the columns
            size_t n = 0;
            std::vector<int> columnIndexes;
            columnIndexes.reserve(columnNames.size());
            for (auto & c: columnNames) {
                auto it = state->columnIndex.find(c.oldHash());
                if (it == state->columnIndex.end()) {
                    columnIndexes.emplace_back(-1);
                }
                else {
                    columnIndexes.emplace_back(it->second);
                }
            }

            // 2.  Go through chunk by chunk
            while (n < numValues) {
                // 1.  Find the columns for the current chunk
                std::vector<const FrozenColumn *> columns;
                columns.reserve(columnNames.size());
                for (size_t i = 0;  i < columnNames.size();  ++i) {
                    columns.push_back
                        ((*chunkiter)->maybeGetColumn(columnIndexes[i],
                                                      columnNames[i]));
                    if (!columns.back())
                        throw HttpReturnException
                            (400,
                             "Couldn't find column "
                             + columnNames[i].toUtf8String());
                }

                // 2.  Go through the rows and get the values
                for (; rowIndex < rowCount && n < numValues;) {
                    for (size_t i = 0;  i < columnNames.size();  ++i) {
                        output[n * columnNames.size() + i]
                            = extractVal(columns[i]->get(rowIndex), (T *)0);
                    }
                    
                    ++n;

                    if (rowIndex == rowCount - 1) {
                        advance();
                        break;  // new chunk, so new columns
                    }
                    advance();
                }
            }
        }

        virtual void
        extractNumbers(size_t numValues,
                       const std::vector<ColumnPath> & columnNames,
                       double * output) override
        {
            return extractT<double>(numValues, columnNames, output);
        }

        virtual void
        extractColumns(size_t numValues,
                       const std::vector<ColumnPath> & columnNames,
                       CellValue * output) override
        {
            return extractT<CellValue>(numValues, columnNames, output);
        }

        std::shared_ptr<const CurrentState> state;
        std::vector<std::shared_ptr<const TabularDatasetChunk> >
            ::const_iterator chunkiter;
        size_t rowIndex;   ///< Number of row within this chunk
        size_t rowCount;   ///< Total number of rows within this chunk
    };

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

    /// Holds the current state of everything.  Writing is protected
    /// by the datasetMutex; reading is unprotected as it's constant
    atomic_shared_ptr<const CurrentState> currentState;

    /// Everything below here is protected by the dataset lock
    /// Mutex to make changes in the current state
    std::mutex datasetMutex;

    /// List of the names of the fixed columns in the dataset.  This is
    /// set on the first time a row is added or in initialization and is
    /// immutable afterwards.
    std::vector<ColumnPath> fixedColumns;

    /// Index of just the fixed columns, to look them up by hash
    Lightweight_Hash<uint64_t, int> fixedColumnIndex;

    /// All chunks we've added but haven't yet committed
    std::vector<std::shared_ptr<TabularDatasetChunk> > frozenChunks;

    /// Configuration passed in.  Constant after initialization.
    TabularDatasetConfig config;

    /// The number of background jobs that we're currently waiting for
    std::atomic<size_t> backgroundJobsActive;

    /// Logger instance for this class
    shared_ptr<spdlog::logger> logger;

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnPath & column) const override
    {
        return currentState.load()->getColumn(column);
    }

    virtual std::vector<CellValue>
    getColumnDense(const ColumnPath & column) const override
    {
        return currentState.load()->getColumnDense(column);
    }

    virtual std::tuple<BucketList, BucketDescriptions>
    getColumnBuckets(const ColumnPath & column,
                     int maxNumBuckets) const override
    {
        return currentState.load()->getColumnBuckets(column, maxNumBuckets);
    }

    virtual uint64_t
    getColumnRowCount(const ColumnPath & column) const override
    {
        return currentState.load()->getColumnRowCount(column);
    }

    virtual bool knownColumn(const ColumnPath & column) const override
    {
        return currentState.load()->knownColumn(column);
    }

    virtual std::vector<ColumnPath>
    getColumnPaths(ssize_t offset, ssize_t limit) const override
    {
        return currentState.load()->getColumnPaths(offset, limit);
    }

    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const
    {
        return currentState.load()->getKnownColumnInfo(columnName);
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const override
    {
        return currentState.load()->getRowPaths(start, limit);
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const override
    {
        return currentState.load()->getRowHashes(start, limit);
    }

    virtual bool knownRow(const RowPath & rowName) const override
    {
        return currentState.load()->knownRow(rowName);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const override
    {
        return currentState.load()->getRow(rowName);
    }

    virtual ExpressionValue getRowExpr(const RowPath & rowName) const
    {
        return currentState.load()->getRowExpr(rowName);
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const override
    {
        return currentState.load()->getRowPath(rowHash);
    }

    virtual ColumnPath getColumnPath(ColumnHash column) const override
    {
        return currentState.load()->getColumnPath(column);
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnPath & column, ColumnStats & stats) const override
    {
        return currentState.load()->getColumnStats(column, stats);
    }

    virtual size_t getRowCount() const override
    {
        return currentState.load()->getRowCount();
    }

    virtual size_t getColumnCount() const override
    {
        return currentState.load()->getColumnCount();
    }

    virtual std::pair<Date, Date> getTimestampRange() const
    {
        return currentState.load()->getTimestampRange();
    }

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const Utf8String & alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const
    {
        return currentState.load()
            ->generateRowsWhere(context, alias, where, offset, limit);
    }

    /// Create a new current state from the old one plus the extra
    /// chunks.
    std::shared_ptr<CurrentState>
    finalize(std::shared_ptr<const CurrentState> oldState,
             std::vector<std::shared_ptr<TabularDatasetChunk> > & inputChunks)
    {
        // NOTE: must be called with the lock held

        size_t totalRows = oldState->rowCount;

        cerr << "commiting " << frozenChunks.size() << " frozen chunks"
             << endl;

        for (auto & c: inputChunks) {
            totalRows += c->rowCount();
        }

        auto newState = std::make_shared<CurrentState>(*oldState);

        newState->rowCount = totalRows;
        
        size_t numChunksBefore = newState->chunks.size();
        
        newState->chunks.reserve(numChunksBefore + inputChunks.size());

        for (auto & c: inputChunks) {
            newState->chunks.emplace_back(std::move(c));
        }

        // Make sure they aren't reused
        inputChunks.clear();

        // This will only happen when this is the first commit
        if (newState->columns.empty()) {
            newState->columns.reserve(fixedColumns.size());
            for (size_t i = 0;  i < fixedColumns.size();  ++i) {
                const ColumnPath & c = fixedColumns[i];
                ColumnEntry entry;
                entry.columnName = c;
                newState->columns.emplace_back(entry);
                newState->columnIndex[c.oldHash()] = i;
                newState->columnHashIndex[c] = i;
            }
        }

        // Create the column index.  This should be rapid, as there shouldn't
        // be too many columns.
        for (size_t i = numChunksBefore;  i < newState->chunks.size();  ++i) {
            const TabularDatasetChunk & chunk = *newState->chunks[i];
            ExcAssertEqual(fixedColumns.size(), chunk.fixedColumnCount());
            for (size_t j = 0;  j < chunk.columns.size();  ++j) {
                newState->columns[j].chunks.emplace_back(i, chunk.columns[j]);
                newState->columns[j].nonNullRowCount
                    += chunk.columns[j]->nonNullRowCount();
            }
            for (auto & c: chunk.sparseColumns) {
                auto it = newState->columnIndex
                    .insert(make_pair(c.first.oldHash(),
                                      newState->columns.size()))
                    .first;
                if (it->second == newState->columns.size()) {
                    ColumnEntry entry;
                    entry.columnName = c.first;
                    newState->columns.emplace_back(entry);
                    newState->columnHashIndex[c.first] = it->second;
                }
                newState->columns[it->second].chunks.emplace_back(i, c.second);
                newState->columns[it->second].nonNullRowCount
                    += c.second->nonNullRowCount();
            }
        }
        
        ExcAssertEqual(newState->columns.size(), newState->columnIndex.size());
        ExcAssertEqual(newState->columns.size(),
                       newState->columnHashIndex.size());

        
        // Recreate the index if a new chunk has been added
        if (numChunksBefore != newState->chunks.size()) {
            MutablePathIndex index;

            // We create the row index in multiple chunks

            Timer rowIndexTimer;

            auto indexChunk = [&] (int chunkNum)
                {
                    auto recorder = index.getRecorder(chunkNum);
                    for (unsigned j = 0;
                         j < newState->chunks[chunkNum]->rowCount();
                         ++j) {
                        RowPath rowNameStorage;
                        const RowPath & rowName
                            = newState->chunks[chunkNum]
                            ->getRowPath(j, rowNameStorage);
                        recorder.record(rowName, j);
                    }
                
                    recorder.commit();
                };
        
            // NOTE: we currently re-index everything from the
            // previous chunks; this is a big scalability problem.
            // We should not do that once multiple commits become
            // an important use case
            parallelMap(0, newState->chunks.size(), indexChunk);

            std::vector<std::tuple<int, int, int, int> > possibleCollisions;
            std::tie(newState->rowIndex, possibleCollisions)
                = index.freeze(serializer);

            cerr << possibleCollisions.size() << " possible collisions"
                 << endl;

            std::set<Path> duplicateRowNames;
            bool extraDuplicates = false;
            
            for (auto & c: possibleCollisions) {
                Path name1 = newState->chunks[std::get<0>(c)]->getRowPath(std::get<1>(c));
                Path name2 = newState->chunks[std::get<2>(c)]->getRowPath(std::get<3>(c));
                if (name1 == name2) {
                    duplicateRowNames.emplace(std::move(name1));
                }
                if (duplicateRowNames.size() > 1000) {
                    extraDuplicates = true;
                    break;
                }
            }

            if (!duplicateRowNames.empty()) {
                Utf8String duplicateNames;
                for (auto & n: duplicateRowNames) {
                    if (!duplicateNames.empty())
                        duplicateNames += ", ";
                    duplicateNames += n.toUtf8String();
                    if (duplicateNames.length() > 100) {
                        extraDuplicates = true;
                        break;
                    }
                }
                if (extraDuplicates)
                    duplicateNames += "...";
                throw HttpReturnException
                    (400, "Duplicate row name(s) in tabular dataset: "
                     + duplicateNames,
                     "duplicates", duplicateRowNames);
            }
            
            cerr << "rowIndex.memusage() = " << newState->rowIndex.memusage()
                 << endl;
            INFO_MSG(logger) << "row index took " << rowIndexTimer.elapsed();
        }

        return newState;
    }

    void initialize(vector<ColumnPath> columnNames)
    {
        ExcAssert(this->fixedColumns.empty());
        this->fixedColumns = std::move(columnNames);

        for (size_t i = 0;  i < fixedColumns.size();  ++i) {
            if (!fixedColumnIndex.insert(make_pair(fixedColumns[i].oldHash(), i))
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
            : Recorder(store->server),
              store(store)
        {
        }

        TabularDataStore * store;

        virtual void
        recordRowExpr(const RowPath & rowName,
                      const ExpressionValue & expr) override
        {
            RowValue row;
            expr.appendToRow(ColumnPath(), row);
            recordRowDestructive(rowName, std::move(row));
        }

        virtual void
        recordRowExprDestructive(RowPath rowName,
                                 ExpressionValue expr) override
        {
            RowValue row;
            ColumnPath columnName;
            expr.appendToRowDestructive(columnName, row);
            recordRowDestructive(std::move(rowName), std::move(row));
        }

        virtual void
        recordRow(const RowPath & rowName,
                  const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override
        {
            store->recordRow(rowName, vals);
        }

        virtual void
        recordRowDestructive(RowPath rowName,
                             std::vector<std::tuple<ColumnPath, CellValue, Date> > vals) override
        {
            store->recordRow(std::move(rowName), std::move(vals));
        }

        virtual void
        recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) override
        {
            for (auto & r: rows)
                store->recordRow(r.first, r.second);
        }

        virtual void
        recordRowsDestructive(std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows) override
        {
            for (auto & r: rows)
                store->recordRow(std::move(r.first), std::move(r.second));
        }

        virtual void
        recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue > > & rows) override
        {
            for (auto & r: rows) {
                recordRowExpr(r.first, r.second);
            }
        }

        virtual void
        recordRowsExprDestructive(std::vector<std::pair<RowPath, ExpressionValue > > rows) override
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
            : Recorder(store->server),
              store(store), doneFirst(store->mutableChunks.load())
        {
            // Note that this may return a null pointer, if nothing has
            // been loaded yet.
            chunk = store->createNewChunk();
        }

        TabularDataStore * store;
        bool doneFirst;

        std::shared_ptr<MutableTabularDatasetChunk> chunk;

        virtual void
        recordRowExpr(const RowPath & rowName,
                      const ExpressionValue & expr) override
        {
            RowValue row;
            expr.appendToRow(ColumnPath(), row);
            recordRowDestructive(rowName, std::move(row));
        }

        virtual void
        recordRowExprDestructive(RowPath rowName,
                                 ExpressionValue expr) override
        {
            RowValue row;
            ColumnPath columnName;
            expr.appendToRowDestructive(columnName, row);
            recordRowDestructive(std::move(rowName), std::move(row));
        }

        virtual void
        recordRow(const RowPath & rowName,
                  const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override
        {
            recordRowImpl(rowName, vals);
        }

        virtual void
        recordRowDestructive(RowPath rowName,
                             std::vector<std::tuple<ColumnPath, CellValue, Date> > vals) override
        {
            recordRowImpl(std::move(rowName), std::move(vals));
        }

        template<typename Vals>
        void recordRowImpl(RowPath rowName, Vals&& vals)
        {
            if (!chunk) {
                {
                    std::unique_lock<std::mutex> guard(store->datasetMutex);
                    store->createFirstChunks(vals);
                }

                chunk = store->createNewChunk();
            }
            ExcAssert(chunk);


            // Prepare what we need to record
            auto rowVals = store->prepareRow(vals);

            std::vector<CellValue> & orderedVals = std::get<0>(rowVals);
            std::vector<std::pair<ColumnPath, CellValue> > & newColumns
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
                chunk.reset
                    (new MutableTabularDatasetChunk
                     (orderedVals.size(),
                      chunkSizeForNumColumns(orderedVals.size())));
            }
        }

        virtual void
        recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) override
        {
            for (auto & r: rows)
                recordRow(r.first, r.second);
        }

        virtual void
        recordRowsDestructive(std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows) override
        {
            for (auto & r: rows)
                recordRowDestructive(std::move(r.first), std::move(r.second));
        }

        virtual void
        recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue > > & rows) override
        {
            for (auto & r: rows) {
                recordRowExpr(r.first, r.second);
            }
        }

        virtual void
        recordRowsExprDestructive(std::vector<std::pair<RowPath, ExpressionValue > > rows) override
        {
            for (auto & r: rows) {
                recordRowExprDestructive(std::move(r.first), std::move(r.second));
            }
        }

        virtual void finishedChunk() override
        {
            if (!chunk || chunk->rowCount() == 0)
                return;
            ColumnFreezeParameters params;
            auto frozen = chunk->freeze(params);
            store->addFrozenChunk(std::move(frozen));
        }

        virtual
        std::function<void (RowPath rowName,
                            Date timestamp,
                            CellValue * vals,
                            size_t numVals,
                            std::vector<std::pair<ColumnPath, CellValue> > extra)>
        specializeRecordTabular(const std::vector<ColumnPath> & columnNames) override
        {
            /* We return a function that knows it will always receive the same
               set of columns.  This allows us to directly record them without
               needing to do any manipulation of column names at all.
            */

            return [=] (RowPath rowName, Date timestamp,
                        CellValue * vals, size_t numVals,
                        std::vector<std::pair<ColumnPath, CellValue> > extra)
                {
                    if (!chunk) {
                        {
                            std::unique_lock<std::mutex> guard(store->datasetMutex);

                            // We create a sample set of values for the
                            // column to analyze, so it can identify the
                            // column names.
                            std::vector<std::tuple<ColumnPath, CellValue, Date> > sampleVals;
                            for (unsigned i = 0;  i < columnNames.size();  ++i)
                                sampleVals.emplace_back(columnNames[i], vals[i], timestamp);
                   
                            store->createFirstChunks(sampleVals);
                        }

                        chunk = store->createNewChunk();
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
                        chunk.reset
                            (new MutableTabularDatasetChunk
                             (columnNames.size(),
                              chunkSizeForNumColumns(columnNames.size())));
                    }
                };
        }
    };

    void commit()
    {
        // Create new chunks to hold any new data that comes in
        auto newChunks = std::make_shared<ChunkList>(NUM_PARALLEL_CHUNKS);

        for (auto & c: *newChunks) {
            auto newChunk = std::make_shared<MutableTabularDatasetChunk>
                (fixedColumns.size(), chunkSizeForNumColumns(fixedColumns.size()));
            c.store(std::move(newChunk));
        }
            
        // Atomically swap out the old pointer.  New records go in the new chunks.
        auto oldMutableChunks = mutableChunks.exchange(std::move(newChunks));

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

        // Now we have a lock, we now that nobody else can come along and
        // modify anything.  First, grab the current state.  Others may
        // still access this state to read while we're working, but it
        // will not change as we have the lock.
        auto oldState = currentState.load();

        // At this point, nobody can see oldMutableChunks or its contents
        // apart from this thread.  So we can perform operations unlocked
        // on it without any problem.

        auto newState = finalize(oldState, frozenChunks);

        currentState.store(newState);
        
        uint64_t totalRows = newState->rowCount;
        size_t mem = 0;
        size_t rowNameMem = 0, timestampMem = 0;
        for (auto & c: newState->chunks) {
            mem += c->memusage();
            rowNameMem += c->rowNames->memusage();
            timestampMem += c->timestamps->memusage();
        }

        if (!newState->chunks.empty()) {
            INFO_MSG(logger) << "row name usage is " << rowNameMem
                             << " bytes at "
                             << 1.0 * rowNameMem / totalRows << " per row with "
                             << ML::type_name(*newState->chunks[0]->rowNames);
            INFO_MSG(logger) << "timestamp usage is " << timestampMem
                             << " bytes at "
                             << 1.0 * timestampMem / totalRows << " per row with "
                             << ML::type_name(*newState->chunks[0]->timestamps);
        }

        size_t columnMem = 0;
        for (auto & c: newState->columns) {
            size_t bytesUsed = 0;
            for (auto & chunk: c.chunks) {
                bytesUsed += chunk.second->memusage();
            }
            INFO_MSG(logger)
                << "column " << c.columnName << " used "
                << bytesUsed << " bytes at "
                << 1.0 * bytesUsed / totalRows << " per row with "
                << ML::type_name(*c.chunks[0].second);
            columnMem += bytesUsed;
        }

        size_t rowIndexMem = newState->rowIndex.memusage();
        mem += rowIndexMem;

        INFO_MSG(logger) << "row index usage is " << rowIndexMem
                         << " bytes at "
                         << 1.0 * rowIndexMem / totalRows << " per row";

        INFO_MSG(logger) << "total mem usage is " << mem << " bytes" << " for "
                         << totalRows << " rows and "
                         << newState->columns.size()
                         << " columns for "
                         << 1.0 * mem / totalRows << " bytes/row";
        INFO_MSG(logger) << "column memory is " << columnMem;

    }

    // freezes a new chunk in the background, and adds it to frozenChunks.
    // Updates the number of background jobs atomically so that we can know
    // when everything is finished.
    void freezeChunkInBackground(std::shared_ptr<MutableTabularDatasetChunk> chunk)
    {
        if (chunk->rowCount() == 0)
            return;

        ColumnFreezeParameters params;
        auto job = [=] ()
            {
                Scope_Exit(--this->backgroundJobsActive);
                auto frozen = chunk->freeze(params);
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
        frozenChunks.emplace_back
            (new TabularDatasetChunk(std::move(frozen)));
    }
    
    std::shared_ptr<MutableTabularDatasetChunk>
    createNewChunk(ssize_t expectedSize = -1)
    {
        // Have we initialized things yet?
        bool mc = mutableChunks.load() != nullptr;
        if (!mc)
            return nullptr;

        return std::make_shared<MutableTabularDatasetChunk>
            (fixedColumns.size(),
             expectedSize == -1
             ? chunkSizeForNumColumns(fixedColumns.size())
             : expectedSize);
    }

    /** Analyze the first row to know what the columns are. */
    void createFirstChunks(const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        // Must be done with the dataset lock held
        if (!mutableChunks.load()) {
            //need to create the mutable chunk
            vector<ColumnPath> columnNames;

            //The first recorded row will determine the columns
            Lightweight_Hash<uint64_t, int> inputColumnIndex;
            for (unsigned i = 0;  i < vals.size();  ++i) {
                const ColumnPath & c = std::get<0>(vals[i]);
                uint64_t ch(c.oldHash());
                if (!inputColumnIndex.insert(make_pair(ch, i)).second)
                    throw HttpReturnException(400, "Duplicate column name in tabular dataset entry",
                                              "columnName", c.toUtf8String());
                columnNames.push_back(c);
            }

            initialize(std::move(columnNames));

            auto newChunks = std::make_shared<ChunkList>(NUM_PARALLEL_CHUNKS);

            for (auto & c: *newChunks) {
                auto newChunk = std::make_shared<MutableTabularDatasetChunk>
                    (fixedColumns.size(), chunkSizeForNumColumns(fixedColumns.size()));
                c.store(std::move(newChunk));
            }
            
            auto old = mutableChunks.exchange(std::move(newChunks));
            ExcAssert(!old);
        }
    }

    // Vals is std::vector<std::tuple<ColumnPath, CellValue, Date> >
    // either a const reference (in which case we copy), or a
    // rvalue or non-const reference (in which case we move)
    template<typename Vals>
    std::tuple<std::vector<CellValue>,
               std::vector<std::pair<ColumnPath, CellValue> >,
               Date>
    prepareRow(Vals&& vals)
    {
        std::vector<CellValue> orderedVals(fixedColumns.size());
        Date ts = Date::negativeInfinity();

        std::vector<std::pair<ColumnPath, CellValue> > newColumns;

        for (unsigned i = 0;  i < vals.size();  ++i) {
            const ColumnPath & c = std::get<0>(vals[i]);
            auto iter = fixedColumnIndex.find(c.oldHash());
            if (iter == fixedColumnIndex.end()) {
                switch (config.unknownColumns) {
                case UC_ERROR:
                    throw HttpReturnException
                        (400,
                         "New column name while recording row in tabular dataset "
                         "with unknownColumns=ERROR",
                         "columnName", c.toUtf8String(),
                         "knownColumns", fixedColumns);
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

    // Vals is std::vector<std::tuple<ColumnPath, CellValue, Date> >
    // Same const/non-const as is happening above
    template<typename Vals>
    void recordRow(RowPath rowName,
                   Vals&& vals)
    {
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
        std::vector<std::pair<ColumnPath, CellValue> > & newColumns
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
                    (fixedColumns.size(), chunkSizeForNumColumns(fixedColumns.size()));
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
               const ProgressFunc & onProgress)
    : Dataset(owner)
{
    itl = make_shared<TabularDataStore>
        (owner,
         config.params.convert<TabularDatasetConfig>(),
         MLDB::getMldbLog<TabularDataset>());
}

TabularDataset::
~TabularDataset()
{
}

Any
TabularDataset::
getStatus() const
{
    return itl->currentState.load()->getStatus();
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
        (itl->currentState.load()); 
} 

ExpressionValue
TabularDataset::
getRowExpr(const RowPath & row) const
{
    return itl->getRowExpr(row);
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
        = itl->generateRowsWhere(context, alias, where, offset, limit);
    if (!fn)
        fn = Dataset::generateRowsWhere(context, alias, where, offset, limit);
    return fn;
}

KnownColumn
TabularDataset::
getKnownColumnInfo(const ColumnPath & columnName) const
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
recordRowItl(const RowPath & rowName,
             const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
{
    validateNames(rowName, vals);
    itl->recordRow(rowName, vals);
}

void
TabularDataset::
recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows)
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

} // file scope

} // MLDB
