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
#include "mldb/utils/smart_ptr_utils.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/scope.h"
#include "mldb/core/bucket.h"
#include "mldb/base/parallel_merge_sort.h"
#include "mldb/base/map_reduce.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/types/set_description.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/utils/atomic_shared_ptr.h"
#include "mldb/utils/floating_point.h"
#include "mldb/utils/log.h"
#include "mldb/engine/dataset_utils.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/engine/dataset_utils.h"
#include "mldb/types/db/persistent.h"
#include "mldb/block/zip_serializer.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
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

        // Returns number of rows indexed
        size_t commit()
        {
            size_t result = 0;

            for (size_t i = 0;  i < INDEX_SHARDS;  ++i) {
                size_t shard = (i + chunkNumber) % INDEX_SHARDS;
                std::unique_lock<std::mutex> guard(owner->indexLock[shard]);

                // NOTE: shard == 0, not i == 0!
                if (shard == 0) {
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

                result += toInsert[shard].size();
            }

            return result;
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

    // These are protected by indexLock[0]
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

        chunkBits = MLDB::highest_bit(numChunks, -1) + 1;
        offsetBits = MLDB::highest_bit(maxChunkSize - 1, -1) + 1;

        //ExcAssertGreater(chunkBits, 0);
        //ExcAssertGreater(offsetBits, 0);

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
        factor = 2.0 * input.size() / (double)std::numeric_limits<uint64_t>::max();

        auto setEntry = [&] (size_t bucket, int chunkNumber, int indexInChunk)
            {
                MLDB::Bit_Writer<uint32_t> writer(storage.data());
                writer.skip((chunkBits + offsetBits) * bucket);
                writer.write(chunkNumber + 1, chunkBits);
                writer.write(indexInChunk, offsetBits);
            };

        auto entryIsOccupied = [&] (size_t bucket) -> bool
            {
                MLDB::Bit_Extractor<uint32_t> bits(storage.data());
                bits.advance((chunkBits + offsetBits) * bucket);
                uint32_t chunk = bits.extract<uint32_t>(chunkBits);
                return chunk > 0;
            };
     
        int collisions [[maybe_unused]] = 0;  // unused for debug
        size_t maxOffset [[maybe_unused]]= 0;  // unused for debug
        size_t totalOffset [[maybe_unused]]= 0;
        
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
                        throw AnnotatedException(500, "Hash bucket error");
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
        size_t bucket = getBucket(hash);
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
        MLDB::Bit_Extractor<uint32_t> bits(storage.data());
        bits.advance((chunkBits + offsetBits) * bucket);
        uint32_t chunk = bits.extract<uint32_t>(chunkBits) - 1;
        uint32_t offset = bits.extract<uint32_t>(offsetBits);
        return {chunk, offset};
    }

    bool entryIsOccupied(size_t bucket) const
    {
        MLDB::Bit_Extractor<uint32_t> bits(storage.data());
        bits.advance((chunkBits + offsetBits) * bucket);
        uint32_t chunk = bits.extract<uint32_t>(chunkBits);
        return chunk > 0;
    }

    void serialize(StructuredSerializer & serializer) const
    {
        serializer.newObject<PathIndexMetadata>("md", *this);
        serializer.addRegion(storage, "ri");
    }

    void reconstitute(StructuredReconstituter & reconstituter)
    {
        reconstituter.getObject<PathIndexMetadata>("md", *this);
        storage = reconstituter.getRegionT<uint32_t>("ri");
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

    void serialize(StructuredSerializer & serializer) const
    {
        for (size_t i = 0;  i < INDEX_SHARDS;  ++i) {
            shards[i].serialize(*serializer.newStructure(i));
        }
    }

    void reconstitute(StructuredReconstituter & reconstituter)
    {
        // TODO: make it possible to reconstitute a different number of
        // shards.
        ExcAssertEqual(reconstituter.getDirectory().size(), INDEX_SHARDS);
        for (size_t i = 0;  i < INDEX_SHARDS;  ++i) {
            shards[i].reconstitute(*reconstituter.getStructure(i));
        }
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
    //cerr << "freeze: maxChunkNumber " << maxChunkNumber << " maxChunkIndex " << maxChunkIndex << endl;

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
/* TABULAR DATA STORE METADATA                                               */
/*****************************************************************************/

struct TabularDataStoreMetadata {
    std::vector<ColumnPath> columns;
    Date earliestTs = Date::notADate();
    Date latestTs = Date::notADate();
    uint32_t numFixedColumns = 0;
};

IMPLEMENT_STRUCTURE_DESCRIPTION(TabularDataStoreMetadata)
{
    setVersion(1);
    addField("columns", &TabularDataStoreMetadata::columns, "");
    addField("earliestTs", &TabularDataStoreMetadata::earliestTs, "");
    addField("latestTs", &TabularDataStoreMetadata::latestTs, "");
    addField("numFixedColumns", &TabularDataStoreMetadata::numFixedColumns, "");
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

    TabularDataStore(MldbEngine * engine,
                     TabularDatasetConfig config,
                     shared_ptr<spdlog::logger> logger)
        : engine(engine),
          serializer(new MemorySerializer),
          currentState(std::make_shared<CurrentState>(this, logger)),
          config(std::move(config)),
          backgroundJobsActive(0), logger(std::move(logger))
    {
        ExcAssert(this->logger);
        initRoutes();

        if (!config.dataFileUrl.empty()) {
            load(config.dataFileUrl);
        }
    }

    MldbEngine * engine = nullptr;

    /// This is used to allocate mapped memory when chunks are frozen
    std::unique_ptr<MappedSerializer> serializer;

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
            : Dataset(owner->engine),
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
        LightweightHash<uint64_t, int> columnIndex;

        /// Same index, but using the old (slow) hash.  Useful only for when
        /// we are forced to lookup on ColumnHash.
        LightweightHash<ColumnHash, int> columnHashIndex;

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
                throw AnnotatedException(400, "Tabular dataset contains no column with given hash",
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
                throw AnnotatedException(400, "Tabular dataset contains no column with given name",
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
                throw AnnotatedException(400, "Tabular dataset contains no column with given name",
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
                            throw AnnotatedException
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

            auto sortedNumerics = parallelMergeSortUnique(numerics, MLDB::safe_less<double>());
            auto sortedStrings = parallelMergeSortUnique(strings);

            BucketDescriptions desc;
            desc.initialize(hasNulls,
                            std::move(sortedNumerics),
                            std::move(sortedStrings),
                            maxNumBuckets);

            //cerr << "Tabular getColumnBuckets " << column << " has " << desc.numBuckets() << " buckets " << endl;

            // In parallel, create a bucket list for each chunk, then
            // add them together in order.

            ParallelWritableBucketList buckets(totalRows, desc.numBuckets(),
                                               *owner->serializer);

            size_t numWritten = 0;

            // This will be called chunk by chunk in order to add the
            // given chunk buckets to the current ones.  It's mostly
            // a memcpy apart from dealing with the boundary conditions.
            auto addBuckets = [&] (size_t chunkNum,
                                   WritableBucketList & chunkBuckets)
                {
                    numWritten += chunkBuckets.rowCount();
                    buckets.append(std::move(chunkBuckets));
                };
            
            auto onChunk2 = [&] (size_t i) -> WritableBucketList
                {
                    auto & column = *chunks[i]->columns[it->second];
                    
                    size_t chunkRows = column.size();
                    WritableBucketList result(chunkRows, desc.numBuckets(),
                                              *owner->serializer);
                    
                    auto onRow = [&] (size_t rowNum, const CellValue & val)
                    {
                        uint32_t bucket = desc.getBucket(val);
                        ExcAssertLess(bucket, desc.numBuckets());
                        result.write(bucket);
                        return true;
                    };
                
                    column.forEachDense(onRow);

                    return result;
                };

            parallelMapInOrderReduce(0, chunks.size(), onChunk2, addBuckets);
            

            if (numWritten != totalRows) {
                throw AnnotatedException
                    (500, "Column " + column.toUtf8String()
                     + " had wrong number written ("
                     + to_string(numWritten) + " vs " + to_string(totalRows)
                     + "); internal error (contact support with your script and "
                     + "dataset if possible");
            }

            ExcAssertEqual(numWritten, totalRows);

            //for (size_t i = 0;  i < totalRows;  ++i) {
            //    ExcAssertLess(buckets[i], desc.numBuckets());
            //}

            auto serializer = std::make_shared<MemorySerializer>();
            
            auto frozenBuckets = buckets.freeze(*serializer);
            frozenBuckets.backingStore = serializer;

            //for (size_t i = 0;  i < totalRows;  ++i) {
            //    ExcAssertLess(frozenBuckets[i], desc.numBuckets());
            //}

            return std::make_tuple(frozenBuckets, std::move(desc));
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
                throw AnnotatedException(400, "Tabular dataset contains no column with given hash",
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

        // ChunkNumber, IndexInChunk
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
#if 0
            cerr << "tryLookupRow " << rowName << ": possible chunks " << chunks << " of " << chunks.size() << endl;
            for (auto & c: chunks) {
                int chunkNumber = c.first;
                int indexInChunk = c.second;
                cerr << "  trying " << chunkNumber << " " << indexInChunk << ": ";
                if (indexInChunk >= this->chunks[chunkNumber]->rowCount()) {
                    cerr << "ERRRO: indexInChunk " << indexInChunk << " >= chunk rowCount " << this->chunks[chunkNumber]->rowCount() << endl;
                    continue;
                }

                Path storage;
                auto & pathName = this->chunks[chunkNumber]->getRowPath(indexInChunk, storage);
                cerr << pathName << " != " << rowName << endl;
            }
            cerr << "total rows " << rowCount << endl;
            for (size_t i = 0;  i < this->chunks.size();  ++i) {
                cerr << "chunk " << i << " has " << this->chunks[i]->rowCount() << " rows" << endl;
                for (size_t j = 0;  j < this->chunks[i]->rowCount();  ++j) {
                    Path storage;
                    const Path & rowPath = this->chunks[i]->getRowPath(j, storage);
                    if (rowPath == rowName) {
                        cerr << "*** found at index " << j << " in chunk " << i << endl;
                    }
                }
            }
#endif
            return {-1,-1};
        }
    
        std::pair<int, int> lookupRow(const RowPath & rowName) const
        {
            auto result = tryLookupRow(rowName);
            if (result.first == -1)
                throw AnnotatedException
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
            throw AnnotatedException
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
                throw AnnotatedException
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
                throw AnnotatedException(400, "Tabular dataset contains no column with given hash",
                                          "columnPath", column,
                                          "knownColumns", getColumnPaths(0, -1));
            }

            stats = ColumnStats();

            bool isNumeric = true;

            for (auto & c: columns.at(it->second).chunks) {

                auto onValue = [&] (const CellValue & value, size_t rowCount)
                    {
                        if (!value.isNumber())
                            isNumeric = false;
                        stats.values[value].rowCount_ += rowCount;
                        return true;
                    };
                                
                c.second->forEachDistinctValueWithRowCount(onValue);
            }

            stats.isNumeric_ = isNumeric && !chunks.empty();
            stats.rowCount_ = rowCount;
            return stats;
        }

        virtual uint64_t getRowCount() const override
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
            cerr << "Tabular generateRowsWhere: " << where.print() << " offset " << offset << " limit " << limit << endl;
            return Dataset::generateRowsWhere(context, alias, where, offset, limit);
            GenerateRowsWhereFunction result;
            return result;
        }

        void serialize(StructuredSerializer & serializer) const
        {
            // Chunks first.  This allows us to rewrite the indexes if
            // new chunks are added.

            {
                auto chunkSerializer
                    = serializer.newStructure("ch");

                for (size_t i = 0;  i < chunks.size();  ++i) {
                    chunks[i]->serialize
                        (*chunkSerializer->newStructure(to_string(i)));
                }
                chunkSerializer->commit();
            }

            // Now the things that change, such as column lists, indexes
            // etc

            rowIndex.serialize(*serializer.newStructure("ri"));

            TabularDataStoreMetadata md;
            md.columns.reserve(columns.size());
            for (auto & c: columns) {
                md.columns.push_back(c.columnName);
            }

            md.earliestTs = earliestTs;
            md.latestTs = latestTs;

            // If nothing was ever recorded in this chunk, then we never created our
            // columns.  So we say no fixed columns.
            md.numFixedColumns = columns.empty() ? 0 : owner->fixedColumns.size();
            
            serializer.newObject("md", md);
        }

        void reconstitute(StructuredReconstituter & reconstituter)
        {
            // Read the metadata ahead of everything else, so we can
            // handle the fixed columns
            TabularDataStoreMetadata md;
            reconstituter.getObject("md", md);

            this->earliestTs = md.earliestTs;
            this->latestTs = md.latestTs;

            cerr << jsonEncode(md) << endl;
            auto subitems = reconstituter.getDirectory();
            cerr << subitems.size() << " subitems" << endl;
            for (auto & item: subitems) {
                cerr << "  has item " << item.name << endl;
            }

            size_t numColumnsToIterate = md.numFixedColumns;

            if (!md.columns.empty()) {
                ExcAssertGreaterEqual(md.numFixedColumns, 0);
                ExcAssertLessEqual(md.numFixedColumns, md.columns.size());
            }
            else {
                numColumnsToIterate = 0;
            }

            // TODO: ugly reaching into the owner like this; refactor
            for (size_t i = 0;  i < numColumnsToIterate;  ++i) {
                owner->fixedColumns.emplace_back(md.columns[i]);
                owner->fixedColumnIndex[md.columns[i].oldHash()] = i;
            }
            
            std::vector<std::shared_ptr<TabularDatasetChunk> > newChunks;
            if (!md.columns.empty()) {
                auto chunkStructure = reconstituter.getStructure("ch");
                auto entries = chunkStructure->getDirectory();
                    
                newChunks.resize(entries.size());
            
                for (auto & e: entries) {
                    int chunkNumber = e.name.toIndex();
                    if (chunkNumber < 0 || chunkNumber >= newChunks.size()) {
                        throw AnnotatedException
                            (400, "Corrupt Tabular Dataset: chunk index out of range");
                    }
                    if (newChunks[chunkNumber]) {
                        throw AnnotatedException
                            (400, "Corrupt Tabular Dataset: duplicate chunk index");
                    }

                    newChunks[chunkNumber].reset(new TabularDatasetChunk(*e.getStructure()));
                }
            }

            // Add these chunks properly...
            addChunks(newChunks);

            // ... and map the row index in place
            rowIndex.reconstitute(*reconstituter.getStructure("ri"));
        }

        void addChunks(std::vector<std::shared_ptr<TabularDatasetChunk> > & inputChunks)
        {
            for (auto & c: inputChunks) {
                rowCount += c->rowCount();
            }

            size_t numChunksBefore = chunks.size();
        
            chunks.reserve(numChunksBefore + inputChunks.size());

            for (auto & c: inputChunks) {
                chunks.emplace_back(std::move(c));
            }

            // Make sure they aren't reused
            inputChunks.clear();

            // This will only happen when this is the first commit
            if (columns.empty()) {
                columns.reserve(owner->fixedColumns.size());
                for (size_t i = 0;  i < owner->fixedColumns.size();  ++i) {
                    const ColumnPath & c = owner->fixedColumns[i];
                    ColumnEntry entry;
                    entry.columnName = c;
                    columns.emplace_back(entry);
                    columnIndex[c.oldHash()] = i;
                    columnHashIndex[c] = i;
                }
            }

            // Create the column index.  This should be rapid, as there shouldn't
            // be too many columns.
            for (size_t i = numChunksBefore;  i < chunks.size();  ++i) {
                const TabularDatasetChunk & chunk = *chunks[i];
                ExcAssertEqual(owner->fixedColumns.size(),
                               chunk.fixedColumnCount());
                for (size_t j = 0;  j < chunk.columns.size();  ++j) {
                    columns[j].chunks.emplace_back(i, chunk.columns[j]);
                    columns[j].nonNullRowCount
                        += chunk.columns[j]->nonNullRowCount();
                }
                for (auto & c: chunk.sparseColumns) {
                    auto it = columnIndex
                        .insert(make_pair(c.first.oldHash(),
                                          columns.size()))
                        .first;
                    if (it->second == columns.size()) {
                        ColumnEntry entry;
                        entry.columnName = c.first;
                        columns.emplace_back(entry);
                        columnHashIndex[c.first] = it->second;
                    }
                    columns[it->second].chunks.emplace_back(i, c.second);
                    columns[it->second].nonNullRowCount
                        += c.second->nonNullRowCount();
                }
            }
        
            ExcAssertEqual(columns.size(), columnIndex.size());
            ExcAssertEqual(columns.size(),
                           columnHashIndex.size());
        }

        void reIndex()
        {
            cerr << "indexing " << chunks.size() << " chunks" << endl;

            MutablePathIndex index;

            // We create the row index in multiple chunks

            Timer rowIndexTimer;

            auto indexChunk = [&] (int chunkNum)
                {
                    auto recorder = index.getRecorder(chunkNum);
                    for (unsigned j = 0;
                         j < chunks[chunkNum]->rowCount();
                         ++j) {
                        RowPath rowNameStorage;
                        const RowPath & rowName
                            = chunks[chunkNum]
                            ->getRowPath(j, rowNameStorage);
                        recorder.record(rowName, j);
                    }
                
                    recorder.commit();
                };
        
            // NOTE: we currently re-index everything from the
            // previous chunks; this is a big scalability problem.
            // We should not do that once multiple commits become
            // an important use case
            parallelMap(0, chunks.size(), indexChunk);

            std::vector<std::tuple<int, int, int, int> > possibleCollisions;
            std::tie(rowIndex, possibleCollisions)
                = index.freeze(*owner->serializer);

            cerr << possibleCollisions.size() << " possible collisions"
                 << endl;

            std::set<Path> duplicateRowNames;
            bool extraDuplicates = false;
            
            for (auto & c: possibleCollisions) {
                Path name1 = chunks[std::get<0>(c)]->getRowPath(std::get<1>(c));
                Path name2 = chunks[std::get<2>(c)]->getRowPath(std::get<3>(c));
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
                throw AnnotatedException
                    (400, "Duplicate row name(s) in tabular dataset: "
                     + duplicateNames,
                     "duplicates", duplicateRowNames);
            }
            
            cerr << "rowIndex.memusage() = " << rowIndex.memusage()
                 << endl;
            cerr << "row index took " << rowIndexTimer.elapsed();
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

        static float extractVal(const CellValue & val, float *)
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
                        throw AnnotatedException
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
        extractNumbers(size_t numValues,
                       const std::vector<ColumnPath> & columnNames,
                       float * output) override
        {
            return extractT<float>(numValues, columnNames, output);
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
    LightweightHash<uint64_t, int> fixedColumnIndex;

    /// All chunks we've added but haven't yet committed
    std::vector<std::shared_ptr<TabularDatasetChunk> > frozenChunks;

    /// Configuration passed in.  Constant after initialization.
    TabularDatasetConfig config;

    /// The number of background jobs that we're currently waiting for
    std::atomic<size_t> backgroundJobsActive;

    /// Logger instance for this class
    shared_ptr<spdlog::logger> logger;

    /// Handler for special routes called on this dataset
    RestRequestRouter router;

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

    virtual uint64_t getRowCount() const override
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

    void initRoutes()
    {
        addRouteSyncJsonReturn(router, "/saves", {"POST"},
                               "Save the dataset to the given artifact",
                               "Information about the saved artifact",
                               &TabularDataStore::save,
                               this,
                               JsonParam<Url>("dataFileUrl", "URI of artifact to save under"));
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return router.processRequest(connection, request, context);
    }

    /// Create a new current state from the old one plus the extra
    /// chunks.
    std::shared_ptr<const CurrentState>
    finalize(std::shared_ptr<const CurrentState> oldState,
             std::vector<std::shared_ptr<TabularDatasetChunk> > & inputChunks)
    {
        // NOTE: must be called with the lock held
        if (inputChunks.empty()) {
            return oldState;
        }

        cerr << "commiting " << frozenChunks.size() << " frozen chunks"
             << endl;

        auto newState = std::make_shared<CurrentState>(*oldState);

        newState->addChunks(inputChunks);
        newState->reIndex();
        
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
            std::atomic<uint64_t> rowsIndexed = 0;

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
                
                    rowsIndexed += recorder.commit();
                };
        
            // NOTE: we currently re-index everything from the
            // previous chunks; this is a big scalability problem.
            // We should not do that once multiple commits become
            // an important use case
            parallelMap(0, newState->chunks.size(), indexChunk);

            // Verify that all of our rows were indexed
            ExcAssertEqual(rowsIndexed, newState->rowCount);

            // Verify the maxChunkIndex field
            uint32_t maxChunkIndex = 0;
            for (int chunkNum = 0;  chunkNum < newState->chunks.size();  ++chunkNum) {
                if (newState->chunks[chunkNum]->rowCount())
                    maxChunkIndex = std::max<uint32_t>(maxChunkIndex, newState->chunks[chunkNum]->rowCount() - 1);
            }

            ExcAssertEqual(index.maxChunkIndex, maxChunkIndex);
            ExcAssertEqual(index.maxChunkNumber, newState->chunks.size() - 1);

            std::vector<std::tuple<int, int, int, int> > possibleCollisions;
            std::tie(newState->rowIndex, possibleCollisions)
                = index.freeze(*serializer);

#if 0
            // Verify that we find all of our rows
            for (size_t i = 0;  i < newState->chunks.size();  ++i) {
                auto & c = newState->chunks[i];
                for (size_t j = 0;  j < c->rowCount();  ++j) {
                    RowPath rowNameStorage;
                    const RowPath & rowName = c->getRowPath(j, rowNameStorage);
                    auto chunks = newState->rowIndex.pathPossibleChunks(rowName);

                    bool found = false;
                    for (auto & c: chunks) {
                        auto chk = c.first;
                        auto idx = c.second;

                        if (chk == i && idx == j) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        // index integrity problem... somehow we didn't find ours
                        auto rowHash = rowName.hash();
                        cerr << "NOT FOUND IN INDEX: " << rowName << " with hash " << rowHash << endl;
                        cerr << "shard " << i << " should be " << MutablePathIndex::getShard(rowHash) << endl;
                        cerr << "factor     " << newState->rowIndex.shards[i].factor << endl;
                        cerr << "numEntries " << newState->rowIndex.shards[i].numEntries << endl;
                        cerr << "chunkBits  " << (int)newState->rowIndex.shards[i].chunkBits << endl;
                        cerr << "offsetBits " << (int)newState->rowIndex.shards[i].offsetBits << endl;
                        cerr << "bucket " << newState->rowIndex.shards[i].getBucket(rowHash) << endl;
                        for (size_t k = 0;  k < newState->rowIndex.shards[i].numEntries;  ++k) {
                            auto entry = newState->rowIndex.shards[i].getEntry(k);
                            cerr << "  entry " << k << ":" << entry.first << "=" << entry.second << endl;
                        }
                        ExcAssert(false);
                    }
                }
            }

#endif
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
                throw AnnotatedException
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
                throw AnnotatedException(500,
                                          "Duplicate column name in tabular dataset",
                                          "columnName", fixedColumns[i]);
        }
    }

    void serialize(StructuredSerializer & serializer) const
    {
        auto cs = currentState.load();
        cs->serialize(serializer);
    }

    PolyConfigT<Dataset> save(Url dataFileUrl) const
    {
        MLDB::makeUriDirectory(dataFileUrl.toString());

        ZipStructuredSerializer serializer(dataFileUrl.toUtf8String());

        serialize(serializer);

        PolyConfigT<Dataset> result;
        result.type = "tabular";
        
        PersistentDatasetConfig params;
        params.dataFileUrl = dataFileUrl;
        result.params = params;

        return result;
    }

    void reconstitute(StructuredReconstituter & reconstituter)
    {
        auto newCurrentState = std::make_shared<CurrentState>
            (this, this->logger);
        newCurrentState->reconstitute(reconstituter);

        currentState.store(std::move(newCurrentState));
    }

    void load(Url dataFileUrl)
    {
        ZipStructuredReconstituter reconstituter(dataFileUrl);

        reconstitute(reconstituter);
    }
    
    /** This is a recorder that allows parallel records from multiple
        threads. */
    struct BasicRecorder: public Recorder {
        BasicRecorder(TabularDataStore * store)
            : Recorder(store->engine),
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
            : Recorder(store->engine), store(store)
        {
            // Note that this may return a null pointer, if nothing has
            // been loaded yet.
            chunk = store->createNewChunk();
            //cerr << "Creating new ChunkRecorder at " << this << " with chunk " << (bool)chunk << endl;
        }

        TabularDataStore * store = nullptr;
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
                     (store->fixedColumns, &store->fixedColumnIndex,
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
            auto frozen = chunk->freeze(*store->serializer, params);
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
            std::unique_lock<std::mutex> guard(store->datasetMutex);

            if (!chunk) {
                // We create a sample set of values for the
                // column to analyze, so it can identify the
                // column names.
                std::vector<std::tuple<ColumnPath, CellValue, Date> > sampleVals;
                sampleVals.reserve(columnNames.size());
                for (unsigned i = 0;  i < columnNames.size();  ++i)
                    sampleVals.emplace_back(columnNames[i], CellValue(), Date());
        
                store->createFirstChunks(sampleVals);
                chunk = store->createNewChunk();             
            }

            //cerr << "specializing recorder " << this << " for " << columnNames.size() << " columns" << endl;
            //cerr << "hasChunk = " << (bool)chunk << endl;

            // Currently we can't add new fixed columns, they are frozen once determined
            // So this function needs to identify which ones map, map them, and add those
            // which don't to extra.

            std::vector<int> outputPositions(columnNames.size());
            for (size_t i = 0;  i < columnNames.size();  ++i) {
                const auto & columnName = columnNames[i];
                auto it = store->fixedColumnIndex.find(columnName.hash());
                if (it == store->fixedColumnIndex.end()) {
                    outputPositions[i] = -1;
                }
                else {
                    outputPositions[i] = it->second;
                }
            }

            //cerr << "outputPositions = " << jsonEncode(outputPositions) << endl;

            auto numInputColumns = columnNames.size();
            auto numOutputColumns = store->fixedColumns.size();

            return [=] (RowPath rowName, Date timestamp,
                        CellValue * vals, size_t numVals,
                        std::vector<std::pair<ColumnPath, CellValue> > extra)
                {
                    ExcAssert(chunk);
                    ExcAssertEqual(numVals, numInputColumns);

                    PossiblyDynamicBuffer<CellValue> output(numOutputColumns);
                    for (size_t i = 0;  i < numInputColumns;  ++i) {
                        int pos = outputPositions[i];
                        if (pos == -1) {
                            extra.emplace_back(columnNames[i], std::move(vals[i]));
                        }
                        else {
                            output[pos] = std::move(vals[i]);
                        }
                    }

                    //cerr << "recording " << numVals << " values and " << extra.size()
                    //     << " extra into specialized recorder for " << chunk->columns.size()
                    //     << " columns specialized for " << columnNames.size() << " columns" << endl;

                    for (;;) {
                        int written = chunk->add(rowName, timestamp,
                                                 output.data(), output.size(),
                                                 extra);
                        if (written == MutableTabularDatasetChunk::ADD_SUCCEEDED)
                            break;
                        
                        ExcAssertEqual(written,
                                       MutableTabularDatasetChunk::ADD_PERFORM_ROTATION);
                        finishedChunk();
                        chunk.reset
                            (new MutableTabularDatasetChunk
                             (store->fixedColumns, &store->fixedColumnIndex,
                              chunkSizeForNumColumns(numOutputColumns)));
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
                (fixedColumns, &fixedColumnIndex, chunkSizeForNumColumns(fixedColumns.size()));
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
        auto gb = [] (size_t s) { return 1.0 * s / 1000000000.0; };
        auto perrow = [&] (size_t s) { return 1.0 * s / totalRows; };

        size_t mem = 0;
        size_t rowNameMem = 0, timestampMem = 0;
        for (auto & c: newState->chunks) {
            mem += c->memusage();
            rowNameMem += c->rowNames->memusage();
            timestampMem += c->timestamps->memusage();
        }

        size_t columnMem = 0;
        size_t totalColumnMem = 0;
        std::vector<std::pair<size_t, ColumnPath>> colMem;

        if (!newState->chunks.empty()) {
            INFO_MSG(logger) << "row name usage is " << gb(rowNameMem)
                             << " gb at "
                             << perrow(rowNameMem) << " per row with "
                             << MLDB::type_name(*newState->chunks[0]->rowNames);
            INFO_MSG(logger) << "timestamp usage is " << timestampMem
                             << " bytes at "
                             << perrow(timestampMem) << " per row with "
                             << MLDB::type_name(*newState->chunks[0]->timestamps);
            colMem.emplace_back(rowNameMem, "<<ROW NAMES>>");
            colMem.emplace_back(timestampMem, "<<TIMESTAMP>>");

            totalColumnMem += rowNameMem + timestampMem;
        }

        for (auto & c: newState->columns) {
            size_t bytesUsed = 0;
            for (auto & chunk: c.chunks) {
                bytesUsed += chunk.second->memusage();
            }
            INFO_MSG(logger)
                << "column " << c.columnName << " used "
                << gb(bytesUsed) << " gb at "
                << perrow(bytesUsed) << " per row with "
                << MLDB::type_name(*c.chunks[0].second);
            columnMem += bytesUsed;
            totalColumnMem += bytesUsed;
            colMem.emplace_back(bytesUsed, c.columnName);
        }

        size_t rowIndexMem = newState->rowIndex.memusage();
        mem += rowIndexMem;
        colMem.emplace_back(rowIndexMem, "<<ROW INDEX>>");

        INFO_MSG(logger) << "row index usage is " << gb(rowIndexMem)
                         << " gb at "
                         << perrow(rowIndexMem) << " per row";
        

        INFO_MSG(logger) << "total mem usage is " << gb(mem) << "gb for "
                         << totalRows << " rows and "
                         << newState->columns.size()
                         << " columns for "
                         << perrow(mem) << " bytes/row";
        INFO_MSG(logger) << "column memory is " << columnMem;
        INFO_MSG(logger) << "total column memory is " << totalColumnMem;

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
                auto frozen = chunk->freeze(*serializer, params);
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
            (fixedColumns, &fixedColumnIndex,
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
            LightweightHash<uint64_t, int> inputColumnIndex;
            for (unsigned i = 0;  i < vals.size();  ++i) {
                const ColumnPath & c = std::get<0>(vals[i]);
                uint64_t ch(c.oldHash());
                if (!inputColumnIndex.insert(make_pair(ch, i)).second)
                    throw AnnotatedException(400, "Duplicate column name in tabular dataset entry",
                                              "columnName", c.toUtf8String());
                columnNames.push_back(c);
            }

            initialize(std::move(columnNames));

            auto newChunks = std::make_shared<ChunkList>(NUM_PARALLEL_CHUNKS);

            for (auto & c: *newChunks) {
                auto newChunk = std::make_shared<MutableTabularDatasetChunk>
                    (fixedColumns, &fixedColumnIndex, chunkSizeForNumColumns(fixedColumns.size()));
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
                    throw AnnotatedException
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
                    (fixedColumns, &fixedColumnIndex, chunkSizeForNumColumns(fixedColumns.size()));
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
TabularDataset(MldbEngine * owner,
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

RestRequestMatchResult
TabularDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    RestRequestMatchResult result
        = itl->handleRequest(connection, request, context);
    if (result == MR_NO) {
        result = Dataset::handleRequest(connection, request, context);
    }
    return result;
}


/*****************************************************************************/
/* TABULAR DATASET                                                           */
/*****************************************************************************/

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

    addAuto("unknownColumns", &TabularDatasetConfig::unknownColumns,
            "Action to take on unknown columns.  Values are 'ignore', "
             "'error' (default), or 'add' which will allow an unlimited "
            "number of sparse columns to be added.");
    addField("dataFileUrl", &TabularDatasetConfig::dataFileUrl,
             "URL (which must currently be on the local filesystem, ie "
             "file://...) from which the data will be memory mapped.  In "
             "the case that the given URL does not exist, it will be "
             "created and the file used as a memory mapped backing for "
             "the data file.");
}

namespace {

RegisterDatasetType<TabularDataset, TabularDatasetConfig>
regTabular(builtinPackage(),
           "tabular",
           "Columnar dataset which can be recorded to",
           "datasets/TabularDataset.md.html");

} // file scope

} // MLDB
