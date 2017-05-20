// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** behavior_dataset.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Dataset interface to behaviors.
*/

#include "mldb/sql/sql_expression.h"
#include "mldb/plugins/behavior/behavior_utils.h"

#include "mldb/arch/timers.h"
#include "mldb/base/less.h"
#include "mldb/base/parallel.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/hex_dump.h"
#include "behavior_dataset.h"
#include "mldb/types/any_impl.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/compact_vector_description.h"
#include "mldb/vfs/fs_utils.h"
#include "behavior/behavior_manager.h"
#include "behavior/mutable_behavior_domain.h"
#include "mldb/types/map_description.h"
#include "mldb/types/hash_wrapper_description.h"
#include "behavior/behavior_utils.h"
#include "mldb/server/dataset_utils.h"
#include <future>

using namespace std;


namespace MLDB {

using namespace behaviors;

// Static instance of a behavior manager, shared between all beh datasets
BehaviorManager behManager;


/*****************************************************************************/
/* BEHAVIOR DATASET CONFIG                                                  */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(BehaviorDatasetConfig);

BehaviorDatasetConfigDescription::
BehaviorDatasetConfigDescription()
{
    nullAccepted = true;

    addField("dataFileUrl", &BehaviorDatasetConfig::dataFileUrl,
             "URL of the data file (with extension '.beh') from which to load the dataset.");
}



/*****************************************************************************/
/* KEY VALUE INFO                                                            */
/*****************************************************************************/

struct BehaviorValueInfo {
    BehaviorValueInfo()
        : rowCount(0)
    {
    }

    int rowCount;
    compact_vector<BH, 1> itl;  /// For use of the implementation

    void merge(BehaviorValueInfo & other)
    {
        rowCount += other.rowCount;
        itl.insert(itl.end(), other.itl.begin(), other.itl.end());
    }

    void merge(BehaviorValueInfo && other)
    {
        rowCount += other.rowCount;
        itl.insert(itl.end(), other.itl.begin(), other.itl.end());
    }

    bool operator < (const BehaviorValueInfo & other) const
    {
        return MLDB::less_all(rowCount, other.rowCount,
                            itl, other.itl);
    }
};

DEFINE_STRUCTURE_DESCRIPTION(BehaviorValueInfo);

BehaviorValueInfoDescription::
BehaviorValueInfoDescription()
{
    addField("rc", &BehaviorValueInfo::rowCount, "Number of rows");
    addField("behs", &BehaviorValueInfo::itl, "Behaviors with this value");
}

struct BehaviorColumnInfo {
    BehaviorColumnInfo()
        : rowCount(0), coveringType(CellValue::EMPTY) /*, min(INFINITY), max(-INFINITY) */
    {
    }

    uint64_t rowCount;
    ColumnPath columnName;
    std::map<CellValue, BehaviorValueInfo> values;
    CellValue::CellType coveringType;
    //double min;   ///< For integer or string types only
    //double max;   ///< For integer or string types only

    /// Count of each cell type
    //std::map<CellValue::CellType, int> types;

#if 0
    void merge(BehaviorColumnInfo & other)
    {
        count += other.count;
        for (auto & v: other.values) {
            values[v.first].merge(v.second);
        }

        min = std::min(min, other.min);
        max = std::max(max, other.max);

        for (auto & v: other.values) {
            values[v.first].merge(v.second);
        }

        for (auto & t: other.types)
            types[t.first] += t.second;
    }
#endif
};

DEFINE_STRUCTURE_DESCRIPTION(BehaviorColumnInfo);

BehaviorColumnInfoDescription::
BehaviorColumnInfoDescription()
{
    addField("rowCount", &BehaviorColumnInfo::rowCount, "Number of rows with this column");
    addField("columnName", &BehaviorColumnInfo::columnName, "Name of column");
    addField("values", &BehaviorColumnInfo::values, "Values with this column");
    addField("coveringType", &BehaviorColumnInfo::coveringType, "Type covering this value");
}


/*****************************************************************************/
/* BEHAVIOR KEY VALUE INDEX                                                 */
/*****************************************************************************/

struct BehaviorKeyValueIndex {

    BehaviorKeyValueIndex(std::shared_ptr<BehaviorDomain> behs)
        : behs(behs)
    {
        init();
    }

    std::shared_ptr<BehaviorDomain> behs;

    std::map<ColumnHash, BehaviorColumnInfo> columnInfo;
    std::unordered_map<BH, std::pair<ColumnPath, CellValue> > behIndex;

    void serialize(const std::string & filename)
    {
        filter_ostream stream(filename);

        stream << columnInfo.size() << endl;
        for (auto & c: columnInfo) {
            stream << jsonEncodeStr(pair<ColumnHash, BehaviorColumnInfo>(c)) << endl;
        }
        stream << behIndex.size() << endl;
        for (auto & b: behIndex)
            stream << jsonEncodeStr(pair<BH, pair<ColumnHash, CellValue> >(b)) << endl;
    }


    // Output of the map phase
    struct ValueEntry {
        ColumnHash column;
        CellValue value;
        BehaviorValueInfo info;
    };

    void init()
    {
        Timer timer;

        //std::vector<int> rowCountDistribution(behs->behaviorCount());
    
        constexpr int numBuckets = 32;
        std::mutex locks[numBuckets];
        //typedef std::map<ColumnPath, BehaviorColumnInfo> Bucket;
        //Bucket columnInfo[numBuckets];

        struct Bucket2 : public std::vector<std::pair<std::pair<ColumnHash, CellValue>, BehaviorValueInfo> > {
            std::set<ColumnPath> columnNames;

            void swap(Bucket2 & other)
            {
                std::vector<std::pair<std::pair<ColumnHash, CellValue>, BehaviorValueInfo> >::swap(other);
                columnNames.swap(other.columnNames);
            }
        };

        vector<Bucket2> buckets2(numBuckets);

        for (auto & b: buckets2) {
            b.reserve(behs->behaviorCount() / numBuckets * 2);
        }

        for (auto b: behs->allBehaviorHashes()) {
            this->behIndex[b];  // default construct
        }

        auto onBehavior = [&] (BH beh, const BehaviorIterInfo & info,
                                const BehaviorStats & stats)
            {
                Id id = behs->getBehaviorId(beh);

                ColumnPath columnName;
                CellValue cellValue;
                std::tie(columnName, cellValue)
                    = behaviors::decodeColumn(id, true /* could be legacy */);

                int bucket = info.index % numBuckets;

                //int bucket = columnName.hash() % numBuckets;

                BehaviorValueInfo valueInfo;
                valueInfo.rowCount = stats.subjectCount;
                valueInfo.itl.push_back(beh);

                //rowCountDistribution.at(info.index) = stats.rowCount;
                this->behIndex[beh] = { columnName, cellValue };

                std::unique_lock<std::mutex> guard(locks[bucket]);

                buckets2[bucket].emplace_back(make_pair(columnName,
                                                        std::move(cellValue)),
                                              std::move(valueInfo));
                buckets2[bucket].columnNames.insert(columnName);
                return true;
            };
        
        cerr << "processing " << behs->behaviorCount() << " behaviors" << endl;
        behs->forEachBehaviorParallel(onBehavior,
                                       BehaviorDomain::ALL_BEHAVIORS,
                                       BehaviorDomain::BS_SUBJECT_COUNT);

        cerr << timer.elapsed() << endl;

        cerr << "merging all buckets" << endl;

        std::function<void (Bucket2 & result, Bucket2 *, Bucket2 *)> sortBuckets
            = [&] (Bucket2 & result, Bucket2 * first, Bucket2 * last)
            {
                if (first + 1 == last) {
                    result.swap(*first);
                    std::sort(result.begin(), result.end());
                    return;
                }

                int e = last - first;
                int n = e / 2;

                Bucket2 b1, b2;

                auto f1 = std::async(std::launch::async, sortBuckets, std::ref(b1), first, first + n);
                auto f2 = std::async(std::launch::async, sortBuckets, std::ref(b2), first + n, last);

                //sortBuckets(b1, first, first + n);
                //sortBuckets(b2, first + n, last);

                f1.wait();
                f2.wait();

                result.reserve(b1.size() + b2.size());
                std::merge(b1.begin(), b1.end(), b2.begin(), b2.end(),
                           back_inserter(result));

                result.columnNames = std::move(b1.columnNames);
                result.columnNames.insert(b2.columnNames.begin(),
                                          b2.columnNames.end());

                //cerr << "merging of " << e << " buckets is " << result.size()
                //     << " entries" << endl;
            };

        Bucket2 allSorted;
        sortBuckets(allSorted, &buckets2[0], &buckets2[numBuckets]);

        if (allSorted.empty())
            return;

        cerr << "allSorted.columnNames.size() = " << allSorted.columnNames.size()
             << endl;

        // Fill in the column names
        for (auto & n: allSorted.columnNames)
            columnInfo[n].columnName = n;

        cerr << timer.elapsed() << endl;
        cerr << "finding column ranges" << endl;

        // Get a list of columns, and the value within the column
        vector<tuple<ColumnHash, size_t, size_t> > columnRanges;
        
        ColumnHash current = allSorted.at(0).first.first;
        size_t lastEnd = 0;
        for (size_t i = 0;  i <= allSorted.size();  ++i) {
            if (i == allSorted.size() || (allSorted[i].first.first != current && i != 0)) {
                columnInfo[current];  // default construct it
                columnRanges.emplace_back(current, lastEnd, i);
                lastEnd = i;
                if (i == allSorted.size())
                    break;
                current = allSorted[i].first.first;
            }
        }

        cerr << timer.elapsed() << endl;
        cerr << "extracting info for " << columnRanges.size() << " columns"
             << endl;

        auto doColumn = [&] (int n)
            {
                size_t first, last;
                ColumnHash hash;
                std::tie(hash, first, last) = columnRanges[n];

                auto & info = columnInfo[hash];

                //cerr << "column " << info.columnName << " has "
                //     << last-first << " entries"
                //     << endl;

                int64_t types[CellValue::NUM_CELL_TYPES] = { 0, 0, 0, 0 };

                size_t rowCount = 0;
                for (size_t i = first;  i != last;  ++i) {
                    types[allSorted[i].first.second.cellType()] += 1;
                    info.values[allSorted[i].first.second]
                        .merge(std::move(allSorted[i].second));
                    rowCount += allSorted[i].second.rowCount;
                }

                info.rowCount = rowCount;
                if (types[CellValue::ASCII_STRING])
                    info.coveringType = CellValue::ASCII_STRING;
                if (types[CellValue::UTF8_STRING])
                    info.coveringType = CellValue::UTF8_STRING;
                else if (types[CellValue::FLOAT])
                    info.coveringType = CellValue::FLOAT;
                else if (types[CellValue::INTEGER])
                    info.coveringType = CellValue::INTEGER;
                else info.coveringType = CellValue::EMPTY;
            };

        parallelMap(0, columnRanges.size(), doColumn);

        cerr << timer.elapsed() << endl;

        return;
    }

    std::pair<ColumnPath, CellValue> extractColumn(BH beh) const
    {
        auto it = behIndex.find(beh);
        if (it == behIndex.end()) {
            throw MLDB::Exception("No behavior found");
        }

        return it->second;
    }
};


/*****************************************************************************/
/* BEHAVIOR COLUMN INDEX                                                    */
/*****************************************************************************/

struct BehaviorColumnIndex: public ColumnIndex {
    BehaviorColumnIndex(std::shared_ptr<BehaviorDomain> behs)
        : behs(behs)
    {
        index.reset(new BehaviorKeyValueIndex(behs));
        //index->serialize("index.txt.gz");
    }

    std::shared_ptr<BehaviorDomain> behs;
    std::shared_ptr<BehaviorKeyValueIndex> index;

    virtual const ColumnStats &
    getColumnStats(const ColumnPath & ch, ColumnStats & stats) const
    {
        auto it = index->columnInfo.find(ch);
        if (it == index->columnInfo.end()) {
            stats = ColumnStats();
        }

        auto & c = *it;
        
        //ColumnPath name(c.second.name);

        stats.isNumeric_
            = c.second.coveringType == CellValue::INTEGER
            || c.second.coveringType == CellValue::FLOAT;
        stats.rowCount_ = c.second.rowCount;
        
        for (auto & v: c.second.values) {
            auto & val = stats.values[v.first];
            val.rowCount_ = v.second.rowCount;
        }
        
        return stats;
    }

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnPath & column) const
    {
        MatrixColumn result;

        auto it2 = index->columnInfo.find(column);
        if (it2 == index->columnInfo.end())
            return result;

        const BehaviorColumnInfo & info = it2->second;

        // Now for each value we need to fill it in

        result.columnName = info.columnName;
        result.columnHash = info.columnName;

        // For each value
        for (auto & v: info.values) {

            // For each behavior that makes up the value
            for (auto & beh: v.second.itl) {

                auto subjTs = behs->getSubjectHashesAndAllTimestamps(beh, SH::max(),
                                                                     true /* sorted */);
                for (auto & ts: subjTs)
                    result.rows.emplace_back(toPathElement(behs->getSubjectId(ts.first)), v.first, ts.second);
            }
        }

        std::sort(result.rows.begin(), result.rows.end());

        return result;
    }

    /** Return the value of the column for all rows, ignoring timestamps. */
    virtual std::vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & column,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        std::vector<std::tuple<RowPath, CellValue> > result;

        auto it2 = index->columnInfo.find(column);
        if (it2 == index->columnInfo.end())
            return result;

        const BehaviorColumnInfo & info = it2->second;

        // For each value
        for (auto & v: info.values) {

            if (filter && !filter(v.first))
                continue;

            // For each behavior that makes up the value
            for (auto & beh: v.second.itl) {

                auto subjTs = behs->getSubjectHashes(beh, SH::max(),
                                                     true /* sorted */);
                for (auto & ts: subjTs)
                    result.emplace_back(toPathElement(behs->getSubjectId(ts)), v.first);
            }
        }

        std::sort(result.begin(), result.end());
        
        return result;
    }

    virtual uint64_t getColumnRowCount(const ColumnPath & column) const
    {
        auto it2 = index->columnInfo.find(column);
        if (it2 == index->columnInfo.end())
            return 0;

        const BehaviorColumnInfo & info = it2->second;

        // Now for each value we need to fill it in

        Lightweight_Hash_Set<SH> rows;

        // For each value
        for (auto & v: info.values) {
            // For each behavior that makes up the value
            for (auto & beh: v.second.itl) {

                auto subj = behs->getSubjectHashes(beh, SH::max(), false /* sorted */);
                rows.insert(subj.begin(), subj.end());
            }
        }        
        
        return rows.size();
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        return index->columnInfo.count(column);
    }

    virtual std::vector<ColumnPath>
    getColumnPaths(ssize_t offset, ssize_t limit) const
    {
        std::vector<ColumnPath> result;
        for (auto & col: index->columnInfo)
            result.emplace_back(col.second.columnName);
        return applyOffsetLimit(offset, limit, result);
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        return behs->allSubjectPath(start, limit);
    }

};


/*****************************************************************************/
/* BEHAVIOR MATRIX VIEW                                                     */
/*****************************************************************************/

struct BehaviorMatrixView: public MatrixView {
    BehaviorMatrixView(std::shared_ptr<BehaviorDomain> behs,
                        std::shared_ptr<BehaviorKeyValueIndex> kvIndex)
        : behs(behs), kvIndex(kvIndex)
    {
    }

    std::shared_ptr<BehaviorDomain> behs;
    std::shared_ptr<BehaviorKeyValueIndex> kvIndex;

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        auto subs = behs->allSubjectHashes(SH::max(), true /* sorted */);

        if (limit == -1)
            limit = subs.size();

        std::vector<RowPath> result;
        result.reserve(limit);

        for (size_t i = start;  i < start + limit && i < subs.size();  ++i)
            result.emplace_back(toPathElement(behs->getSubjectId(subs[i])));
        
        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        auto subs = behs->allSubjectHashes(SH::max(), true /* sorted */);

        std::sort(subs.begin(), subs.end());

        if (limit == -1)
            limit = subs.size();

        std::vector<RowHash> result;

        for (int i = start;  i < start + limit && i < subs.size();  ++i)
            result.emplace_back(subs[i]);
        
        return result;
    }

    virtual bool knownRow(const RowPath & rowName) const
    {
        return behs->knownSubject(rowName);
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        return behs->knownSubject(rowHash);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        MatrixNamedRow result;
        result.rowHash = rowName;
        result.rowName = rowName;

        auto onBeh = [&] (BH beh, Date ts, int)
            {
                auto kv = kvIndex->extractColumn(beh);
                result.columns.emplace_back(std::move(kv.first),
                                            std::move(kv.second),
                                            ts);

                return true;
            };

        behs->forEachSubjectBehavior(rowName, onBeh);
        
        return result;
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        return toPathElement(behs->getSubjectId(rowHash));
    }

    virtual MatrixEvent getEvent(const RowHash & rowHash, Date ts) const
    {
        throw MLDB::Exception("getEvent() function is buggy... doesn't return correct values");

        Lightweight_Hash<BH, int> ev = behs->getSubjectTimestamp(rowHash, ts);

        MatrixEvent result;
        result.rowName = toPathElement(behs->getSubjectId(rowHash));
        result.rowHash = rowHash;
        result.timestamp = ts;

        for (auto & b: ev) {
            result.columns.emplace_back(ColumnHash(b.first.hash()), b.second);
        }

        return result;
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        return kvIndex->columnInfo.count(column);
    }

    virtual ColumnPath getColumnPath(ColumnHash column) const
    {
        auto it = kvIndex->columnInfo.find(column);
        if (it == kvIndex->columnInfo.end())
            throw MLDB::Exception("couldn't find given column %s in dataset with "
                                "%zd columns", column.toString().c_str(),
                                kvIndex->columnInfo.size());
        return it->second.columnName;
    }

    virtual std::vector<ColumnPath>
    getColumnPaths(ssize_t offset, ssize_t limit) const
    {
        std::vector<ColumnPath> result;
        for (auto & col: kvIndex->columnInfo)
            result.emplace_back(col.second.columnName);
        return applyOffsetLimit(offset, limit, result);
    }

    virtual size_t getRowCount() const
    {
        return behs->subjectCount();
    }

    virtual size_t getColumnCount() const
    {
        return kvIndex->columnInfo.size();
    }
};


/*****************************************************************************/
/* BEHAVIOR DATASET                                                         */
/*****************************************************************************/

BehaviorDataset::
BehaviorDataset(MldbServer * owner,
                 PolyConfig config,
                 const ProgressFunc & onProgress)
    : Dataset(owner)
{
    ExcAssert(!config.id.empty());
    
    auto params = config.params.convert<BehaviorDatasetConfig>();
    
    //TODO - MLDB-2110 pass progress further down
    behs = behManager.get(params.dataFileUrl.toString(),
                          BehaviorManager::CACHE_CONFIG,
                          nullptr /*onProgress*/);

    const Json::Value type = behs->getFileMetadata("mldbEncoding");
    if (type.isString()) {
        const auto typeStr = type.asString();
        if (typeStr != "beh") {
            throw MLDB::Exception(
                "The loaded dataset is not of type beh, it is of "
                "type %s", typeStr.c_str());
        }
    }
    //result->tranches.infer(*result->behs);
    //this->config.reset(new PolyConfig(std::move(config)));
    //result->lastAccessed = Date::now();
    //result->isFrozen_ = true;
    columns = std::make_shared<BehaviorColumnIndex>(behs);
    matrix = std::make_shared<BehaviorMatrixView>(behs, columns->index);
}

BehaviorDataset::
~BehaviorDataset()
{
}

Any
BehaviorDataset::
getStatus() const
{
    Json::Value result;
    result["rowCount"] = behs->subjectCount();
    result["columnCount"] = columns->index->columnInfo.size();
    result["valueCount"] = behs->behaviorCount();
    result["memUsageMb"] = behs->approximateMemoryUsage() / 1000000.0;
    return result;
}

std::pair<Date, Date>
BehaviorDataset::
getTimestampRange() const
{
    return { behs->earliestTime(), behs->latestTime() };
}

Date
BehaviorDataset::
quantizeTimestamp(Date timestamp) const
{
    return behs->unQuantizeTime(behs->quantizeTime(timestamp));
}

std::shared_ptr<MatrixView>
BehaviorDataset::
getMatrixView() const
{
    return matrix;
}

std::shared_ptr<ColumnIndex>
BehaviorDataset::
getColumnIndex() const
{
    return columns;
}

struct BehaviorDatasetRowStream : public MLDB::RowStream
{
    BehaviorDatasetRowStream(std::shared_ptr<const BehaviorDomain> source)
        : source(std::move(source))
    {
    }

    virtual std::shared_ptr<RowStream> clone() const
    {
        return make_shared<BehaviorDatasetRowStream>(source);
    }

    /* set where the stream should start*/
    virtual void initAt(size_t start)
    {
        shStream = source->getSubjectStream(start);
    }

    virtual MLDB::RowPath next()
    {
        Id result = shStream->current();
        advance();
        return toPathElement(result);
    }
   
    virtual bool supportsExtendedInterface() const
    {
        return false;
        // Once extractColumns is implemented, we can return true
    }

    virtual const MLDB::RowPath & rowName(MLDB::RowPath & storage) const
    {
        return storage = toPathElement(shStream->current());
    }

    virtual void advance()
    {
        shStream->advance();
    }

    virtual void advanceBy(size_t n)
    {
        shStream->advanceBy(n);
    }

    unique_ptr<BehaviorDomain::SubjectStream> shStream;
    std::shared_ptr<const BehaviorDomain> source;
    Id lastSubjectId;
};

std::shared_ptr<RowStream> 
BehaviorDataset::
getRowStream() const
{
    return make_shared<BehaviorDatasetRowStream>(this->behs);
}


/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET CONFIG                                          */
/*****************************************************************************/

MutableBehaviorDatasetConfig::
MutableBehaviorDatasetConfig() 
    : timeQuantumSeconds(1.0)
{
}

DEFINE_STRUCTURE_DESCRIPTION(MutableBehaviorDatasetConfig);

MutableBehaviorDatasetConfigDescription::
MutableBehaviorDatasetConfigDescription()
{
    nullAccepted = true;
    addParent<BehaviorDatasetConfig>();
    addField("timeQuantumSeconds", &MutableBehaviorDatasetConfig::timeQuantumSeconds,
             "a number that controls the resolution of timestamps stored in the dataset, "
             "in seconds. 1 means one second, 0.001 means one millisecond, 60 means one minute. "
             "Higher resolution requires more memory to store timestamps.", 1.0);
}

/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET                                                 */
/*****************************************************************************/

MutableBehaviorDataset::
MutableBehaviorDataset(MldbServer * owner,
                        PolyConfig config,
                        const ProgressFunc & onProgress)
    : Dataset(owner)
{
    ExcAssert(!config.id.empty());
    auto params = config.params.convert<MutableBehaviorDatasetConfig>();
    behs.reset(new MutableBehaviorDomain());
    behs->timeQuantum = params.timeQuantumSeconds;
    this->address = params.dataFileUrl.toString();
}

MutableBehaviorDataset::
~MutableBehaviorDataset()
{
}

Any
MutableBehaviorDataset::
getStatus() const
{
    Json::Value result;
    result["rowCount"] = behs->subjectCount();
    result["valueCount"] = behs->behaviorCount();
    result["eventsRecorded"] = behs->totalEventsRecorded();
    result["memUsageMb"] = behs->approximateMemoryUsage() / 1000000.0;
    return result;
}

std::pair<Date, Date>
MutableBehaviorDataset::
getTimestampRange() const
{
    return { behs->earliestTime(), behs->latestTime() };
}

Date
MutableBehaviorDataset::
quantizeTimestamp(Date timestamp) const
{
    return behs->unQuantizeTime(behs->quantizeTime(timestamp));
}

std::shared_ptr<MatrixView>
MutableBehaviorDataset::
getMatrixView() const
{
    if (!matrix)
        throw MLDB::Exception("No matrix view for an uncommitted mutable dataset");
    return matrix;
}

std::shared_ptr<ColumnIndex>
MutableBehaviorDataset::
getColumnIndex() const
{
    if (!columns)
        throw MLDB::Exception("No matrix view for an uncommitted mutable dataset");
    return columns;
}

std::shared_ptr<RowStream> 
MutableBehaviorDataset::
getRowStream() const
{
    return make_shared<BehaviorDatasetRowStream>(this->behs);
}

void
MutableBehaviorDataset::
recordRowItl(const RowPath & rowName,
             const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
{

    vector<MutableBehaviorDomain::ManyEntryId> toRecord;
    toRecord.reserve(vals.size());
    for (auto & b: vals) {
        const ColumnPath& name = std::get<0>(b);
        const CellValue& value = std::get<1>(b);

        MutableBehaviorDomain::ManyEntryId entry;
        entry.behavior = behaviors::encodeColumn(name, value);
        entry.timestamp = std::get<2>(b);
        entry.count = 1;

        toRecord.emplace_back(std::move(entry));
    }

    behs->recordMany(toId(rowName), &toRecord[0], toRecord.size());
}

void
MutableBehaviorDataset::
recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows)
{
    Lightweight_Hash<RowHash, int64_t> rowIndex;
    std::vector<Id> rowNames;
    Lightweight_Hash<ColumnHash, int64_t> columnIndex;
    std::vector<Id> columnNames;

    auto getRowIndex = [&] (const RowPath & name)
        {
            RowHash rh(name);
            int64_t index
                = rowIndex.insert(make_pair(rh, rowIndex.size())).first->second;
            if (rowNames.size() < rowIndex.size())
                rowNames.emplace_back(toId(name));

            return index;
        };

    auto getColumnIndex = [&] (const ColumnPath & column, const CellValue & val)
        {
            ColumnPath name = toPathElement(behaviors::encodeColumn(column, val));
            ColumnHash ch(name);
            int64_t index
                = columnIndex.insert(make_pair(ch, columnIndex.size())).first->second;

            if (columnNames.size() < columnIndex.size())
                columnNames.emplace_back(toId(name));;
            return index;
        };
    
    std::vector<MutableBehaviorDomain::ManyEntryIndex> toRecord;
    for (auto & row: rows) {
        ExcAssertNotEqual(row.first, RowPath());
        validateNames(row.first, row.second);

        int64_t rowIndex = getRowIndex(row.first);

        for (auto & b: row.second) {
            const ColumnPath& name = std::get<0>(b);
            const CellValue& value = std::get<1>(b);
            Date ts = std::get<2>(b);

            int64_t colIndex = getColumnIndex(name, value);
            MutableBehaviorDomain::ManyEntryIndex entry;
            entry.behIndex = colIndex;
            entry.subjIndex = rowIndex;
            entry.timestamp = ts;

            toRecord.push_back(entry);
        }
    }

    behs->recordMany(&columnNames[0],
                     columnNames.size(),
                     &rowNames[0],
                     rowNames.size(),
                     &toRecord[0],
                     toRecord.size());
}

void
MutableBehaviorDataset::
commit()
{
    behs->setFileMetadata("mldbEncoding", "beh");
    behs->makeImmutable();
    if (!address.empty()) {
        MLDB::makeUriDirectory(this->address);
        behs->save(this->address);
    }
    columns = std::make_shared<BehaviorColumnIndex>(behs);
    matrix = std::make_shared<BehaviorMatrixView>(behs, columns->index);
}

namespace {

RegisterDatasetType<BehaviorDataset, BehaviorDatasetConfig>
regBeh(builtinPackage(),
       "beh",
       "Memory-mappable dataset type to efficiently store behavioral data",
       "datasets/BehaviorDataset.md.html");

RegisterDatasetType<MutableBehaviorDataset, MutableBehaviorDatasetConfig>
regMutableBeh(builtinPackage(),
              "beh.mutable",
              "Recordable dataset designed to store behavioral data",
              "datasets/MutableBehaviorDataset.md.html");

} // file scope

} // namespace MLDB


