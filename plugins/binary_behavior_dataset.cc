// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** binary_behavior_dataset.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Dataset interface to behaviors, in their pure (no value) form.
*/

#include "behavior_dataset.h"  // for behManager

#include "mldb/arch/timers.h"
#include "mldb/base/less.h"
#include "mldb/base/parallel.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/hex_dump.h"
#include "binary_behavior_dataset.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/any_impl.h"
#include "mldb/arch/spinlock.h"
#include "behavior/behavior_manager.h"
#include "behavior/mutable_behavior_domain.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/jml/utils/profile.h"
#include "behavior/behavior_utils.h"
#include "mldb/server/dataset_utils.h"
#include <future>


using namespace std;


namespace MLDB {

using namespace behaviors;

/*****************************************************************************/
/* BINARY BEHAVIOR DATASET                                                  */
/*****************************************************************************/

struct BinaryBehaviorDataset::Itl: public ColumnIndex, public MatrixView {

    static constexpr size_t NUM_COLUMN_NAME_BUCKETS=256;

    Itl(std::shared_ptr<BehaviorDomain> behs)
        : behs(behs), mutableBehs(std::dynamic_pointer_cast<MutableBehaviorDomain>(behs))
    {
        initRoutes();

        // Take an index of known column names so we don't have to call out
        // to the behavior domain.
        
        Spinlock bucketMutexes[NUM_COLUMN_NAME_BUCKETS];

        size_t numBehs = behs->behaviorCount();

        for (auto & b: knownColumnPaths) {
            b.reserve(numBehs * 2 / NUM_COLUMN_NAME_BUCKETS);
        }

        auto onBeh = [&] (BH beh, const BehaviorIterInfo &, const BehaviorStats & stats)
            {
                size_t bucketNumber = (beh.hash() >> 36) % NUM_COLUMN_NAME_BUCKETS;
                std::unique_lock<Spinlock> guard(bucketMutexes[bucketNumber]);
                knownColumnPaths[bucketNumber][stats.id] = stats.id;
                return true;
            };
        
        behs->forEachBehaviorParallel(onBeh, BehaviorDomain::ALL_BEHAVIORS,
                                       BehaviorDomain::BS_ID, PARALLEL);
    }

    std::shared_ptr<BehaviorDomain> behs;
    std::shared_ptr<MutableBehaviorDomain> mutableBehs;
    std::unordered_map<BH, Id> knownColumnPaths[NUM_COLUMN_NAME_BUCKETS];

    virtual const ColumnStats &
    getColumnStats(const ColumnPath & ch, ColumnStats & stats) const
    {
        auto behStats = behs->getBehaviorStats(BH(ch.hash()),
                                                BehaviorDomain::BS_SUBJECT_COUNT);

        stats.isNumeric_ = true;
        stats.rowCount_ = behStats.subjectCount;
        stats.values[CellValue(1)].rowCount_ = behStats.subjectCount;

        return stats;
    }

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnPath & column) const
    {
        MatrixColumn result;
        result.columnName = doGetColumnPath(toId(column));
        result.columnHash = column;

        std::vector<std::pair<SH, Date> > subjTs;

        // MLDB-500 if there is only one timestamp, we don't need to do extra
        // work to get all of them
        if (behs->earliestTime() == behs->latestTime()) {
            subjTs = behs->getSubjectHashesAndTimestamps(BH(column.hash()),
                                                         SH::max(),
                                                         true /* sorted */);
        }
        else {
            subjTs = behs->getSubjectHashesAndAllTimestamps(BH(column.hash()),
                                                            SH::max(),
                                                            true /* sorted */);
        }

        for (auto & ts: subjTs)
            result.rows.emplace_back(toPathElement(behs->getSubjectId(ts.first)), CellValue(1), ts.second);
        
        std::sort(result.rows.begin(), result.rows.end());

        return result;
    }

    virtual std::vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & column,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        std::vector<std::tuple<RowPath, CellValue> > result;

        // There is only one value possible: 1
        if (filter && !filter(CellValue(1)))
            return result;
        
        for (auto & ts: behs->getSubjectHashes
                 (BH(column.hash()), SH::max(), true /* sorted */))
            result.emplace_back(toPathElement(behs->getSubjectId(ts)), CellValue(1));

        std::sort(result.begin(), result.end());
        
        return result;
    }    

    virtual uint64_t getColumnRowCount(const ColumnPath & column) const
    {
        auto behStats = behs->getBehaviorStats(BH(column.hash()),
                                                BehaviorDomain::BS_SUBJECT_COUNT);
        return behStats.subjectCount;
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        return behs->knownBehavior(BH(column.hash()));
    }

    virtual std::vector<ColumnPath>
    getColumnPaths(ssize_t offset, ssize_t limit) const
    {
        std::vector<ColumnPath> result;

        auto onBehavior = [&] (BH, const BehaviorIterInfo &, const BehaviorStats & stats)
            {
                result.push_back(toPathElement(stats.id));
                return true;
            };
        
        behs->forEachBehaviorParallel(onBehavior,
                                      BehaviorDomain::ALL_BEHAVIORS,
                                      BehaviorDomain::BS_ID,
                                      ORDERED);
        
        return applyOffsetLimit(offset, limit, result);
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        return behs->allSubjectPath(start, limit);
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

    ColumnPath doGetColumnPath(BH beh) const
    {
        size_t bucketNumber = (beh.hash() >> 36) % NUM_COLUMN_NAME_BUCKETS;

        auto it = knownColumnPaths[bucketNumber].find(beh);
        if (it == knownColumnPaths[bucketNumber].end()) {
            // In the mutable case, we may have inserted it, so we ask
            // the behavior object for the Id if we can't find it
            // locally.
            return toPathElement(behs->getBehaviorId(beh));
        }
        else return toPathElement(it->second);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        MatrixNamedRow result;
        result.rowHash = result.rowName = rowName;

        auto onBeh = [&] (BH beh, Date ts, int)
            {
                result.columns.emplace_back(doGetColumnPath(beh), CellValue(1), ts);

                return true;
            };

        behs->forEachSubjectBehavior(rowName, onBeh);

        return result;
    }

    virtual MatrixRow getRowByHash(const RowHash & rowHash) const
    {
        MatrixRow result;
        result.rowHash = rowHash;
        result.rowName = toPathElement(behs->getSubjectId(rowHash));

        auto onBeh = [&] (BH beh, Date ts, int)
            {
                result.columns.emplace_back(ColumnHash(beh.hash()), CellValue(1), ts);

                return true;
            };

        behs->forEachSubjectBehavior(rowHash, onBeh);

        return result;
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        return toPathElement(behs->getSubjectId(rowHash));
    }

    virtual MatrixEvent getEvent(const RowHash & rowHash, Date ts) const
    {
        Lightweight_Hash<BH, int> ev = behs->getSubjectTimestamp(rowHash, ts);

        MatrixEvent result;
        result.rowName = toPathElement(behs->getSubjectId(rowHash));
        result.rowHash = rowHash;
        result.timestamp = ts;

        for (auto & b: ev) {
            result.columns.emplace_back(ColumnHash(b.first.hash()), CellValue(1));
        }
        
        return result;
    }

    virtual ColumnPath getColumnPath(ColumnHash column) const
    {
        return doGetColumnPath(BH(column.hash()));
    }

    virtual size_t getRowCount() const
    {
        return behs->subjectCount();
    }

    virtual size_t getColumnCount() const
    {
        return behs->behaviorCount();
    }

    virtual PolyConfigT<Dataset> save(const Url & dataFileUrl)
    {
        MLDB::makeUriDirectory(dataFileUrl.toString());
        behs->save(dataFileUrl.toString());
        
        PolyConfigT<Dataset> result;
        result.type = "beh.binary";

        PersistentDatasetConfig params;
        params.dataFileUrl = dataFileUrl;
        result.params = params;

        return result;
    }
    
    RestRequestRouter router;

    void initRoutes()
    {
        addRouteSyncJsonReturn(router, "/saves", {"POST"},
                               "Save the dataset to the given artifact",
                               "Information about the saved artifact",
                               &Itl::save,
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


    struct BinaryBehaviorDatasetRowStream : public MLDB::RowStream
    {
        BinaryBehaviorDatasetRowStream(const BinaryBehaviorDataset::Itl* source)
            : source(source)
        {

        }

        virtual std::shared_ptr<RowStream> clone() const
        {
            return make_shared<BinaryBehaviorDatasetRowStream>(source);
        }

        /* set where the stream should start*/
        virtual void initAt(size_t start)
        {
            shStream = source->behs->getSubjectStream(start);
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
        const BinaryBehaviorDataset::Itl* source;
    };

    std::shared_ptr<RowStream>
    getRowStream() const
    {
        return make_shared<BinaryBehaviorDatasetRowStream>(this);
    }

    std::shared_ptr<RowValueInfo>
    getRowInfo() const
    {
        // Requires support in MLDB for dealing with unknown info
        // (MLDB-1566)
        if (false && behs->behaviorCount() > 100000)
            return std::make_shared<UnknownRowValueInfo>();
        else {
            auto info = std::make_shared<BooleanValueInfo>();
            std::vector<KnownColumn> columns;
            for (auto & c: getColumnPaths(0 /* offset */, -1 /* limit */)) {
                columns.emplace_back(std::move(c), info, COLUMN_IS_SPARSE);
            }
            auto result = std::make_shared<RowValueInfo>(std::move(columns),
                                                         SCHEMA_CLOSED);
            return result;
        }
    }
};


/*****************************************************************************/
/* BINARY BEHAVIOR DATASET                                                  */
/*****************************************************************************/

BinaryBehaviorDataset::
BinaryBehaviorDataset(MldbServer * owner,
                       PolyConfig config,
                       const ProgressFunc & onProgress)
    : Dataset(owner)
{
    ExcAssert(!config.id.empty());
    
    auto params = config.params.convert<BehaviorDatasetConfig>();
    
    //TODO MLDB-2110 - pass the progress further down
    auto behs = behManager.get(params.dataFileUrl.toString(),
                               BehaviorManager::CACHE_CONFIG,
                               nullptr /*onProgress*/);

    const Json::Value type = behs->getFileMetadata("mldbEncoding");
    if (type.isString()) {
        const auto typeStr = type.asString();
        if (typeStr != "beh.binary") {
            throw MLDB::Exception(
                "The loaded dataset is not of type beh.binary, it is of "
                "type %s", typeStr.c_str());
        }
    }

    itl.reset(new Itl(behs));
}

BinaryBehaviorDataset::
BinaryBehaviorDataset(MldbServer * owner)
    : Dataset(owner)
{
}

BinaryBehaviorDataset::
~BinaryBehaviorDataset()
{
}

Any
BinaryBehaviorDataset::
getStatus() const
{
    Json::Value result;
    result["rowCount"] = itl->behs->subjectCount();
    result["columnCount"] = itl->behs->behaviorCount();
    result["memUsageMb"] = itl->behs->approximateMemoryUsage() / 1000000.0;
    result["cellCount"] = itl->behs->totalEventsRecorded();
    return result;
}

std::pair<Date, Date>
BinaryBehaviorDataset::
getTimestampRange() const
{
    return { itl->behs->earliestTime(), itl->behs->latestTime() };
}

Date
BinaryBehaviorDataset::
quantizeTimestamp(Date timestamp) const
{
    return itl->behs->unQuantizeTime(itl->behs->quantizeTime(timestamp));
}

std::shared_ptr<MatrixView>
BinaryBehaviorDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
BinaryBehaviorDataset::
getColumnIndex() const
{
    return itl;
}

RestRequestMatchResult
BinaryBehaviorDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    return itl->handleRequest(connection, request, context);
}

std::shared_ptr<RowStream>
BinaryBehaviorDataset::
getRowStream() const
{
    return itl->getRowStream();
}

std::shared_ptr<RowValueInfo>
BinaryBehaviorDataset::
getRowInfo() const
{
    return itl->getRowInfo();
}



/******************************************************************************/
/* MUTABLE BINARY BEHAVIOR DATASET CONFIG                                    */
/******************************************************************************/

MutableBinaryBehaviorDatasetConfig::
MutableBinaryBehaviorDatasetConfig() 
    : timeQuantumSeconds(1.0)
{
}

DEFINE_STRUCTURE_DESCRIPTION(MutableBinaryBehaviorDatasetConfig);

MutableBinaryBehaviorDatasetConfigDescription::
MutableBinaryBehaviorDatasetConfigDescription()
{
    nullAccepted = true;
    addParent<BehaviorDatasetConfig>();
    addField("timeQuantumSeconds", &MutableBinaryBehaviorDatasetConfig::timeQuantumSeconds,
             "a number that controls the resolution of timestamps stored in the dataset, "
             "in seconds. 1 means one second, 0.001 means one millisecond, 60 means one minute. "
             "Higher resolution requires more memory to store timestamps.", 1.0);
}

/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET                                                 */
/*****************************************************************************/


MutableBinaryBehaviorDataset::
MutableBinaryBehaviorDataset(MldbServer * owner,
                              PolyConfig config,
                              const ProgressFunc & onProgress)
    : BinaryBehaviorDataset(owner)
{
    ExcAssert(!config.id.empty());

    auto params = config.params.convert<MutableBinaryBehaviorDatasetConfig>();
    address = params.dataFileUrl.toString();
    auto behs = std::make_shared<MutableBehaviorDomain>();
    behs->timeQuantum = params.timeQuantumSeconds;
    itl.reset(new Itl(behs));
}

MutableBinaryBehaviorDataset::
~MutableBinaryBehaviorDataset()
{
}

void
MutableBinaryBehaviorDataset::
recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
{
    ExcAssertNotEqual(rowName, RowPath());

    vector<MutableBehaviorDomain::ManyEntryId> toRecord;
    toRecord.reserve(vals.size());
    for (auto & b: vals) {
        const ColumnPath& name = std::get<0>(b);
        const CellValue& value = std::get<1>(b);

        MutableBehaviorDomain::ManyEntryId entry;
        entry.behavior = toId(name);
        ExcAssertEqual(value, 1);
        entry.timestamp = std::get<2>(b);
        entry.count = 1;

        toRecord.emplace_back(std::move(entry));
    }

    ExcAssert(itl->mutableBehs);

    itl->mutableBehs->recordMany(toId(rowName), &toRecord[0], toRecord.size());
}

void
MutableBinaryBehaviorDataset::
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
                rowNames.push_back(toId(name));

            return index;
        };

    auto getColumnIndex = [&] (const ColumnPath & name)
        {
            ColumnHash ch(name);
            int64_t index
                = columnIndex.insert(make_pair(ch, columnIndex.size())).first->second;

            if (columnNames.size() < columnIndex.size())
                columnNames.push_back(toId(name));
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

            int64_t colIndex = getColumnIndex(name);
            if (value != 1)
                throw HttpReturnException(400, "Binary behavior datasets can only record the value 1", "valueReceived", value);

            MutableBehaviorDomain::ManyEntryIndex entry;
            entry.behIndex = colIndex;
            entry.subjIndex = rowIndex;
            entry.timestamp = ts;

            toRecord.push_back(entry);
        }
    }

    itl->mutableBehs->recordMany(&columnNames[0],
                                 columnNames.size(),
                                 &rowNames[0],
                                 rowNames.size(),
                                 &toRecord[0],
                                 toRecord.size());
}

void
MutableBinaryBehaviorDataset::
commit()
{
    itl->mutableBehs->setFileMetadata("mldbEncoding", "beh.binary");
    itl->mutableBehs->makeImmutable();
    if (!address.empty()) {
        MLDB::makeUriDirectory(address);
        itl->mutableBehs->save(address);
    }
}

namespace {

RegisterDatasetType<BinaryBehaviorDataset, BehaviorDatasetConfig>
regBeh(builtinPackage(),
       "beh.binary",
       "Memory-mappable dataset type to efficiently store binary valued behavioral data",
       "datasets/BinaryBehaviorDataset.md.html");

RegisterDatasetType<MutableBinaryBehaviorDataset, MutableBinaryBehaviorDatasetConfig>
regMutableBeh(builtinPackage(),
              "beh.binary.mutable",
              "Recordable dataset designed to store binary valued behavioral data",
              "datasets/MutableBinaryBehaviorDataset.md.html");

} // file scope

} // namespace MLDB

