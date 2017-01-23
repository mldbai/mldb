/** embedding.cc
    Jeremy Barnes, 9 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Implementation of embedding dataset.
*/

#include "embedding.h"
#include "mldb/ml/tsne/vantage_point_tree.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/set_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/arch/timers.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/bucket.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include <boost/algorithm/clamp.hpp>
#include "mldb/utils/log.h"

using namespace std;



namespace MLDB {

// Defined in stats_table_procedure.cc, until we move them somewhere
// more sensible
ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const PathElement & coord);

ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, PathElement & coord);

ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const Path & coords);

ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, Path & coords);


/*****************************************************************************/
/* EMBEDDING DATASET CONFIG                                                  */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(EmbeddingDatasetConfig);

EmbeddingDatasetConfigDescription::
EmbeddingDatasetConfigDescription()
{
    nullAccepted = true;

    addField("metric", &EmbeddingDatasetConfig::metric,
             "Metric space which is used to index the data for nearest "
             "neighbors calculations.  Options are 'cosine' (which is "
             "good for normalized embeddings like the SVD) and 'euclidean' "
             "(which is good for geometric embeddings like the t-SNE "
             "algorithm).", METRIC_EUCLIDEAN);
}


/*****************************************************************************/
/* EMBEDDING INTERNAL REPRESENTATION                                         */
/*****************************************************************************/

struct EmbeddingDatasetRepr {
    EmbeddingDatasetRepr(MetricSpace metric)
        : vpTree(new ML::VantagePointTreeT<int>()),
          distance(DistanceMetric::create(metric))
    {
    }

    EmbeddingDatasetRepr(std::vector<ColumnPath> columnNames,
                         MetricSpace metric)
        : columnNames(std::move(columnNames)), columns(this->columnNames.size()),
          vpTree(new ML::VantagePointTreeT<int>()),
          distance(DistanceMetric::create(metric))
    {
        for (unsigned i = 0;  i < this->columnNames.size();  ++i) {
            columnIndex[this->columnNames[i]] = i;
        }
    }

    EmbeddingDatasetRepr(const EmbeddingDatasetRepr & other)
        : columnNames(other.columnNames),
          columns(other.columns),
          columnIndex(other.columnIndex),
          rows(other.rows),
          rowIndex(other.rowIndex),
          vpTree(ML::VantagePointTreeT<int>::deepCopy(other.vpTree.get()))
    {
    }

    // Unfortunately, both '0' and 'null' hash to the same thing.  To
    // allow loading with both of these present, we rehash those into
    // different identifiers.
    static const uint64_t nullHashOut = 1;
    static const RowHash nullHashIn; //= RowHash(RowName("null"));
    
    static uint64_t getRowHashForIndex(const RowPath & rowName)
    {
        uint64_t result = RowHash(rowName).hash();
        if (result == nullHashIn) {
            if (rowName.size() == 1 && rowName[0] == PathElement("0"))
                return result;
            ExcAssert(rowName == PathElement("null"));
            result = nullHashOut;
        }
        return result;
    }

    static uint64_t getRowHashForIndex(const RowHash & rowHash)
    {
        return rowHash == nullHashIn ? nullHashOut : rowHash.hash();
    }

    bool initialized() const
    {
        return !columns.empty();
    }

    struct Row {
        Row(RowPath rowName, distribution<float> coords, Date timestamp)
            : rowName(std::move(rowName)), coords(std::move(coords)),
              timestamp(timestamp)
        {
        }

        RowPath rowName;
        distribution<float> coords;
        Date timestamp;

        void serialize(ML::DB::Store_Writer & store) const
        {
            store << rowName.toUtf8String() << coords << timestamp;
        }
    };

    float dist(unsigned row1, unsigned row2) const
    {
        ExcAssertLess(row1, rows.size());
        ExcAssertLess(row2, rows.size());

        if (row1 == row2)
            return 0.0f;
        
        float result = distance->dist(row1, row2,
                                      rows[row1].coords,
                                      rows[row2].coords);
        
        ExcAssert(isfinite(result));
        return result;
    }

    float dist(unsigned row1, const distribution<float> & row2) const
    {
        ExcAssertLess(row1, rows.size());
        ExcAssertEqual(row2.size(), columns.size());
        
        float result = distance->dist(row1, -1,
                                      rows[row1].coords,
                                      row2);
        ExcAssert(isfinite(result));
        return result;
    }
    
    std::pair<Date, Date> getTimestampRange() const
    {
        // TODO: this could be made more efficiently by caching and updating
        // the result...
        bool first = true;
        Date earliest = Date::notADate(), latest = Date::notADate();

        for (auto & r: rows) {
            if (first && r.timestamp.isADate()) {
                earliest = latest = r.timestamp;
                first = false;
            }
            else {
                earliest.setMin(r.timestamp);
                latest.setMax(r.timestamp);
            }
        }

        return { earliest, latest };
    }
    
    std::vector<ColumnPath> columnNames;
    std::vector<std::vector<float> > columns;
    Lightweight_Hash<ColumnHash, int> columnIndex;

    std::vector<Row> rows;
    Lightweight_Hash<uint64_t, int> rowIndex;
    
    std::unique_ptr<ML::VantagePointTreeT<int> > vpTree;
    std::unique_ptr<DistanceMetric> distance;

    void save(const std::string & filename)
    {
        filter_ostream stream(filename);
        ML::DB::Store_Writer store(stream);
        
        serialize(store);

        // Make sure that we saved properly
        stream.close();
    }
    void serialize(ML::DB::Store_Writer & store) const;
};

const RowHash EmbeddingDatasetRepr::nullHashIn(RowPath("null"));

ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const EmbeddingDatasetRepr::Row & row)
{
    row.serialize(store);
    return store;
}

void
EmbeddingDatasetRepr::
serialize(ML::DB::Store_Writer & store) const
{
    store << string("EMBEDDING_DATASET")
          << ML::DB::compact_size_t(1);  // version
    store << columnNames << columns << rows;
    vpTree->serialize(store);
}

struct EmbeddingDataset::Itl
    : public MatrixView, public ColumnIndex {
    Itl(MetricSpace metric)
        : metric(metric), committed(lock, metric), uncommitted(nullptr),
          logger(MLDB::getMldbLog<ProximateVoxelsFunction>())
    {
    }

    // TODO: make it loadable...
    Itl(const std::string & address, MetricSpace metric)
        : metric(metric), committed(lock, metric), uncommitted(nullptr), address(address),
          logger(MLDB::getMldbLog<ProximateVoxelsFunction>())
    {
    }

    ~Itl()
    {
        delete uncommitted.load();
    }

    MetricSpace metric;

    GcLock lock;
    RcuProtected<EmbeddingDatasetRepr> committed;

    //typedef Spinlock Mutex;
    typedef std::mutex Mutex;
    Mutex mutex;
    std::atomic<EmbeddingDatasetRepr *> uncommitted;
    std::string address;

    RestRequestRouter router;

    shared_ptr<spdlog::logger> logger;

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};

        std::vector<RowPath> result;

        if (limit == -1)
            limit = repr->rows.size();

        for (unsigned i = start;  i < repr->rows.size() && i < start + limit;  ++i) {
            result.emplace_back(repr->rows[i].rowName);
        }

        return result;
    }

    struct EmbeddingRowStream : public RowStream {

        EmbeddingRowStream(const EmbeddingDataset::Itl* source) : index(0), source(source)
        {
         
        }

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<EmbeddingRowStream>(source);
            return ptr;
        }

        virtual void initAt(size_t start){
            index = start;
        }

        virtual RowPath next() {
            auto repr = source->committed();     
            return repr->rows[index++].rowName;
        }

        virtual const RowPath & rowName(RowPath & storage) const
        {
            auto repr = source->committed();     
            return repr->rows[index].rowName;
        }

        size_t index;
        const EmbeddingDataset::Itl* source;
    };

    std::shared_ptr<RowStream>
    getRowStream() const
    {
        return std::make_shared<EmbeddingRowStream>(this);
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};

        std::vector<RowHash> result;

        if (limit == -1)
            limit = repr->rows.size();

        for (unsigned i = start;  i < repr->rows.size() && i < start + limit;  ++i) {
            result.emplace_back(repr->rows[i].rowName);
        }

        return result;
    }

    virtual bool knownRow(const RowPath & rowName) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return false;
        auto it = repr->rowIndex.find(EmbeddingDatasetRepr::getRowHashForIndex(rowName));
        return it != repr->rowIndex.end() && it->second != -1;
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        auto repr = committed();
        if (!repr->initialized())
            return false;
        auto it = repr->rowIndex.find(EmbeddingDatasetRepr::getRowHashForIndex(rowHash));
        return it != repr->rowIndex.end() && it->second != -1;
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return MatrixNamedRow();

        auto it = repr->rowIndex.find(EmbeddingDatasetRepr::getRowHashForIndex(rowName));
        if (it == repr->rowIndex.end() || it->second == -1)
            return MatrixNamedRow();
        
        const EmbeddingDatasetRepr::Row & row = repr->rows[it->second];

        if (row.rowName != rowName)
            return MatrixNamedRow();

        MatrixNamedRow result;
        result.rowHash = result.rowName = rowName;
        result.columns.reserve(row.coords.size());

        for (unsigned i = 0;  i < row.coords.size();  ++i) {
            result.columns.emplace_back(repr->columnNames[i], row.coords[i],
                                        row.timestamp);
        }
        return result;
    }

    virtual MatrixRow getRowByHash(const RowHash & rowHash) const
    {
        auto repr = committed();
        if (!repr->initialized())
            return MatrixRow();

        auto it = repr->rowIndex.find(EmbeddingDatasetRepr::getRowHashForIndex(rowHash));
        if (it == repr->rowIndex.end() || it->second == -1)
            return MatrixRow();
        
        const EmbeddingDatasetRepr::Row & row = repr->rows[it->second];

        MatrixRow result;
        result.rowHash = rowHash;
        result.rowName = row.rowName;
        result.columns.reserve(row.coords.size());

        for (unsigned i = 0;  i < row.coords.size();  ++i) {
            result.columns.emplace_back(repr->columnNames[i], row.coords[i],
                                        row.timestamp);
        }
        return result;
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const override
    {
        static const RowHash nullHash(RowPath("null"));
        static const RowHash nullHashMunged(RowPath("\x01null"));
        if (rowHash == nullHash)
            return getRowPath(nullHashMunged);

        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "unknown row");

        auto it = repr->rowIndex.find(EmbeddingDatasetRepr::getRowHashForIndex(rowHash));
        if (it == repr->rowIndex.end() || it->second == -1)
            throw HttpReturnException(400, "unknown row");

        return repr->rows[it->second].rowName;
    }

    virtual bool knownColumn(const ColumnPath & column) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get name of unknown column from uninitialized embedding");

        return repr->columnIndex.count(column);
    }

    virtual ColumnPath getColumnPath(ColumnHash column) const override
    {
        // TODO: shouldn't need to
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get name of unknown column from uninitialized embedding");

        auto it = repr->columnIndex.find(column);
        if (it == repr->columnIndex.end())
            throw HttpReturnException(400, "Can't get name of unknown column");
        return repr->columnNames[it->second];
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnPaths() const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};
        
        return repr->columnNames;
    }

    virtual size_t getRowCount() const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return 0;
        return repr->rows.size();
    }

    virtual size_t getColumnCount() const override
    {
        auto repr = committed();
        if (!repr->initialized())
            return 0;
        return repr->columnNames.size();
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnPath & ch, ColumnStats & toStoreResult) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get stats of unknown column");

        auto it = repr->columnIndex.find(ch);
        if (it == repr->columnIndex.end())
            throw HttpReturnException(400, "Can't get name of unknown column");

        const vector<float> & columnVals = repr->columns.at(it->second);

        toStoreResult.isNumeric_ = true;
        toStoreResult.atMostOne_ = true;
        toStoreResult.rowCount_ = columnVals.size();

        for (auto & v: columnVals) {
            toStoreResult.values[v].rowCount_ += 1;
        }

        return toStoreResult;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnPath & column) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get unknown column");


        auto it = repr->columnIndex.find(column);
        if (it == repr->columnIndex.end())
            throw HttpReturnException(400, "Can't get name of unknown column");

        const vector<float> & columnVals = repr->columns.at(it->second);

        MatrixColumn result;

        result.columnHash = result.columnName = column;

        for (unsigned i = 0;  i < columnVals.size();  ++i) {
            result.rows.emplace_back(repr->rows[i].rowName, columnVals[i],
                                     repr->rows[i].timestamp);
        }
        return result;
    }

    virtual std::vector<CellValue>
    getColumnDense(const ColumnPath & column) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get unknown column");

        auto it = repr->columnIndex.find(column);
        if (it == repr->columnIndex.end())
            throw HttpReturnException(400, "Can't get name of unknown column");

        const vector<float> & columnVals = repr->columns.at(it->second);

        std::vector<CellValue> result(columnVals.begin(), columnVals.end());

        return result;
    }

    virtual std::tuple<BucketList, BucketDescriptions>
    getColumnBuckets(const ColumnPath & column,
                     int maxNumBuckets) const override
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get unknown column");

        auto it = repr->columnIndex.find(column);
        if (it == repr->columnIndex.end())
            throw HttpReturnException(400, "Can't get name of unknown column");

        const vector<float> & columnVals = repr->columns.at(it->second);
        auto sortedVals = columnVals;
        std::sort(sortedVals.begin(), sortedVals.end());
        sortedVals.erase(std::unique(sortedVals.begin(), sortedVals.end()),
                         sortedVals.end());
        
        std::vector<CellValue> valueList(sortedVals.begin(), sortedVals.end());

        BucketDescriptions descriptions;
        descriptions.initialize(valueList, maxNumBuckets);

        // Finally, perform the bucketed lookup
        WritableBucketList buckets(columnVals.size(), descriptions.numBuckets());

        for (auto& v : columnVals) {
            uint32_t bucket = descriptions.getBucket(v);
            buckets.write(bucket);
        }

        return std::make_tuple(std::move(buckets), std::move(descriptions));
    }

    /** Return a RowValueInfo that describes all rows that could be returned
        from the dataset.
    */
    virtual std::shared_ptr<RowValueInfo> getRowInfo() const
    {
        std::vector<KnownColumn> knownColumns;

        auto valueInfo = std::make_shared<Float32ValueInfo>();
        
        auto repr = committed();
        if (repr->initialized()) {
            for (size_t i = 0;  i < repr->columnNames.size();  ++i) {
                knownColumns.emplace_back(repr->columnNames[i], valueInfo,
                                          COLUMN_IS_DENSE, i /* fixed index */);
            }
        }

        return std::make_shared<RowValueInfo>(knownColumns);
    }

    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const
    {
        auto repr = committed();
        if (!repr->initialized()) {
            throw HttpReturnException(400, "Can't get info of unknown column",
                                      "columnName", columnName,
                                      "knownColumns", repr->columnNames);
        }
        
        auto it = repr->columnIndex.find(columnName);
        if (it == repr->columnIndex.end())
            throw HttpReturnException(400, "Can't get info of unknown column",
                                      "columnName", columnName,
                                      "knownColumns", repr->columnNames);
        
        return KnownColumn(columnName, std::make_shared<Float32ValueInfo>(),
                           COLUMN_IS_DENSE);
    }
    

    virtual std::vector<KnownColumn>
    getKnownColumnInfos(const std::vector<ColumnPath> & columnNames) const
    {
        std::vector<KnownColumn> result;

        if (columnNames.empty())
            return result;

        auto repr = committed();
        if (!repr->initialized()) {
            throw HttpReturnException(500, "Asking for column information in uncommitted embedding dataset");
        }

        result.reserve(columnNames.size());

        for (auto & columnName: columnNames) {
            auto it = repr->columnIndex.find(columnName);
            if (it == repr->columnIndex.end())
                throw HttpReturnException(400, "Can't get info of unknown column",
                                          "columnName", columnName,
                                          "knownColumns", repr->columnNames);
            
            result.emplace_back(columnName,
                                std::make_shared<Float32ValueInfo>(),
                                COLUMN_IS_DENSE);
        }

        return result;
    }

    virtual void
    recordEmbedding(const std::vector<ColumnPath> & columnNames,
                    const std::vector<std::tuple<RowPath, std::vector<float>, Date> > & rows)
    {
        auto repr = committed();
        std::unique_lock<Mutex> guard(mutex);

        // If this is the first commit, then create a new structure with the
        // column names now that we know them
        if (!uncommitted) {
            if (!repr->initialized()) {
                // First commit; we just learnt the column names
                uncommitted = new EmbeddingDatasetRepr(columnNames, metric);
            }
            else {
                uncommitted = new EmbeddingDatasetRepr(*repr);
            }
        }
        else {
            if (columnNames.size() != uncommitted.load()->columnNames.size())
                throw HttpReturnException(400, "Attempt to change number of columns in embedding dataset");
            if (columnNames != uncommitted.load()->columnNames)
                throw HttpReturnException(400, "Attempt to change column names in embedding dataset");
        }

        for (auto & r: rows) {
            const RowPath & rowName = std::get<0>(r);
            uint64_t rowHash = EmbeddingDatasetRepr::getRowHashForIndex(rowName);

            const auto & vec = std::get<1>(r);
            distribution<float> embedding(vec.begin(), vec.end());
            Date ts = std::get<2>(r);

            int index = (*uncommitted).rows.size();

            if (!(*uncommitted).rowIndex.insert({ rowHash, index }).second) {
                if ((*uncommitted).rowIndex[rowHash] == -1)
                    (*uncommitted).rowIndex[rowHash] = index;
                else {
                                                
                    DEBUG_MSG(logger) << "rowName = " << rowName;
                    DEBUG_MSG(logger) << "rowHash = " << RowHash(rowName);
                    // Check if it's a double record or a hash collision
                    RowPath oldName
                        = (*uncommitted).rows.at((*uncommitted).rowIndex[rowHash])
                        .rowName;
                    if (oldName == rowName)
                        throw HttpReturnException
                            (400, "Row '" + rowName.toUtf8String()
                             + "' has already been recorded into embedding dataset.  "
                             + "Are you re-using an output dataset (1)?");
                    else {
                        throw HttpReturnException
                            (400, "Row '" + rowName.toUtf8String() + "' and '"
                             + oldName.toUtf8String() + "' both hash to '"
                             + RowHash(rowName).toString() + "' (hash collision). "
                             + "You may be able to modify your names to avoid the collision.");
                    }
                }
            }

            size_t numRowsBefore = (*uncommitted).rows.size();
        
            try {
                // Update the row
                (*uncommitted).rows.emplace_back(rowName, std::move(embedding),
                                                 ts);
                (*uncommitted).distance->addRow(numRowsBefore,
                                                (*uncommitted).rows.back().coords);
            } catch (const std::exception & exc) {
                // If there is an exception, keep the data structure consistent
                (*uncommitted).rowIndex[rowHash] = -1;
                if ((*uncommitted).rows.size() > numRowsBefore)
                    (*uncommitted).rows.pop_back();
                throw;
            }        
        }
    }

    virtual void
    recordRowItl(const RowPath & rowName,
                 const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        auto repr = committed();

        uint64_t rowHash = EmbeddingDatasetRepr::getRowHashForIndex(rowName);

        // Hash the columns before the lock is taken
        PossiblyDynamicBuffer<ColumnHash> columnHashesBuf(vals.size());
        ColumnHash * columnHashes = columnHashesBuf.data();

        for (size_t i = 0;  i < vals.size();  ++i) {
            columnHashes[i] = ColumnHash(std::get<0>(vals[i]));
        }

        Date latestDate = Date::negativeInfinity();

        distribution<float> embedding;

        // Do it here before we acquire the lock in the case that it's initalized
        if (uncommitted) {
            embedding.resize((*uncommitted).columns.size(),
                             std::numeric_limits<float>::quiet_NaN());

            for (size_t i = 0;  i < vals.size();  ++i) {
                auto & v = vals[i];
                auto it = (*uncommitted).columnIndex.find(columnHashes[i]);
                if (it == (*uncommitted).columnIndex.end())
                    throw HttpReturnException(400, "Couldn't extract column with name");
                
                embedding[it->second] = std::get<1>(v).toDouble();
                latestDate.setMax(std::get<2>(v));
            }
        }

        std::unique_lock<Mutex> guard(mutex);

        // If this is the first commit, then create a new structure with the
        // column names now that we know them
        if (!uncommitted) {
            if (!repr->initialized()) {
                // First commit; we just learnt the column names

                // Here is our list of columns
                std::vector<ColumnPath> columnNames;
                for (auto & c: vals) {
                    columnNames.push_back(std::get<0>(c));
                }
                
                //DEBUG_MSG(logger) << "columnNames = " << columnNames;
                
                uncommitted = new EmbeddingDatasetRepr(columnNames, metric);
            }
            else {
                uncommitted = new EmbeddingDatasetRepr(*repr);
            }

            embedding.resize(repr->columns.size(),
                             std::numeric_limits<float>::quiet_NaN());
        }

        if (embedding.empty()) {
            DEBUG_MSG(logger) << "embedding is empty";
            DEBUG_MSG(logger) << "repr->initialized = " << repr->initialized();
            embedding.resize((*uncommitted).columns.size(),
                             std::numeric_limits<float>::quiet_NaN());

            for (size_t i = 0;  i < vals.size();  ++i) {
                auto & v = vals[i];
                auto it = (*uncommitted).columnIndex.find(columnHashes[i]);
                if (it == (*uncommitted).columnIndex.end())
                    throw HttpReturnException(400, "Couldn't extract column with name 2 "
                                        + std::get<0>(v).toUtf8String());
                
                embedding[it->second] = std::get<1>(v).toDouble();
                latestDate.setMax(std::get<2>(v));
            }
        }

        // If this is the first record, then we need to count the number of
        // rows and set everything up.

        int index = (*uncommitted).rows.size();

        if (!(*uncommitted).rowIndex.insert({ rowHash, index }).second) {
            if ((*uncommitted).rowIndex[rowHash] == -1)
                (*uncommitted).rowIndex[rowHash] = index;
            else {
                DEBUG_MSG(logger) << "rowName = " << rowName;
                DEBUG_MSG(logger) << "rowHash = " << RowHash(rowName);
                // Check if it's a double record or a hash collision
                const RowPath & oldName
                    = (*uncommitted).rows.at((*uncommitted).rowIndex[rowHash])
                    .rowName;
                if (oldName == rowName)
                    throw HttpReturnException
                        (400, "Row '" + rowName.toUtf8String()
                         + "' has already been recorded into embedding dataset.  "
                         + "Are you re-using an output dataset (2)?");
                else {
                    return;
                    throw HttpReturnException
                        (400, "Row '" + rowName.toUtf8String() + "' and '"
                         + oldName.toUtf8String() + "' both hash to '"
                         + RowHash(rowName).toString() + "' (hash collision). "
                         + "You may be able to modify your names to avoid the collision.");
                }
            }
        }

        size_t numRowsBefore = (*uncommitted).rows.size();
        
        try {
            // Update the row
            (*uncommitted).rows.emplace_back(rowName, std::move(embedding),
                                             latestDate);
            (*uncommitted).distance->addRow(numRowsBefore,
                                            (*uncommitted).rows.back().coords);
        } catch (const std::exception & exc) {
            // If there is an exception, keep the data structure consistent
            (*uncommitted).rowIndex[rowHash] = -1;
            if ((*uncommitted).rows.size() > numRowsBefore)
                (*uncommitted).rows.pop_back();
            throw;
        }        
    }

    virtual void commit()
    {
        std::unique_lock<Mutex> guard(mutex);

        INFO_MSG(logger) << "committing embedding dataset";

        if (!uncommitted)
            return;

        for (unsigned j = 0;  j < (*uncommitted).columns.size();  ++j)
            (*uncommitted).columns[j].resize((*uncommitted).rows.size());

        // Create the column index; this is a standard matrix inversion
        auto indexRow = [&] (size_t i)
            {
                for (unsigned j = 0;  j < (*uncommitted).columns.size();  ++j)
                    (*uncommitted).columns[j][i] = (*uncommitted).rows[i].coords[j];
            };

        parallelMap(0, (*uncommitted).rows.size(), indexRow);

        // Create the vantage point tree
        INFO_MSG(logger) << "creating vantage point tree";
        Timer timer;
        
        std::vector<int> items;
        for (unsigned i = 0;  i < (*uncommitted).rows.size();  ++i) {
            items.push_back(i);
        }

        // Function used to build the VP tree, that scans all of the items in
        // parallel.
        auto dist = [&] (int item, const std::vector<int> & items, int depth)
            -> distribution<float>
            {
                ExcAssertLessEqual(depth, 100);  // 2^100 items is enough

                distribution<float> result(items.size());

                auto doItem = [&] (int n)
                {
                    int i = items[n];

                    result[n] = (*uncommitted).dist(item, i);

                    if (item == i)
                        ExcAssertEqual(result[n], 0.0);

                    if (!isfinite(result[n])) {
                        INFO_MSG(logger) << "dist between " << i << " and " << item << " is "
                             << result.back();
                    }
                    ExcAssert(isfinite(result[n]));
                };

                if (items.size() < 10000 || depth > 2) {
                    for (unsigned n = 0;  n < items.size();  ++n)
                        doItem(n);
                }
                else parallelMap(0, items.size(), doItem);
                
                return result;
            };
        
        // Create the VP tree for indexed lookups on distance
        (*uncommitted).vpTree.reset(ML::VantagePointTreeT<int>::createParallel(items, dist));

        INFO_MSG(logger) << "VP tree done in " << timer.elapsed();
        
        committed.replace(uncommitted);
        uncommitted = nullptr;

        if (!address.empty()) {
            INFO_MSG(logger) << "saving embedding";
            committed()->save(address);
        }
    }

    vector<tuple<RowPath, RowHash, float> >
    getNeighbors(const distribution<float> & coord,
                 int numNeighbors,
                 double maxDistance)
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};

        auto dist = [&] (int item) -> float
        {
            float result = repr->dist(item, coord);
            ExcAssert(isfinite(result));
            return result;
        };

        //Timer timer;

        auto neighbors = repr->vpTree->search(dist, numNeighbors, maxDistance);

        //DEBUG_MSG(logger) << "neighbors took " << timer.elapsed();

        DEBUG_MSG(logger) << "neighbors = " << jsonEncode(neighbors);
        
        vector<tuple<RowPath, RowHash, float> > result;
        for (auto & n: neighbors) {
            result.emplace_back(repr->rows[n.second].rowName,
                                repr->rows[n.second].rowName,
                                n.first);
        }

        return result;
    }

    vector<tuple<RowPath, RowHash, float> >
    getRowNeighbors(const RowPath & row, int numNeighbors, double maxDistance)
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};

        uint64_t rowHash = EmbeddingDatasetRepr::getRowHashForIndex(row);

        auto it = repr->rowIndex.find(rowHash);
        if (it == repr->rowIndex.end() || it->second == -1) {
            throw HttpReturnException(400, "Couldn't find row '" + row.toUtf8String()
                                      + "' in embedding");
        }
       
        //const EmbeddingDatasetRepr::Row & row = repr->rows[it->second];
        
        auto dist = [&] (int item) -> float
            {
                float result = repr->dist(item, it->second);
                ExcAssert(isfinite(result));
                return result;
            };

        auto neighbors = repr->vpTree->search(dist, numNeighbors, maxDistance);

        vector<tuple<RowPath, RowHash, float> > result;
        for (auto & n: neighbors) {
            result.emplace_back(repr->rows[n.second].rowName,
                                repr->rows[n.second].rowName,
                                n.first);
        }

        return result;
    }

    std::pair<Date, Date> getTimestampRange() const
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get stats of unknown column");

        return repr->getTimestampRange();
    }
};


/*****************************************************************************/
/* EMBEDDING                                                                 */
/*****************************************************************************/

EmbeddingDataset::
EmbeddingDataset(MldbServer * owner,
                 PolyConfig config,
                 const ProgressFunc & onProgress)
    : Dataset(owner)
{
    this->datasetConfig = config.params.convert<EmbeddingDatasetConfig>();
#if 1
    itl.reset(new Itl(datasetConfig.metric));
#else // once persistence is done

    if (!config.address.empty()) {
        itl.reset(new Itl(config.address));
    }
    else {
        itl.reset(new Itl());
    }
#endif
}
    
EmbeddingDataset::
~EmbeddingDataset()
{
}

Any
EmbeddingDataset::
getStatus() const
{
    return Any();
}

void
EmbeddingDataset::
recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
{
    return itl->recordRowItl(rowName, vals);
}

void
EmbeddingDataset::
recordEmbedding(const std::vector<ColumnPath> & columnNames,
                const std::vector<std::tuple<RowPath, std::vector<float>, Date> > & rows)
{
    itl->recordEmbedding(columnNames, rows);
}

void
EmbeddingDataset::
commit()
{
    return itl->commit();
}
    
std::pair<Date, Date>
EmbeddingDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

Date
EmbeddingDataset::
quantizeTimestamp(Date timestamp) const
{
    return timestamp;
}

std::shared_ptr<MatrixView>
EmbeddingDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
EmbeddingDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
EmbeddingDataset::
getRowStream() const
{
    return itl->getRowStream();
}

BoundFunction
EmbeddingDataset::
overrideFunction(const Utf8String & tableName,
                 const Utf8String & functionName,
                 SqlBindingScope & context) const
{
    return BoundFunction();
}

vector<tuple<RowPath, RowHash, float> >
EmbeddingDataset::
getNeighbors(const distribution<float> & coord, int numNeighbors, double maxDistance) const
{
    return itl->getNeighbors(coord, numNeighbors, maxDistance);
}
    
vector<tuple<RowPath, RowHash, float> >
EmbeddingDataset::
getRowNeighbors(const RowPath & row, int numNeighbors, double maxDistance) const
{
    return itl->getRowNeighbors(row, numNeighbors, maxDistance);
}

KnownColumn
EmbeddingDataset::
getKnownColumnInfo(const ColumnPath & columnName) const
{
    return itl->getKnownColumnInfo(columnName);
}

std::vector<KnownColumn>
EmbeddingDataset::
getKnownColumnInfos(const std::vector<ColumnPath> & columnNames) const
{
    return itl->getKnownColumnInfos(columnNames);
}

std::shared_ptr<RowValueInfo>
EmbeddingDataset::
getRowInfo() const
{
    return itl->getRowInfo();
}

static RegisterDatasetType<EmbeddingDataset, EmbeddingDatasetConfig>
regEmbedding(builtinPackage(),
             "embedding",
             "Dataset to record a set of coordinates per row",
             "datasets/EmbeddingDataset.md.html");


/*****************************************************************************/
/* NEAREST NEIGHBOUR FUNCTION                                                */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(NearestNeighborsFunctionConfig);

NearestNeighborsFunctionConfigDescription::
NearestNeighborsFunctionConfigDescription()
{
    addField("defaultNumNeighbors",
             &NearestNeighborsFunctionConfig::defaultNumNeighbors,
             "Default number of neighbors to return. This can be overwritten "
             "when calling the function.", unsigned(10));
    addField("defaultMaxDistance",
             &NearestNeighborsFunctionConfig::defaultMaxDistance,
             "Default value for `maxDistance` parameter to function if not "
             "specified in the function call.  This can be overridden on "
             "a call-by-call basis.",
             double(INFINITY));
    addField("dataset", &NearestNeighborsFunctionConfig::dataset,
             "Embedding dataset in which to find neighbors.  This must be a "
             "dataset of type `embedding`.");
    addField("columnName", &NearestNeighborsFunctionConfig::columnName,
             "The column name within the embedding dataset to use to match "
             "values against.  This must match the columns within the dataset "
             "referred to in the `dataset` parameter.  "
             "In the case that the embedding contains values from multiple "
             "columns instead of a single embedding (in other words, they "
             "are not of the format `columnName.0, columnName.1, ...` but "
             "instead look like `name1, name2, ...`), then pass "
             "in `[]` which signifies use all columns (and is the default).",
             ColumnPath());
}

NearestNeighborsInput::
NearestNeighborsInput()
{
}

DEFINE_STRUCTURE_DESCRIPTION(NearestNeighborsInput);

NearestNeighborsInputDescription::
NearestNeighborsInputDescription()
{
    addField("numNeighbors", &NearestNeighborsInput::numNeighbors,
             "Number of neighbors to find (-1, which is the default, will "
             "use the value in the config", CellValue());
    addField("maxDistance", &NearestNeighborsInput::maxDistance,
             "Maximum distance to accept.  Passing null will use the "
             "value in the config", CellValue());
    addField("coords", &NearestNeighborsInput::coords,
             "Coordinates of the value whose neighbors are being sought, "
             "or alternatively the `rowName` of the value in the underlying "
             "dataset whose neighbors are being sought");
}

DEFINE_STRUCTURE_DESCRIPTION(NearestNeighborsOutput);

NearestNeighborsOutputDescription::
NearestNeighborsOutputDescription()
{
    addField("neighbors", &NearestNeighborsOutput::neighbors,
             "Row containing the row names of the nearest neighbors in rank order");
    addField("distances", &NearestNeighborsOutput::distances,
             "Row containing the nearest neighbors, each with its distance");
}

NearestNeighborsFunction::
NearestNeighborsFunction(MldbServer * owner,
                         PolyConfig config,
                         const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<NearestNeighborsFunctionConfig>();
}

NearestNeighborsFunction::
~NearestNeighborsFunction()
{
}

struct NearestNeighborsFunctionApplier
    : public FunctionApplierT<NearestNeighborsInput, NearestNeighborsOutput> {

    NearestNeighborsFunctionApplier(const Function * owner)
        : FunctionApplierT<NearestNeighborsInput, NearestNeighborsOutput>(owner)
    {
        info = owner->getFunctionInfo();
    }

    std::shared_ptr<EmbeddingDataset> embeddingDataset;

    /// This is used to extract an embedding in the right column order from
    /// the ExpressionValue passed into the coords pin of the function.
    ExpressionValueInfo::ExtractDoubleEmbeddingFunction getEmbeddingFromExpr;
};

NearestNeighborsOutput
NearestNeighborsFunction::
applyT(const ApplierT & applier_, NearestNeighborsInput input) const
{
    auto & applier = static_cast<const NearestNeighborsFunctionApplier &>(applier_);
    
    const ExpressionValue & inputRow = input.coords;

    unsigned numNeighbors = functionConfig.defaultNumNeighbors;
    double maxDistance = functionConfig.defaultMaxDistance;

    if(!input.numNeighbors.empty())
        numNeighbors = input.numNeighbors.toInt();

    if(!input.maxDistance.empty())
        maxDistance = input.maxDistance.toDouble();
    
    Date ts;
    vector<tuple<RowPath, RowHash, float> > neighbors;
    if (inputRow.isAtom()) {
        neighbors = applier.embeddingDataset
            ->getRowNeighbors(RowPath(inputRow.toUtf8String()),
                               numNeighbors, maxDistance);
    }
    else if(inputRow.isEmbedding() || inputRow.isRow()) {
        auto embedding = applier.getEmbeddingFromExpr(inputRow);
        neighbors = applier.embeddingDataset
            ->getNeighbors(embedding.cast<float>(), numNeighbors, maxDistance);
    }
    else {
        throw MLDB::Exception("Input row must be either a row name or an embedding");
    }

    std::vector<CellValue> neighborsOut;
    RowValue distances;

    distances.reserve(neighbors.size());
    neighborsOut.reserve(neighbors.size());
    for(auto & neighbor : neighbors) {
        distances.emplace_back(get<0>(neighbor), get<2>(neighbor), ts);
        neighborsOut.emplace_back(std::move(std::get<0>(neighbor)));
    }

    return {ExpressionValue(std::move(neighborsOut), ts),
            ExpressionValue(std::move(distances))};
}
    
std::unique_ptr<FunctionApplierT<NearestNeighborsInput, NearestNeighborsOutput> >
NearestNeighborsFunction::
bindT(SqlBindingScope & outerContext,
      const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<NearestNeighborsFunctionApplier> result
        (new NearestNeighborsFunctionApplier(this));

    auto boundDataset = functionConfig.dataset->bind(outerContext, nullptr /*onProgress*/);
    if (!boundDataset.dataset) {
        throw HttpReturnException
            (400, "Nearest neighbors function cannot operate on the output of "
             "a table expression, only dataset of type embedding (passed "
             "dataset was '" + functionConfig.dataset->surface + "'");
    }
    
    std::shared_ptr<ExpressionValueInfo> datasetInput
        = boundDataset.dataset->getRowInfo();
    vector<ColumnPath> columnNames
        = datasetInput->allColumnNames();

    // Remove the columnName from these columns, to allow us to get the actual
    // embedding
    vector<ColumnPath> reducedColumnNames;

    if (!functionConfig.columnName.empty()) {
        for (auto & c: columnNames) {
            if (c.startsWith(functionConfig.columnName)) {
                ColumnPath tail = c.removePrefix(functionConfig.columnName);
                if (tail.size() != 1 || !tail.at(0).isIndex()) {
                    throw HttpReturnException
                        (400, "The column name passed into the embedding.neighbors "
                         "function is not a simple embedding, as it contains the "
                         "column '" + c.toUtf8String() +"' inside the dataset '"
                         + functionConfig.dataset->surface + "'.");
                }
                reducedColumnNames.emplace_back(std::move(tail));
            }
        }
        
        if (reducedColumnNames.empty()) {
            std::set<ColumnPath> knownEmbeddings;

            for (auto & c: columnNames) {
                if (!c.empty() || c.back().isIndex()) {
                    ColumnPath knownEmbedding(c.begin(), c.end() - 1);
                    knownEmbeddings.insert(knownEmbedding);
                }
            }

            throw HttpReturnException
                (400, "The column name '" + functionConfig.columnName.toUtf8String()
                 + "' passed into the embedding.neighbors function "
                 + "does not exist inside the dataset '"
                 + functionConfig.dataset->surface + "'",
                 "knownColumnNames", columnNames,
                 "knownEmbeddings", knownEmbeddings);
        }
    }
    else {
        reducedColumnNames = columnNames;
    }

    if (input.size() != 1)
        throw HttpReturnException
            (400, "nearest neighbours function takes a single input");
    
    auto & coordInput = *input.at(0);//input.getValueInfo("coords").getExpressionValueInfo();
    if (coordInput.couldBeRow()) {
        result->getEmbeddingFromExpr
            = coordInput.extractDoubleEmbedding(reducedColumnNames);
    }

    result->embeddingDataset
        = dynamic_pointer_cast<EmbeddingDataset>(boundDataset.dataset);
    if (!result->embeddingDataset) {
        throw HttpReturnException
            (400, "A dataset of type embedding needs to be provided for "
             "the nearest.neighbors function; the provided dataset '"
             + functionConfig.dataset->surface + "' is of type '"
             + MLDB::type_name(*boundDataset.dataset) + "'");
    }
 
    return std::move(result);
}

static RegisterFunctionType<NearestNeighborsFunction, NearestNeighborsFunctionConfig>
regNearestNeighborsFunction(builtinPackage(),
                            "embedding.neighbors",
                            "Return the nearest neighbors of a known row in an embedding dataset",
                            "functions/NearestNeighborsFunction.md.html");

/*****************************************************************************/
/* Read Pixels function                                                      */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ReadPixelsFunctionConfig);

ReadPixelsFunctionConfigDescription::
ReadPixelsFunctionConfigDescription()
{
    addField("expression",
             &ReadPixelsFunctionConfig::expression,
             "SQL Expression that will evaluate to the embedding we want to provide access to");           
}

ReadPixelsInput::
ReadPixelsInput()
{
}

DEFINE_STRUCTURE_DESCRIPTION(ReadPixelsInput);

ReadPixelsInputDescription::
ReadPixelsInputDescription()
{
    addField("x", &ReadPixelsInput::x,
             "X coordinate to fetch in the embedding");
    addField("y", &ReadPixelsInput::y,
             "Y coordinate to fetch in the embedding");
}

DEFINE_STRUCTURE_DESCRIPTION(ReadPixelsOutput);

ReadPixelsOutputDescription::
ReadPixelsOutputDescription()
{
    addField("value", &ReadPixelsOutput::value,
             "Value in the embedding at the specified 2d position");
}

ReadPixelsFunction::
ReadPixelsFunction(MldbServer * owner,
                         PolyConfig config,
                         const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<ReadPixelsFunctionConfig>();
    SqlExpressionMldbScope context(owner);
    auto boundExpr = functionConfig.expression->bind(context);
    SqlRowScope scope;
    embedding = boundExpr(scope, GET_ALL);
    shape = embedding.getEmbeddingShape();
}

ReadPixelsFunction::
~ReadPixelsFunction()
{
}

struct ReadPixelsFunctionApplier
    : public FunctionApplierT<ReadPixelsInput, ReadPixelsOutput> {

    ReadPixelsFunctionApplier(const Function * owner)
        : FunctionApplierT<ReadPixelsInput, ReadPixelsOutput>(owner)
    {
        info = owner->getFunctionInfo();
    }
};

ReadPixelsOutput
ReadPixelsFunction::
applyT(const ApplierT & applier_, ReadPixelsInput input) const
{   
    ReadPixelsOutput output;    

    //We clamp, we do not currently provide interpolation
    int x = input.x.coerceToInteger().toInt();
    int y = input.y.coerceToInteger().toInt();
    x = boost::algorithm::clamp(x, 0, shape[0]-1);
    y = boost::algorithm::clamp(y, 0, shape[1]-1);

    ColumnPath columnPath;
    columnPath = columnPath + PathElement(y);
    columnPath = columnPath + PathElement(x);

    ExpressionValue storage;
    auto pValue = embedding.tryGetNestedColumn(columnPath, storage);

    if (pValue)
        return {*pValue};
    else
        return {ExpressionValue(0, Date::negativeInfinity())};
}
    
std::unique_ptr<FunctionApplierT<ReadPixelsInput, ReadPixelsOutput> >
ReadPixelsFunction::
bindT(SqlBindingScope & outerContext,
      const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<ReadPixelsFunctionApplier> result
        (new ReadPixelsFunctionApplier(this));
 
    return std::move(result);
}

static RegisterFunctionType<ReadPixelsFunction, ReadPixelsFunctionConfig>
regReadPixelsFunction(builtinPackage(),
                            "image.readpixels",
                            "Wraps access to a 2d embedding",
                            "functions/ReadPixelsFunction.md.html");

/*****************************************************************************/
/* Proximate Voxels Function                                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ProximateVoxelsFunctionConfig);

ProximateVoxelsFunctionConfigDescription::
ProximateVoxelsFunctionConfigDescription()
{
    addField("expression",
             &ProximateVoxelsFunctionConfig::expression,
             "SQL Expression that will evaluate to the embedding we want to provide access to");
    addField("range",
             &ProximateVoxelsFunctionConfig::range,
             "Semi axis range we want to consider in 3 dimensions");
}

ProximateVoxelsInput::
ProximateVoxelsInput()
{
}

DEFINE_STRUCTURE_DESCRIPTION(ProximateVoxelsInput);

ProximateVoxelsInputDescription::
ProximateVoxelsInputDescription()
{
    addField("x", &ProximateVoxelsInput::x,
             "X coordinate to fetch in the embedding");
    addField("y", &ProximateVoxelsInput::y,
             "Y coordinate to fetch in the embedding");
    addField("z", &ProximateVoxelsInput::z,
             "Z coordinate to fetch in the embedding");
}

DEFINE_STRUCTURE_DESCRIPTION(ProximateVoxelsOutput);

ProximateVoxelsOutputDescription::
ProximateVoxelsOutputDescription()
{
    addField("value", &ProximateVoxelsOutput::value,
             "");
}

ProximateVoxelsFunction::
ProximateVoxelsFunction(MldbServer * owner,
                         PolyConfig config,
                         const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<ProximateVoxelsFunctionConfig>();
    N = functionConfig.range;
    SqlExpressionMldbScope context(owner);
    auto boundExpr = functionConfig.expression->bind(context);
    SqlRowScope scope;
    embedding = boundExpr(scope, GET_ALL);
}

ProximateVoxelsFunction::
~ProximateVoxelsFunction()
{
}

struct ProximateVoxelsFunctionApplier
    : public FunctionApplierT<ProximateVoxelsInput, ProximateVoxelsOutput> {

    ProximateVoxelsFunctionApplier(const Function * owner)
        : FunctionApplierT<ProximateVoxelsInput, ProximateVoxelsOutput>(owner)
    {
        info = owner->getFunctionInfo();
    }
};

ProximateVoxelsOutput
ProximateVoxelsFunction::
applyT(const ApplierT & applier_, ProximateVoxelsInput input) const
{
    
    ProximateVoxelsOutput output;

    int x = input.x.coerceToInteger().toInt();
    int y = input.y.coerceToInteger().toInt();
    int z = input.z.coerceToInteger().toInt();
    
    auto shape = embedding.getEmbeddingShape();

    size_t numChannels = shape.back();
    const size_t num_values = (N*2+1)*(N*2+1)*(N*2+1)*numChannels;

    std::shared_ptr<float> buffer(new float[num_values],
                                  [] (float * p) { delete[] p; });

    float* p = buffer.get();
    for (int i = -N; i <= N; ++i) {
        for (int j = -N; j <= N; ++j) {
            for (int k = -N; k <= N; ++k) {
                for (int c = 0; c < 3; ++c) {

                    int ii = x + i;
                    int jj = y + j;
                    int kk = z + k;

                    ColumnPath columnPath;
                  
                    //voxelize is z, y, x...

                    columnPath = columnPath + PathElement(kk);
                    columnPath = columnPath + PathElement(jj);
                    columnPath = columnPath + PathElement(ii);
                    columnPath = columnPath + PathElement(c);

                    DEBUG_MSG(logger) << columnPath;

                    ExpressionValue storage;
                    auto pValue = embedding.tryGetNestedColumn(columnPath, storage);

                    float val = 0.0f;

                    if (!pValue) {
                        INFO_MSG(logger) << columnPath;
                        ExcAssert(pValue);
                    }

                    val = pValue->coerceToNumber().toDouble();

                    *p = val;
                    p++;
                }
            }
        }
    }

    auto tensor = ExpressionValue::embedding
        (Date::notADate(), buffer, ST_FLOAT32, DimsVector{ num_values });

    return {tensor};
}
    
std::unique_ptr<FunctionApplierT<ProximateVoxelsInput, ProximateVoxelsOutput> >
ProximateVoxelsFunction::
bindT(SqlBindingScope & outerContext,
      const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<ProximateVoxelsFunctionApplier> result
        (new ProximateVoxelsFunctionApplier(this));
 
    return std::move(result);
}

static RegisterFunctionType<ProximateVoxelsFunction, ProximateVoxelsFunctionConfig>
regProximateVoxelsFunction(builtinPackage(),
                            "image.proximatevoxels",
                            "Find values in a cubic volume inside a 3d embedding",
                            "functions/ProximateVoxelsFunction.md.html");


} // namespace MLDB

