/** embedding.cc
    Jeremy Barnes, 9 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Implementation of embedding dataset.
*/

#include "embedding.h"
#include "mldb/ml/tsne/vantage_point_tree.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/vfs/filter_streams.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


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
             "neighbours calculations.  Options are 'cosine' (which is "
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

    EmbeddingDatasetRepr(std::vector<ColumnName> columnNames,
                         MetricSpace metric)
        : columnNames(columnNames), columns(this->columnNames.size()),
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
    
    static uint64_t getRowHashForIndex(const RowName & rowName)
    {
        uint64_t result = RowHash(rowName).hash();
        if (result == nullHashIn) {
            if (rowName.stringEqual("0"))
                return result;
            ExcAssert(rowName.stringEqual("null"));
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
        Row(RowName rowName, ML::distribution<float> coords, Date timestamp)
            : rowName(std::move(rowName)), coords(std::move(coords))
        {
        }

        RowName rowName;
        ML::distribution<float> coords;
        Date timestamp;

        void serialize(ML::DB::Store_Writer & store) const
        {
            store << rowName.toString() << coords << timestamp;
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

    float dist(unsigned row1, const ML::distribution<float> & row2) const
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

    std::vector<ColumnName> columnNames;
    std::vector<std::vector<float> > columns;
    ML::Lightweight_Hash<ColumnHash, int> columnIndex;

    std::vector<Row> rows;
    ML::Lightweight_Hash<uint64_t, int> rowIndex;
    
    std::unique_ptr<ML::VantagePointTreeT<int> > vpTree;
    std::unique_ptr<DistanceMetric> distance;

    void save(const std::string & filename)
    {
        ML::filter_ostream stream(filename);
        ML::DB::Store_Writer store(stream);
        
        serialize(store);

        // Make sure that we saved properly
        stream.close();
    }
    void serialize(ML::DB::Store_Writer & store) const;
};

const RowHash EmbeddingDatasetRepr::nullHashIn(RowName("null"));

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
        : metric(metric), committed(lock, metric), uncommitted(nullptr)
    {
        initRoutes();
    }

    // TODO: make it loadable...
    Itl(const std::string & address, MetricSpace metric)
        : metric(metric), committed(lock, metric), uncommitted(nullptr), address(address)
    {
        initRoutes();
    }

    ~Itl()
    {
        delete uncommitted.load();
    }

    MetricSpace metric;

    GcLock lock;
    RcuProtected<EmbeddingDatasetRepr> committed;

    //typedef ML::Spinlock Mutex;
    typedef std::mutex Mutex;
    Mutex mutex;
    std::atomic<EmbeddingDatasetRepr *> uncommitted;
    std::string address;

    RestRequestRouter router;

    void initRoutes()
    {
        addRouteSyncJsonReturn(router, "/rowNeighbours", {"GET"},
                               "Return the nearest neighbours of a known row",
                               "Tuple of [rowName, rowId, distance]",
                               &Itl::getRowNeighbours,
                               this,
                               RestParam<RowName>("row", "The row to query"),
                               RestParamDefault<int>("numNeighbours",
                                                     "Number of neighbours to find",
                                                     10),
                               RestParamDefault<double>("maxDistance",
                                                        "Maximum distance to return",
                                                        INFINITY));
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};

        std::vector<RowName> result;

        if (limit == -1)
            limit = repr->rows.size();

        for (unsigned i = start;  i < repr->rows.size() && i < start + limit;  ++i) {
            result.emplace_back(repr->rows[i].rowName);
        }

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
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

    virtual bool knownRow(const RowName & rowName) const
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

    virtual MatrixNamedRow getRow(const RowName & rowName) const
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

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        static const RowHash nullHash(RowName("null"));
        static const RowHash nullHashMunged(RowName("\x01null"));
        if (rowHash == nullHash)
            return getRowName(nullHashMunged);

        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "unknown row");

        auto it = repr->rowIndex.find(EmbeddingDatasetRepr::getRowHashForIndex(rowHash));
        if (it == repr->rowIndex.end() || it->second == -1)
            throw HttpReturnException(400, "unknown row");

        return repr->rows[it->second].rowName;
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        auto repr = committed();
        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get name of unknown column from uninitialized embedding");

        return repr->columnIndex.count(column);
    }

    virtual ColumnName getColumnName(ColumnHash column) const
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
    virtual std::vector<ColumnName> getColumnNames() const
    {
        auto repr = committed();
        if (!repr->initialized())
            return {};
        
        return repr->columnNames;
    }

    virtual size_t getRowCount() const
    {
        auto repr = committed();
        if (!repr->initialized())
            return 0;
        return repr->rows.size();
    }

    virtual size_t getColumnCount() const
    {
        auto repr = committed();
        if (!repr->initialized())
            return 0;
        return repr->columnNames.size();
    }

    virtual bool forEachColumnGetStats(const OnColumnStats & onColumnStats) const
    {
        auto repr = committed();

        if (!repr->initialized())
            throw HttpReturnException(400, "Can't get stats of unknown column");

        for (auto & col: repr->columnIndex) {
            ColumnStats toStoreResult;
            const ColumnName & columnName = repr->columnNames.at(col.second);
            if (!onColumnStats(columnName,
                               getColumnStats(columnName, toStoreResult)))
                return false;
        }

        return true;
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnName & ch, ColumnStats & toStoreResult) const
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
    virtual MatrixColumn getColumn(const ColumnName & column) const
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

    /** Return a RowValueInfo that describes all rows that could be returned
        from the dataset.
    */
    virtual std::shared_ptr<RowValueInfo> getRowInfo() const
    {
        std::vector<KnownColumn> knownColumns;
        
        auto valueInfo = std::make_shared<Float32ValueInfo>();
        
        auto repr = committed();
        if (repr->initialized()) {
            for (auto & c: repr->columnNames) {
                knownColumns.emplace_back(c, valueInfo, COLUMN_IS_DENSE);
            }
        }

        return std::make_shared<RowValueInfo>(knownColumns);
    }

    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const
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
    getKnownColumnInfos(const std::vector<ColumnName> & columnNames) const
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
    recordEmbedding(const std::vector<ColumnName> & columnNames,
                    const std::vector<std::tuple<RowName, std::vector<float>, Date> > & rows)
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
            const RowName & rowName = std::get<0>(r);
            uint64_t rowHash = EmbeddingDatasetRepr::getRowHashForIndex(rowName);

            if (rowName.stringEqual("null") || rowName.stringEqual("0"))
                cerr << "inserting " << rowName << " with hash " << rowHash
                     << endl;

            const auto & vec = std::get<1>(r);
            ML::distribution<float> embedding(vec.begin(), vec.end());
            Date ts = std::get<2>(r);

            int index = (*uncommitted).rows.size();

            if (!(*uncommitted).rowIndex.insert({ rowHash, index }).second) {
                if ((*uncommitted).rowIndex[rowHash] == -1)
                    (*uncommitted).rowIndex[rowHash] = index;
                else {
                                                
                    //cerr << "rowName = " << rowName << endl;
                    //cerr << "rowHash = " << RowHash(rowName) << endl;
                    // Check if it's a double record or a hash collision
                    RowName oldName
                        = (*uncommitted).rows.at((*uncommitted).rowIndex[rowHash])
                        .rowName;
                    if (oldName == rowName)
                        throw HttpReturnException
                            (400, "Row '" + rowName.toString()
                             + "' has already been recorded into embedding dataset.  "
                             + "Are you re-using an output dataset?");
                    else {
                        throw HttpReturnException
                            (400, "Row '" + rowName.toString() + "' and '"
                             + oldName.toString() + "' both hash to '"
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
    recordRowItl(const RowName & rowName,
                 const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        auto repr = committed();

        uint64_t rowHash = EmbeddingDatasetRepr::getRowHashForIndex(rowName);
        ML::distribution<float> embedding;
        Date latestDate = Date::negativeInfinity();

        // Do it here before we acquire the lock in the case that it's initalized
        if (uncommitted) {
            embedding.resize((*uncommitted).columns.size(),
                             std::numeric_limits<float>::quiet_NaN());

            for (auto & v: vals) {
                auto it = (*uncommitted).columnIndex.find(std::get<0>(v));
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
                std::vector<ColumnName> columnNames;
                for (auto & c: vals) {
                    columnNames.push_back(std::get<0>(c));
                }
                
                //cerr << "columnNames = " << columnNames << endl;
                
                uncommitted = new EmbeddingDatasetRepr(columnNames, metric);
            }
            else {
                uncommitted = new EmbeddingDatasetRepr(*repr);
            }

            embedding.resize(repr->columns.size(),
                             std::numeric_limits<float>::quiet_NaN());
        }

        if (embedding.empty()) {
            //cerr << "embedding is empty" << endl;
            //cerr << "repr->initialized = " << repr->initialized() << endl;
            embedding.resize((*uncommitted).columns.size(),
                             std::numeric_limits<float>::quiet_NaN());
            for (auto & v: vals) {
                auto it = (*uncommitted).columnIndex.find(std::get<0>(v));
                if (it == (*uncommitted).columnIndex.end())
                    throw HttpReturnException(400, "Couldn't extract column with name 2 "
                                        + std::get<0>(v).toString());
                
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
                //cerr << "rowName = " << rowName << endl;
                //cerr << "rowHash = " << RowHash(rowName) << endl;
                // Check if it's a double record or a hash collision
                RowName oldName
                    = (*uncommitted).rows.at((*uncommitted).rowIndex[rowHash])
                    .rowName;
                if (oldName == rowName)
                    throw HttpReturnException
                        (400, "Row '" + rowName.toString()
                         + "' has already been recorded into embedding dataset.  "
                         + "Are you re-using an output dataset?");
                else {
                    return;
                    throw HttpReturnException
                        (400, "Row '" + rowName.toString() + "' and '"
                         + oldName.toString() + "' both hash to '"
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

        cerr << "committing embedding dataset" << endl;

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

        ML::run_in_parallel_blocked(0, (*uncommitted).rows.size(), indexRow);

        // Create the vantage point tree
        cerr << "creating vantage point tree" << endl;
        
        std::vector<int> items;
        for (unsigned i = 0;  i < (*uncommitted).rows.size();  ++i) {
            items.push_back(i);
        }

        // Function used to build the VP tree, that scans all of the items in
        // parallel.
        auto dist = [&] (int item, const std::vector<int> & items, int depth)
            -> ML::distribution<float>
            {
                ExcAssertLessEqual(depth, 100);  // 2^100 items is enough

                ML::distribution<float> result;
                result.reserve(items.size());

                for (auto & i: items) {
                    result.push_back((*uncommitted).dist(item, i));

                    if (item == i)
                        ExcAssertEqual(result.back(), 0.0);

                    if (!isfinite(result.back())) {
                        cerr << "dist between " << i << " and " << item << " is "
                             << result.back() << endl;
                    }
                    ExcAssert(isfinite(result.back()));
                }

                return result;
            };
        
        // Create the VP tree for indexed lookups on distance
        (*uncommitted).vpTree.reset(ML::VantagePointTreeT<int>::createParallel(items, dist));
        
        committed.replace(uncommitted);
        uncommitted = nullptr;

        if (!address.empty()) {
            cerr << "saving embedding" << endl;
            committed()->save(address);
        }
    }

    vector<tuple<RowName, RowHash, float> >
    getRowNeighbours(const RowName & row, int numNeighbours, double maxDistance)
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
        
        auto neighbours = repr->vpTree->search(dist, numNeighbours, INFINITY);

        vector<tuple<RowName, RowHash, float> > result;
        for (auto & n: neighbours) {
            result.emplace_back(repr->rows[n.second].rowName,
                                repr->rows[n.second].rowName,
                                n.first);
        }

        return result;
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        if (context.remaining == "/neighbours") {
            // get all of the points

            int numNeighbours = 10;

            auto repr = committed();
            if (!repr->initialized())
                return RestRequestRouter::MR_NO;
            
            ML::distribution<float> vals(repr->columnIndex.size(), 0.0);
            //std::numeric_limits<float>::quiet_NaN());

            for (auto & p: request.params) {
                CellValue v = jsonDecodeStr<CellValue>(p.second);

                if (p.first == "numNeighbours") {
                    numNeighbours = v.toInt();
                    continue;
                }
                ColumnName n(p.first);
                
                auto it = repr->columnIndex.find(n);
                if (it == repr->columnIndex.end()) {
                    connection.sendErrorResponse(422, "unknown column name '" + p.first + "'");
                    return RestRequestRouter::MR_ERROR;
                }
                vals.at(it->second) = v.toDouble();
            }

            auto dist = [&] (int item) -> float
            {
                float result = repr->dist(item, vals);
                ExcAssert(isfinite(result));
                return result;
            };

            auto neighbours = repr->vpTree->search(dist, numNeighbours, INFINITY);

            //cerr << "neighbours = " << jsonEncode(neighbours) << endl;
            
            vector<tuple<RowName, RowHash, float> > result;
            for (auto & n: neighbours) {
                result.emplace_back(repr->rows[n.second].rowName,
                                    repr->rows[n.second].rowName,
                                    n.first);
            }

#if 0
            vector<MatrixNamedRow> result;
            for (auto & n: neighbours) {
                MatrixNamedRow row;
                row.rowName = repr->rows[n.second].rowName;
                row.rowHash = row.rowName;
                row.columns.emplace_back(RowName("distance"), n.first, Date());
                result.emplace_back(std::move(row));
            }
#endif
            
            connection.sendResponse(200, jsonEncodeStr(result), "application/json");

            return RestRequestRouter::MR_YES;
            
        }

        return router.processRequest(connection, request, context);
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
                 const std::function<bool (const Json::Value &)> & onProgress)
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
recordRowItl(const RowName & rowName,
          const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    return itl->recordRowItl(rowName, vals);
}

void
EmbeddingDataset::
recordEmbedding(const std::vector<ColumnName> & columnNames,
                const std::vector<std::tuple<RowName, std::vector<float>, Date> > & rows)
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

BoundFunction
EmbeddingDataset::
overrideFunction(const Utf8String & functionName,
                 SqlBindingScope & context) const
{
    if (functionName == "distance") {
        // 1.  We need the rowName() function
        //auto rowName = context.getfunction("rowName");

        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context) -> ExpressionValue
                {
                    //cerr << "calling overridden distance with args " << jsonEncode(args)
                    //     << endl;

                    auto row1 = args.at(1).getRow();
                    string row2Name = args.at(2).toString();
                    auto row2 = itl->getRow(RowName(row2Name)).columns;

                    // Work out the timestamp at which this was known
                    Date ts = Date::negativeInfinity();
                    for (auto & c: row1)
                        ts.setMax(std::get<1>(c).getEffectiveTimestamp());
                    for (auto & c: row2)
                        ts.setMax(std::get<2>(c));

                    //cerr << "row = " << jsonEncode(row) << endl;

                    if (args.at(0).toString() == "pythag") {
                        ExcAssertEqual(row1.size(), row2.size());
                        double total = 0.0;
                        for (unsigned i = 0;  i < row1.size();  ++i) {
                            double v1 = std::get<1>(row1[i]).toDouble();
                            double v2 = std::get<1>(row2[i]).toDouble();

                            //cerr << "v1 = " << v1 << " v2 = " << v2 << endl;

                            ExcAssertEqual(std::get<0>(row1[i]),
                                           std::get<0>(row2[i]));
                            total += (v1 - v2) * (v1 - v2);
                        }

                        //cerr << "total = " << total << endl;

                        return ExpressionValue(sqrt(total), ts);
                    }
                    else if (args.at(0).toString() == "cosine") {
                        ExcAssertEqual(row1.size(), row2.size());
                        size_t nd = row1.size();
                        
                        ML::distribution<double> vec1(nd), vec2(nd);
                        for (unsigned i = 0;  i < nd;  ++i) {
                            double v1 = std::get<1>(row1[i]).toDouble();
                            double v2 = std::get<1>(row2[i]).toDouble();

                            ExcAssertEqual(std::get<0>(row1[i]),
                                           std::get<0>(row2[i]));
                            vec1[i] = v1;
                            vec2[i] = v2;
                        }

                        return ExpressionValue(1 - vec1.dotprod(vec2) / (vec1.two_norm() * vec2.two_norm()), ts);
                    }
                    else if (args.at(0).toString() == "angular") {
                        ExcAssertEqual(row1.size(), row2.size());
                        size_t nd = row1.size();
                        
                        ML::distribution<double> vec1(nd), vec2(nd);
                        for (unsigned i = 0;  i < nd;  ++i) {
                            double v1 = std::get<1>(row1[i]).toDouble();
                            double v2 = std::get<1>(row2[i]).toDouble();

                            ExcAssertEqual(std::get<0>(row1[i]),
                                           std::get<0>(row2[i]));
                            vec1[i] = v1;
                            vec2[i] = v2;
                        }

                        return ExpressionValue(1 - acos(vec1.dotprod(vec2) / (vec1.two_norm() * vec2.two_norm())) / M_PI, ts);
                    }
                    
                    throw HttpReturnException(400,
                                              "unknown distance metric " + args.at(0).toString());
                },
                std::make_shared<Float64ValueInfo>() };
    }

    return BoundFunction();
}

KnownColumn
EmbeddingDataset::
getKnownColumnInfo(const ColumnName & columnName) const
{
    return itl->getKnownColumnInfo(columnName);
}

std::vector<KnownColumn>
EmbeddingDataset::
getKnownColumnInfos(const std::vector<ColumnName> & columnNames) const
{
    return itl->getKnownColumnInfos(columnNames);
}

RestRequestMatchResult
EmbeddingDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    return itl->handleRequest(connection, request, context);
}

static RegisterDatasetType<EmbeddingDataset, EmbeddingDatasetConfig>
regEmbedding(builtinPackage(),
             "embedding",
             "Dataset to record a set of coordinates per row",
             "datasets/EmbeddingDataset.md.html");

} // namespace MLDB
} // namespace Datacratic
