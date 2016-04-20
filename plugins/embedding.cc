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
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/arch/timers.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const Coord & coord)
{
    // Currently not used, since we haven't exposed serialization
    // of embedding datasets.
    throw ML::Exception("Coord serialization");
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, Coord & coord)
{
    // Currently not used, since we haven't exposed serialization
    // of embedding datasets.
    throw ML::Exception("Coord deserialization");
}

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const Coords & coords)
{
    // Currently not used, since we haven't exposed serialization
    // of embedding datasets.
    throw ML::Exception("Coords serialization");
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, Coords & coords)
{
    // Currently not used, since we haven't exposed serialization
    // of embedding datasets.
    throw ML::Exception("Coords deserialization");
}


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

    EmbeddingDatasetRepr(std::vector<ColumnName> columnNames,
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
    
    static uint64_t getRowHashForIndex(const RowName & rowName)
    {
        uint64_t result = RowHash(rowName).hash();
        if (result == nullHashIn) {
            if (rowName.size() == 1 && rowName[0] == Coord("0"))
                return result;
            ExcAssert(rowName == Coord("null"));
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
        filter_ostream stream(filename);
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
    }

    // TODO: make it loadable...
    Itl(const std::string & address, MetricSpace metric)
        : metric(metric), committed(lock, metric), uncommitted(nullptr), address(address)
    {
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

        virtual RowName next() {
            auto repr = source->committed();     
            return repr->rows[index++].rowName;
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
            for (size_t i = 0;  i < repr->columnNames.size();  ++i) {
                knownColumns.emplace_back(repr->columnNames[i], valueInfo,
                                          COLUMN_IS_DENSE, i /* fixed index */);
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
                //cerr << "rowName = " << rowName << endl;
                //cerr << "rowHash = " << RowHash(rowName) << endl;
                // Check if it's a double record or a hash collision
                RowName oldName
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

        parallelMap(0, (*uncommitted).rows.size(), indexRow);

        // Create the vantage point tree
        cerr << "creating vantage point tree" << endl;
        ML::Timer timer;
        
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

                ML::distribution<float> result(items.size());

                auto doItem = [&] (int n)
                {
                    int i = items[n];

                    result[n] = (*uncommitted).dist(item, i);

                    if (item == i)
                        ExcAssertEqual(result[n], 0.0);

                    if (!isfinite(result[n])) {
                        cerr << "dist between " << i << " and " << item << " is "
                             << result.back() << endl;
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

        cerr << "VP tree done in " << timer.elapsed() << endl;
        
        committed.replace(uncommitted);
        uncommitted = nullptr;

        if (!address.empty()) {
            cerr << "saving embedding" << endl;
            committed()->save(address);
        }
    }

    vector<tuple<RowName, RowHash, float> >
    getNeighbors(const ML::distribution<float> & coord,
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

        auto neighbors = repr->vpTree->search(dist, numNeighbors, maxDistance);

        //cerr << "neighbors = " << jsonEncode(neighbors) << endl;
        
        vector<tuple<RowName, RowHash, float> > result;
        for (auto & n: neighbors) {
            result.emplace_back(repr->rows[n.second].rowName,
                                repr->rows[n.second].rowName,
                                n.first);
        }

        return result;
    }

    vector<tuple<RowName, RowHash, float> >
    getRowNeighbors(const RowName & row, int numNeighbors, double maxDistance)
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

        vector<tuple<RowName, RowHash, float> > result;
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
// Should probably remove; it's subsumed by the nearest neigbors function.
#if 0
    if (functionName == "distance") {
        // 1.  We need the rowName() function
        //auto rowName = context.getfunction("rowName");

        return {[=] (const std::vector<ExpressionValue> & evaluatedArgs,
                     const SqlRowScope & context) -> ExpressionValue
                {
                    //cerr << "calling overridden distance with args " << jsonEncode(args)
                    //     << endl;
                    auto row1 = evaluatedArgs.at(1).getRow();
                    Utf8String row2Name = evaluatedArgs.at(2).toUtf8String();
                    auto row2 = itl->getRow(RowName(row2Name)).columns;

                    // Work out the timestamp at which this was known
                    Date ts = Date::negativeInfinity();
                    for (auto & c: row1)
                        ts.setMax(std::get<1>(c).getEffectiveTimestamp());
                    for (auto & c: row2)
                        ts.setMax(std::get<2>(c));

                    //cerr << "row = " << jsonEncode(row) << endl;

                    if (evaluatedArgs.at(0).toUtf8String() == "pythag") {
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
                    else if (evaluatedArgs.at(0).toUtf8String() == "cosine") {
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
                    else if (evaluatedArgs.at(0).toUtf8String() == "angular") {
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
                                              "unknown distance metric " + evaluatedArgs.at(0).toUtf8String());
                },
                std::make_shared<Float64ValueInfo>() };
    }
#endif

    return BoundFunction();
}

vector<tuple<RowName, RowHash, float> >
EmbeddingDataset::
getNeighbors(const ML::distribution<float> & coord, int numNeighbors, double maxDistance) const
{
    return itl->getNeighbors(coord, numNeighbors, maxDistance);
}
    
vector<tuple<RowName, RowHash, float> >
EmbeddingDataset::
getRowNeighbors(const RowName & row, int numNeighbors, double maxDistance) const
{
    return itl->getRowNeighbors(row, numNeighbors, maxDistance);
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


/*****************************************************************************/
/* NEAREST NEIGHBOUR FUNCTION                                                */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(NearestNeighborsFunctionConfig);

NearestNeighborsFunctionConfigDescription::
NearestNeighborsFunctionConfigDescription()
{
    addField("defaultNumNeighbors",
             &NearestNeighborsFunctionConfig::defaultNumNeighbors,
             "Default number of neighbors to return. This can be overritten "
             "when calling the function.", unsigned(10));
    addField("defaultMaxDistance",
             &NearestNeighborsFunctionConfig::defaultMaxDistance,
             "Default maximum distance from the original row returned "
             "neighbors can be.",
             double(INFINITY));
    addField("dataset", &NearestNeighborsFunctionConfig::dataset,
             "Embedding dataset in which to find neighbors.");
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
             "Row containing the nearest neighbors, each with its distance");
}

NearestNeighborsFunction::
NearestNeighborsFunction(MldbServer * owner,
                         PolyConfig config,
                         const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner)
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
    vector<tuple<RowName, RowHash, float> > neighbors;
    if (inputRow.isAtom()) {
        neighbors = applier.embeddingDataset
            ->getRowNeighbors(RowName(inputRow.toUtf8String()),
                               numNeighbors, maxDistance);
    }
    else if(inputRow.isEmbedding() || inputRow.isRow()) {
        auto embedding = applier.getEmbeddingFromExpr(inputRow);
        neighbors = applier.embeddingDataset
            ->getNeighbors(inputRow.getEmbedding(-1),
                           numNeighbors, maxDistance);
    }
    else {
        throw ML::Exception("Input row must be either a row name or an embedding");
    }

    RowValue rtnRow;
    rtnRow.reserve(neighbors.size());
    for(auto & neighbor : neighbors) {
        rtnRow.emplace_back(get<0>(neighbor), get<2>(neighbor), ts);
    }
    
    return {ExpressionValue(rtnRow)};
}
    
std::unique_ptr<FunctionApplierT<NearestNeighborsInput, NearestNeighborsOutput> >
NearestNeighborsFunction::
bindT(SqlBindingScope & outerContext, const FunctionValues & input) const
{
    std::unique_ptr<NearestNeighborsFunctionApplier> result
        (new NearestNeighborsFunctionApplier(this));

    auto boundDataset = functionConfig.dataset->bind(outerContext);
    if (!boundDataset.dataset) {
        throw HttpReturnException
            (400, "Nearest neighbors function cannot operate on the output of "
             "a table expression, only dataset of type embedding.");
    }
    
    std::shared_ptr<ExpressionValueInfo> datasetInput
        = boundDataset.dataset->getRowInfo();
    vector<ColumnName> columnNames
        = datasetInput->allColumnNames();
    auto coordInput = *input;//input.getValueInfo("coords").getExpressionValueInfo();
    if (coordInput.couldBeRow()) {
        result->getEmbeddingFromExpr
            = coordInput.extractDoubleEmbedding(columnNames);
    }

    result->embeddingDataset
        = dynamic_pointer_cast<EmbeddingDataset>(boundDataset.dataset);
    if (!result->embeddingDataset) {
        throw HttpReturnException
            (400, "A dataset of type embedding needs to be provided for "
             "the nearest.neighbors function");
    }
 
    return std::move(result);
}

static RegisterDatasetType<EmbeddingDataset, EmbeddingDatasetConfig>
regEmbedding(builtinPackage(),
             "embedding",
             "Dataset to record a set of coordinates per row",
             "datasets/EmbeddingDataset.md.html");

static RegisterFunctionType<NearestNeighborsFunction, NearestNeighborsFunctionConfig>
regNearestNeighborsFunction(builtinPackage(),
                            "embedding.neighbors",
                            "Return the nearest neighbors of a known row in an embedding dataset",
                            "functions/NearestNeighborsFunction.md.html");


} // namespace MLDB
} // namespace Datacratic
