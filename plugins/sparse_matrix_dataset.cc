// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** sparse_matrix_dataset.cc
    Jeremy Barnes, 9 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Implementation of sparseMatrix database.
*/

#include "sparse_matrix_dataset.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/compact_vector_value_description.h"
#include "mldb/types/map_description.h"
#include "sparse_matrix.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/any_impl.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/arch/timers.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


DEFINE_STRUCTURE_DESCRIPTION(BaseEntry);

BaseEntryDescription::
BaseEntryDescription()
{
    addField("rowcol", &BaseEntry::rowcol,
             "Row or column ID");
    addField("timestamp", &BaseEntry::timestamp,
             "Timestamp, encoded as integer");
    addField("val", &BaseEntry::val,
             "Value, encoded as integer");
    addField("tag", &BaseEntry::tag,
             "Tag, encoded as integer");
    addField("metadata", &BaseEntry::metadata,
             "Metadata strings");
}


/*****************************************************************************/
/* VALUE ENCODINGS                                                           */
/*****************************************************************************/

/** Encoding works as follows:
    - Values are either stored inline or as a pointer to an external table
    - Inline values 

    Encoding types
    - Null
    - 7 bit nonnegative integer

    - 14 bit nonnegative integer

    - Null is encoded as 0
    - Zero is encoded as 1
    - Small positive integers
    - 
    - ...
*/


/*****************************************************************************/
/* SPARSE MATRIX DATASET INTERNAL                                            */
/*****************************************************************************/

struct SparseMatrixDataset::Itl
    : public MatrixView, public ColumnIndex {

    Itl()
        : epoch(0), defaultTransaction(gc), timeQuantumSeconds(1.0)
    {
    }

    void init(std::shared_ptr<BaseMatrix> metadata,
              std::shared_ptr<BaseMatrix> matrix,
              std::shared_ptr<BaseMatrix> inverse,
              std::shared_ptr<BaseMatrix> values)
    {
        this->metadata = std::move(metadata);
        this->matrix = std::move(matrix);
        this->inverse  = std::move(inverse);
        this->values = std::move(values);

        auto defaultTransaction = std::make_shared<ReadTransaction>();
        defaultTransaction->matrix = this->matrix->startReadTransaction();
        defaultTransaction->inverse = this->inverse->startReadTransaction();
        defaultTransaction->values = this->values->startReadTransaction();

        setDefaultTransaction(defaultTransaction);
    }
    
    ~Itl()
    {
    }
    
    typedef std::mutex RootLock;
    mutable RootLock rootLock;
    std::atomic<int64_t> epoch;
    std::shared_ptr<BaseMatrix> metadata;
    std::shared_ptr<BaseMatrix> matrix;
    std::shared_ptr<BaseMatrix> inverse;
    std::shared_ptr<BaseMatrix> values;

    struct ReadTransaction {
        int64_t epoch;
        std::shared_ptr<MatrixReadTransaction> matrix;
        std::shared_ptr<MatrixReadTransaction> inverse;
        std::shared_ptr<MatrixReadTransaction> values;
    };

    struct WriteTransaction: public ReadTransaction {
        WriteTransaction(const ReadTransaction & view)
            : ReadTransaction(view),
              matrix(view.matrix->startWriteTransaction()),
              inverse(view.inverse->startWriteTransaction()),
              values(view.values->startWriteTransaction())
        {
        }

        std::shared_ptr<MatrixWriteTransaction> matrix;
        std::shared_ptr<MatrixWriteTransaction> inverse;
        std::shared_ptr<MatrixWriteTransaction> values;
    };

    struct SparseRowStream : public RowStream {

        SparseRowStream(SparseMatrixDataset::Itl* source) : source(source)
        {
            trans = source->getReadTransaction();
        }

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<SparseRowStream>(source);
            return ptr;
        }

        virtual void initAt(size_t start){
            internalStream = trans->matrix->getStream();
            internalStream->initAt(start);
        }

        virtual RowName next() {
            uint64_t i = internalStream->next();
            return source->getRowNameTrans(RowHash(i), *trans);
        }

        std::shared_ptr<MatrixReadTransaction::Stream> internalStream;
        std::shared_ptr<ReadTransaction> trans;
        SparseMatrixDataset::Itl* source;
    };

    std::shared_ptr<SparseRowStream> getRowStream()
    {
        auto trans = getReadTransaction();
        if (trans->matrix->isSingleReadEntry())
            return make_shared<SparseRowStream>(this);
        else
            return std::shared_ptr<SparseRowStream>();
    }

    GcLock gc;

    /// Default transaction when none was passed
    RcuProtected<std::shared_ptr<ReadTransaction> > defaultTransaction;

    /// Control the quantization of timestamp (default is quantize to second)
    double timeQuantumSeconds;

    /// Obtain a new read transaction at the current state
    std::shared_ptr<ReadTransaction>
    getReadTransaction() const
    {
        return *defaultTransaction();
    }

    /// Update the current default read transaction after a commit
    void setDefaultTransaction(std::shared_ptr<ReadTransaction> trans)
    {
        this->defaultTransaction
            .replace(new std::shared_ptr<ReadTransaction>
                     (std::move(trans)));
    }

    /// Obtain a new write transaction based upon a current read transaction
    std::shared_ptr<WriteTransaction>
    getWriteTransaction(ReadTransaction & view)
    {
        return std::make_shared<WriteTransaction>(view);
    }

    /// Commit a set of writes to the database
    void commitWrites(WriteTransaction & trans)
    {
        std::unique_lock<RootLock> guard(rootLock);
        ++epoch;

        ThreadPool tp;

        auto doCommit = [&] (MatrixWriteTransaction & trans)
            {
                if (trans.commitNeedsThread())
                    tp.add(std::bind(&MatrixWriteTransaction::commit,
                                     &trans));
            };
        
        doCommit(*trans.matrix);
        doCommit(*trans.inverse);
        doCommit(*trans.values);
        
        tp.waitForAll();

        auto result = std::make_shared<ReadTransaction>();
        result->matrix = matrix->startReadTransaction();
        result->inverse = inverse->startReadTransaction();
        result->values = values->startReadTransaction();

        setDefaultTransaction(result);
    }

    void optimize()
    {
        //cerr << "optimize() on MutableSparseMatrixDataset" << endl;
        ML::Timer timer;

        std::unique_lock<RootLock> guard(rootLock);
        // We don't increment the epoch since logically it's exactly the same

        ThreadPool tp;

        tp.add(std::bind(&BaseMatrix::optimize,
                         matrix.get()));
        tp.add(std::bind(&BaseMatrix::optimize,
                         inverse.get()));
        tp.add(std::bind(&BaseMatrix::optimize,
                         values.get()));
        
        tp.waitForAll();

        auto result = std::make_shared<ReadTransaction>();
        result->matrix = matrix->startReadTransaction();
        result->inverse = inverse->startReadTransaction();
        result->values = values->startReadTransaction();

        setDefaultTransaction(result);
    }

    Date decodeTs(int64_t ts) const
    {
        if (ts == -1)
            return Date::negativeInfinity();
        else if (ts == 1)
            return Date::positiveInfinity();
        else if (ts == 0)
            return Date::notADate();
        else if (ts < 0)
            return Date::fromSecondsSinceEpoch((ts + 1) * timeQuantumSeconds);
        else return Date::fromSecondsSinceEpoch((ts - 2) * timeQuantumSeconds);
    }

    CellValue decodeVal(uint64_t val, uint32_t tag, 
                        ReadTransaction & trans) const
    {
        switch (tag) {
        // null
        case 0:
            return CellValue();


        // integers
        case 1:
            return CellValue(val);
        case 3:
            return CellValue(-int64_t(val));
        case 2:
            return CellValue(std::numeric_limits<int64_t>::min());

        // floating point
        case 8: {
            union {  double d;  uint64_t u; }; u = val;  return d;
        }
        case 9: { // date
            union {  double d;  uint64_t u; }; u = val;
            return Date::fromSecondsSinceEpoch(d);
        }
        case 25: { // time interval, seconds exactly represented with a float
            union Repr { struct { uint16_t mths; uint16_t days; float seconds; }; uint64_t u; } repr;
            repr.u = val;
            return CellValue::fromMonthDaySecond(repr.mths, repr.days, repr.seconds);
        }
        case 26: { // time interval, seconds can't be done with a float
            CellValue result;

            auto onRow = [&] (const BaseEntry & entry)
                {
                    result = jsonDecodeStr<CellValue>(entry.metadata.at(0));
                    return false;
                };
            
            if (trans.values->iterateRow(val, onRow))
                throw HttpReturnException(400, "Can't find unknown value hash",
                                          "hash", val);
            return result;
        }            

        // Strings
        case 16:
            return CellValue("");
        case 17:
        case 18:
        case 19:
        case 20: {
            char c[4];
            int len = tag - 16;
            for (int i = len - 1;  i >= 0;  --i) {
                c[i] = val & 255;
                val >>= 8;
            }

            return CellValue(Utf8String(string(c, c + len)));
        }
        case 24: {
            CellValue result;

            auto onRow = [&] (const BaseEntry & entry)
                {
                    result = CellValue(Utf8String(entry.metadata.at(0)));
                    return false;
                };
            
            if (trans.values->iterateRow(val, onRow))
                throw HttpReturnException(400, "Can't find unknown value hash",
                                          "hash", val);
            return result;
        }
        default:
            throw HttpReturnException(500, "Unknown value tag", "tag", tag);
        }            
    }
    
    uint64_t encodeTs(Date val)
    {
        if (val == Date::negativeInfinity())
            return -1;
        else if (val == Date::positiveInfinity())
            return 1;
        else if (!val.isADate())
            return 0;

        val.quantize(timeQuantumSeconds);
        int64_t seconds = (val.secondsSinceEpoch() / timeQuantumSeconds);
        if (seconds < 0)
            return seconds - 1;
        else return seconds + 2;
    }

    std::pair<uint64_t, uint32_t>
    encodeVal(const CellValue & val, WriteTransaction & trans)
    {
        switch (val.cellType()) {
        case CellValue::EMPTY:
            return { 0, 0 };
        case CellValue::INTEGER:
            if (val.isUInt64())
                return { val.toUInt(), 1 };
            if (val.toInt() == std::numeric_limits<int64_t>::min())
                return { 0, 2 };
            return { -val.toInt(), 3 };
        case CellValue::FLOAT: {
            union {  double d;  uint64_t u; }; d = val.toDouble();
            return { u, 8 };
        }
        case CellValue::TIMESTAMP: {
            union {  double d;  uint64_t u; }; d = val.toTimestamp().secondsSinceEpoch();
            return { u, 9 };
        }
        case CellValue::TIMEINTERVAL: {
            union Repr { struct { uint16_t mths; uint16_t days; float seconds; }; uint64_t u; } repr;
            double seconds;
            std::tie(repr.mths, repr.days, seconds) = val.toMonthDaySecond();
            repr.seconds = seconds;
            if (repr.seconds == seconds) {
                // Fits in a floating point number with no loss in precision
                return { repr.u, 25 };
            }
            else {
                // Need to add the string representation to the list of values
                CellValueHash hash = val.hash();
                if (!trans.values->knownRow(hash)) {
                    BaseEntry entry;
                    entry.rowcol = 0;
                    entry.timestamp = 0;
                    entry.val = 0;
                    entry.metadata.push_back(jsonEncodeStr(val));
                    trans.values->recordRow(hash, &entry, 1);
                }
                return { hash, 26 };
            }
        }
        case CellValue::ASCII_STRING: {
            uint32_t strNumChars = val.toStringLength();
            const unsigned char * strChars = (const unsigned char *)val.stringChars();

            // 1-4 character strings get inlined
            if (strNumChars <= 4) {
                if (strNumChars == 0) {
                    return { 0, 16 };
                }
                else if (strNumChars == 1) {
                    return { strChars[0], 17 };
                }
                else if (strNumChars == 2) {
                    return { 0U | strChars[0] << 8 | strChars[1] , 18 };
                }
                else if (strNumChars == 3) {
                    return { 0U | strChars[0] << 16 | strChars[1] << 8 | strChars[2],
                            19 };
                }
                else {
                    return { 0U | strChars[0] << 24 | strChars[1] << 16
                            | strChars[2] << 8 | strChars[3],
                            20 };
                }
            }
            // fall through for non-inlined version
        }
        case CellValue::UTF8_STRING: {
            uint32_t strNumChars = val.toStringLength();
            const unsigned char * strChars = (const unsigned char *)val.stringChars();
            CellValueHash hash = val.hash();
            if (!trans.values->knownRow(hash)) {
                BaseEntry entry;
                entry.rowcol = 0;
                entry.timestamp = 0;
                entry.val = 0;
                entry.metadata.push_back(std::string(strChars, strChars + strNumChars));
                trans.values->recordRow(hash, &entry, 1);
            }
            return { hash, 24 };
        }
        default:
            ExcAssert(false);
        }
    }

    uint64_t encodeCol(const ColumnName & col, WriteTransaction & trans)
    {
        if (col == ColumnName())
            throw HttpReturnException(400, "Datasets don't accept empty column names");

        ColumnHash ch(col);
        if (!trans.values->knownRow(ch.hash())) {
            BaseEntry entry;
            entry.rowcol = 0;
            entry.timestamp = 0;
            entry.val = 0;
            entry.metadata.push_back(col.toString());
            trans.values->recordRow(ch.hash(), &entry, 1);
        }
        return ch.hash();
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowName> result;
        auto trans = getReadTransaction();
        trans->matrix
            ->iterateRows([&] (uint64_t row)
                          {
                              result.emplace_back(getRowNameTrans(RowHash(row), *trans));
                              return true;
                          });

         std::sort(result.begin(), result.end(),
              [&] (const RowName & r1, const RowName & r2)
              {
                  return r1.hash() < r2.hash();
              });

        if (start < 0)
            throw HttpReturnException(400, "Invalid start for row names",
                                      "start", start);
        if (limit < -1)
            throw HttpReturnException(400, "Invalid limit for row names",
                                      "limit", limit);
        
        if (start >= result.size()) {
            result.clear();
            return result;
        }

        result.erase(result.begin(), result.begin() + start);

        if (limit != -1 && limit < result.size())
            result.erase(result.begin() + limit, result.end());

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowHash> result;
        auto trans = getReadTransaction();
        trans->matrix
            ->iterateRows([&] (uint64_t row)
                          {
                              result.emplace_back(RowHash(row));
                              return true;
                          });

        std::sort(result.begin(), result.end());

        if (start < 0)
            throw HttpReturnException(400, "Invalid start for row names",
                                      "start", start);
        if (limit < -1)
            throw HttpReturnException(400, "Invalid limit for row names",
                                      "limit", limit);
        
        if (start >= result.size()) {
            result.clear();
            return result;
        }

        if (limit != -1 && limit < result.size())
            result.erase(result.begin() + limit, result.end());

        result.erase(result.begin(), result.begin() + start);

        return result;
    }

    virtual bool knownRow(const RowName & rowName) const
    {
        auto trans = getReadTransaction();
        return trans->matrix->knownRow(RowHash(rowName).hash());
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        auto trans = getReadTransaction();

        MatrixNamedRow result;
        result.rowHash = result.rowName = rowName;

        auto onEntry = [&] (const BaseEntry & entry)
            {
                Date ts = decodeTs(entry.timestamp);
                ColumnHash col(entry.rowcol);
                CellValue v = decodeVal(entry.val, entry.tag, *trans);
                result.columns.emplace_back(getColumnNameTrans(col, *trans), v, ts);
                return true;
            };

        trans->matrix->iterateRow(result.rowHash.hash(), onEntry);
        
        return result;
    }

    RowName getRowNameTrans(const RowHash & rowHash,
                            ReadTransaction & trans) const
    {
        RowName result;

        auto onRow = [&] (const BaseEntry & entry)
            {
                result = RowName(entry.metadata.at(0));
                return false;
            };
            
        if (trans.values->iterateRow(rowHash.hash(), onRow))
            throw HttpReturnException(400, "Can't get name of unknown row");
        return result;
    }
        
    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto trans = getReadTransaction();
        return getRowNameTrans(rowHash, *trans);
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        auto trans = getReadTransaction();
        return trans->inverse->knownRow(column.hash());
    }

    virtual ColumnName getColumnNameTrans(ColumnHash column,
                                          ReadTransaction & trans) const
    {
        ColumnName result;

        auto onRow = [&] (const BaseEntry & entry)
            {
                result = ColumnName(entry.metadata.at(0));
                return false;  // return false to short circuit
            };
            
        // True return means not short circuited, ie not found
        if (trans.values->iterateRow(column.hash(), onRow))
            throw HttpReturnException(400,
                                      "Can't get name of unknown column '" + column.toString() + "'");
        return result;
    }

    virtual ColumnName getColumnName(ColumnHash column) const
    {
        auto trans = getReadTransaction();
        return getColumnNameTrans(column, *trans);
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnName> getColumnNames() const
    {
        std::vector<ColumnName> result;
        auto trans = getReadTransaction();
        trans->inverse
            ->iterateRows([&] (uint64_t row)
                          {
                              result.emplace_back(getColumnNameTrans(ColumnHash(row),
                                                                     *trans));
                              return true;
                          });
        
        std::sort(result.begin(), result.end());

        return result;
    }

    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const
    {
        // TODO: make sure it's known...
        return { columnName, std::make_shared<AnyValueInfo>(), COLUMN_IS_SPARSE };
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumnTrans(const ColumnName & column,
                                        ReadTransaction & trans) const
    {
        MatrixColumn result;
        result.columnHash = result.columnName = column;

        auto onEntry = [&] (const BaseEntry & entry)
            {
                Date ts = decodeTs(entry.timestamp);
                RowHash row(entry.rowcol);
                CellValue v = decodeVal(entry.val, entry.tag, trans);
                result.rows.emplace_back(getRowNameTrans(row, trans), v, ts);
                return true;
            };

        trans.inverse->iterateRow(column.hash(), onEntry);
        return result;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        auto trans = getReadTransaction();
        return getColumnTrans(column, *trans);
    }
    
    void
    recordRowTrans(const RowName & rowName,
                   const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals,
                   WriteTransaction & trans)
    {
        if (rowName == RowName())
            throw HttpReturnException(400, "Datasets don't accept empty row names");

        RowHash hash(rowName);

        // Make sure we know the row name
        if (!trans.values->knownRow(hash.hash())) {
            BaseEntry entry;
            entry.rowcol = 0;
            entry.timestamp = 0;
            entry.val = 0;
            entry.metadata.push_back(rowName.toString());
            trans.values->recordRow(hash, &entry, 1);
        }
        
        // Now record the values
        std::vector<BaseEntry> entries;
        entries.reserve(vals.size());
        for (auto & v: vals) {
            uint64_t ts = encodeTs(std::get<2>(v));
            uint32_t tag;
            uint64_t val;
            std::tie(val, tag) = encodeVal(std::get<1>(v), trans);

            //CellValue decoded = decodeVal(val, tag, trans);
            //ExcAssertEqual(decoded, std::get<1>(v));

            uint64_t col = encodeCol(std::get<0>(v), trans);
            //cerr << "col " << std::get<0>(v) << " encoded as " << col << endl;

            entries.push_back({col, ts, val, tag, {}});
        }

        trans.matrix->recordRow(hash.hash(), &entries[0], entries.size());
        trans.inverse->recordCol(hash.hash(), &entries[0], entries.size());
    }

    virtual void
    recordRow(const RowName & rowName,
              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        std::shared_ptr<WriteTransaction> trans
            = getWriteTransaction(**defaultTransaction());
        recordRowTrans(rowName, vals, *trans);
        commitWrites(*trans);
    }

    virtual void
    recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
    {
        std::shared_ptr<WriteTransaction> trans
            = getWriteTransaction(**defaultTransaction());
        
        for (auto & r: rows) {
            recordRowTrans(r.first, r.second, *trans);
        }

        commitWrites(*trans);
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return Datacratic::MR_NO;
    }

    virtual size_t getRowCount() const
    {
        auto trans = getReadTransaction();
        return trans->matrix->rowCount();
    }
    
    virtual size_t getColumnCount() const
    {
        auto trans = getReadTransaction();
        return trans->inverse->rowCount();
    }

    Any getStatus() const
    {
        auto trans = getReadTransaction();
        Json::Value result;
        result["rowCount"] = trans->matrix->rowCount();
        result["columnCount"] = trans->matrix->rowCount();
        result["valueCount"] = trans->values->rowCount();
        return result;
    }
};


/*****************************************************************************/
/* SPARSE MATRIX DATASET                                                     */
/*****************************************************************************/

SparseMatrixDataset::
SparseMatrixDataset(MldbServer * owner)
    : Dataset(owner)
{
}
    
SparseMatrixDataset::
~SparseMatrixDataset()
{
}

Any
SparseMatrixDataset::
getStatus() const
{
    return itl->getStatus();
}

void
SparseMatrixDataset::
recordRowItl(const RowName & rowName,
             const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    validateNames(rowName, vals);
    return itl->recordRow(rowName, vals);
}

void
SparseMatrixDataset::
recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
{
    validateNames(rows);
    return itl->recordRows(rows);
}

KnownColumn
SparseMatrixDataset::
getKnownColumnInfo(const ColumnName & columnName) const
{
    return itl->getKnownColumnInfo(columnName);
}

void
SparseMatrixDataset::
commit()
{
    // We call commit() when we're done with writing data.  We take advantage
    // of it to optimize the storage of the data that's been recorded to
    // date.
    return itl->optimize();
}
    
Date
SparseMatrixDataset::
quantizeTimestamp(Date timestamp) const
{
    return itl->decodeTs(itl->encodeTs(timestamp));
}

std::shared_ptr<MatrixView>
SparseMatrixDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
SparseMatrixDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
SparseMatrixDataset::
getRowStream() const
{
    return itl->getRowStream();
}

RestRequestMatchResult
SparseMatrixDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    return itl->handleRequest(connection, request, context);
}


enum CommitMode {
    READ_ON_COMMIT,
    READ_FAST,
    WRITE_FAST
};

/*****************************************************************************/
/* MUTABLE BASE MATRIX                                                       */
/*****************************************************************************/

struct MutableBaseData {

    MutableBaseData(GcLock & gc, CommitMode commitMode)
        : repr(gc, new Repr()), commitMode(commitMode)
    {
    }

    typedef std::unordered_map<uint64_t, ML::compact_vector<BaseEntry, 1> > RowsEntry;

    struct Rows {
        Rows()
            : cachedRowCount(-1)
        {
        }

        Rows(Rows && other) noexcept
            : entries(std::move(other.entries)),
              cachedRowCount(other.cachedRowCount.load())
        {
        }

        Rows(const Rows & other)
            : entries(other.entries),
              cachedRowCount(other.cachedRowCount.load())
        {
        }

        Rows & operator = (Rows && other) noexcept
        {
            this->entries = std::move(other.entries);
            this->cachedRowCount = other.cachedRowCount.load();
            return *this;
        }

        Rows & operator = (const Rows & other)
        {
            this->entries = other.entries;
            this->cachedRowCount = other.cachedRowCount.load();
            return *this;
        }

        std::vector<std::shared_ptr<const RowsEntry> > entries;
        mutable std::atomic<int64_t> cachedRowCount;
        mutable std::mutex rowCountMutex;
        
        bool iterateRow(uint64_t rowNum,
                        const std::function<bool (const BaseEntry & entry)> & onEntry) const
        {
            for (auto & e: entries) {
                auto it = e->find(rowNum);
                if (it != e->end()) {
                    for (auto & r: it->second) {
                        if (!onEntry(r))
                            return false;
                    }
                }
            }

            return true;
        }

        bool iterateRows(const std::function<bool (uint64_t row)> & onRow) const
        {
            std::vector<uint64_t> allRows;

            for (auto & e: entries) {
                for (auto & r: *e) {
                    allRows.emplace_back(r.first);
                }
            }

            std::sort(allRows.begin(), allRows.end());
            auto end = std::unique(allRows.begin(), allRows.end());
            for (auto it = allRows.begin(); it != end;  ++it) {
                if (!onRow(*it))
                    return false;
            }

            return true;
        }

        bool knownRow(uint64_t rowNum) const
        {
            for (auto & e: entries) {
                if (e->count(rowNum))
                    return true;
            }
            return false;
        }
        
        size_t rowCount() const
        {
            if (entries.empty())
                return 0;
            if (entries.size() == 1)
                return entries.back()->size();
            int64_t r = cachedRowCount.load();
            if (r != -1)
                return r;

            // TODO: this is slow, but at least correct
            std::unique_lock<std::mutex> guard(rowCountMutex);
            std::vector<uint64_t> allRows;

            for (auto & e: entries) {
                for (auto & r: *e) {
                    allRows.emplace_back(r.first);
                }
            }

            std::sort(allRows.begin(), allRows.end());

            int64_t rowCount = std::unique(allRows.begin(), allRows.end())
                - allRows.begin();
            cachedRowCount = rowCount;
            return rowCount;
        }

        Rows optimize(std::vector<std::shared_ptr<RowsEntry> > & nonReadableWrites) const
        {
            Rows result;

            RowsEntry newEntries;

            if (!entries.empty()) {
                newEntries = *entries.front();

                for (unsigned i = 1;  i < entries.size();  ++i) {
                    auto & e = entries[i];
                    for (auto & v: *e) {
                        auto & vec = newEntries[v.first];
                        vec.insert(vec.end(), v.second.begin(), v.second.end());
                    }
                }
            }

            for (auto & w: nonReadableWrites) {

                auto & e = *w;
                for (auto & v: e) {
                    auto & vec = newEntries[v.first];
                    vec.insert(vec.end(),
                               std::make_move_iterator(v.second.begin()),
                               std::make_move_iterator(v.second.end()));
                }
            }

            nonReadableWrites.clear();

            result.entries.emplace_back(new RowsEntry(std::move(newEntries)));
            return std::move(result);
        }

        struct Stream {       

            Stream(const MutableBaseData::Rows* source) : source(source)
            {
            }

            void initAt(size_t start)
            {
                entriesIter = source->entries.begin();
               
                subIter = (*entriesIter)->begin();
                size_t count = 0;
                while (start - count > (*entriesIter)->size())
                {
                    count += (*entriesIter)->size();
                    ++entriesIter;
                    subIter = (*entriesIter)->begin();
                };

                //should we switch to an ordered map?
                while (count < start)
                {
                    ++count;
                    ++subIter;
                };
            }

            virtual uint64_t next()
            {
                uint64_t value = subIter->first;
                subIter++;
                if (subIter == (*entriesIter)->end())  {
                    ++entriesIter;
                    if (entriesIter != source->entries.end())  {
                        subIter = (*entriesIter)->begin();
                    }
                }

                return value;
            }

            std::vector<std::shared_ptr<const RowsEntry> >::const_iterator entriesIter;
            RowsEntry::const_iterator subIter;
            const MutableBaseData::Rows* source;
        };

        bool isSingleReadEntry() const
        {
            return entries.size() == 1;
        }

    };

    struct Repr {
        Rows rows;
    };

    RcuProtected<std::shared_ptr<Repr> > repr;
    CommitMode commitMode;
    std::vector<std::shared_ptr<RowsEntry> > nonReadableWrites;

    /** Commit and optimize everything that's been written up to here. */
    void optimize()
    {
        std::unique_lock<std::mutex> guard(mutex);

        auto newRows = (*repr.unsafePtr())->rows.optimize(nonReadableWrites);

        std::unique_ptr<std::shared_ptr<Repr> > newRepr
            (new std::shared_ptr<Repr>);
        newRepr->reset(new Repr());
        (**newRepr).rows = std::move(newRows);

        repr.replace(newRepr.release());
    }

    /** Insert the given set of rows very quickly, but in a way that they
        will not be available for reading until the next commit()
        operation has completed.
    */
    void insertNonReadable(std::shared_ptr<RowsEntry> written)
    {
        std::unique_lock<std::mutex> guard(this->mutex);
        this->nonReadableWrites.emplace_back(std::move(written));
    }

    /** Insert the given set of rows in a manner that will execute quickly
        but may be slow to read back.
    */
    void insertReadable(std::shared_ptr<RowsEntry> written)
    {
        std::unique_lock<std::mutex> guard(this->mutex);

        auto r = this->repr();
        const Rows & oldRows = (*r)->rows;
        
        std::vector<std::shared_ptr<const RowsEntry> >
            newRows = oldRows.entries;
        newRows.emplace_back(std::move(written));

        std::unique_ptr<std::shared_ptr<Repr> > newRepr
            (new std::shared_ptr<Repr>);
        newRepr->reset(new Repr());

        (**newRepr).rows.entries = std::move(newRows);
        (**newRepr).rows.cachedRowCount = oldRows.cachedRowCount.load();

        this->repr.replace(newRepr.release());
    }

    /** Insert the given set of rows in a manner that may have significant
        latency but will be fast to read back afterwards.
    */
    void insertBalanced(std::shared_ptr<RowsEntry> written)
    {
        // Only one balancing at a time
        std::unique_lock<std::mutex> guard(this->mutex);

        ML::Timer timer;

        // Get a reference to the data
        auto r = this->repr();
        const Rows & oldRows = (*r)->rows;
        
        std::shared_ptr<RowsEntry> current = written;

        std::vector<std::shared_ptr<const RowsEntry> >
            newRows;
        
        /* This loop maintains the invariant that:
           - Entries are in decreasing order of size
           - Each entry is at most 1/2 the size of the precedent

           It's done that way so that an insertion has an amortized
           constant cost.
        */
           
        for (int i = oldRows.entries.size() - 1;  i >= 0;  --i) {
            // Two choices here: either we a) merge "current" with
            // the existing value, or b) write the existing value and
            // then current

            std::shared_ptr<const RowsEntry> rows
                = oldRows.entries[i];

            // If we are balanced or were previously balanced then
            // continue
            if (!current) {
                newRows.push_back(rows);
                continue;
            }

            double ratio = 1.0 * current->size() / rows->size();
            if (ratio <= 0.5) {
                newRows.emplace_back(std::move(current));
                current.reset();
                newRows.push_back(rows);
                continue;
            }

            // Otherwise, merge the two together
            auto merged = std::make_shared<RowsEntry>(*rows);

            for (auto & rowIn: *current) {
                auto & rowOut = (*merged)[rowIn.first];
                rowOut.insert(rowOut.end(),
                              std::make_move_iterator(rowIn.second.begin()),
                              std::make_move_iterator(rowIn.second.end()));
            }
            
            //cerr << "merging " << current->size() << " and " << rows->size()
            //     << " yielded " << merged->size() << " with ratio " << ratio
            //     << endl;

            current = merged;
        }

        if (current)
            newRows.emplace_back(std::move(current));

        // Put them back in order of size
        std::reverse(newRows.begin(), newRows.end());

        //for (unsigned i = 0;  i < newRows.size();  ++i) {
        //    cerr << "Rows entry " << i << " has " << newRows[i]->size()
        //         << endl;
        //}

        std::unique_ptr<std::shared_ptr<Repr> > newRepr
            (new std::shared_ptr<Repr>);
        newRepr->reset(new Repr());

        (**newRepr).rows.entries = std::move(newRows);
        (**newRepr).rows.cachedRowCount = oldRows.cachedRowCount.load();

        this->repr.replace(newRepr.release());
    }

    void insert(std::shared_ptr<RowsEntry> written)
    {
        switch (commitMode) {
        case READ_ON_COMMIT:
            insertNonReadable(std::move(written));
            return;
        case READ_FAST:
            insertBalanced(std::move(written));
            return;
        case WRITE_FAST:
            insertReadable(std::move(written));
            return;
        default:
            throw HttpReturnException(500, "Invalid commitMode");
        }
    }

    mutable std::mutex mutex;
};

struct MutableReadTransaction;

struct MutableWriteTransaction: public MatrixWriteTransaction {

    MutableWriteTransaction(std::shared_ptr<MutableBaseData> data)
        : data(data),
          view(*data->repr()),
          rows(view->rows),
          written(new MutableBaseData::RowsEntry)
    {
        rows.entries.push_back(written);
    }

    std::shared_ptr<MutableBaseData> data;
    std::shared_ptr<MutableBaseData::Repr> view;
    MutableBaseData::Rows rows;
    std::shared_ptr<MutableBaseData::RowsEntry> written;

    virtual bool
    iterateRow(uint64_t rowNum,
               const std::function<bool (const BaseEntry & entry)> & onEntry)
    {
        return rows.iterateRow(rowNum, onEntry);
    }

    virtual bool iterateRows(const std::function<bool (uint64_t row)> & onRow)
    {
        return rows.iterateRows(onRow);
    }

    virtual bool knownRow(uint64_t rowNum)
    {
        return rows.knownRow(rowNum);
    }

    virtual size_t rowCount() const
    {
        return rows.rowCount();
    }

    virtual void recordRow(uint64_t rowNum, const BaseEntry * entries, int n)
    {
        auto & row = (*written)[rowNum];
        for (unsigned i = 0;  i < n;  ++i) {
            row.emplace_back(entries[i]);
        }
    }

    virtual void recordRow(uint64_t rowNum, BaseEntry * entries, int n)
    {
        auto & row = (*written)[rowNum];
        for (unsigned i = 0;  i < n;  ++i) {
            row.emplace_back(std::move(entries[i]));
        }
    }

    virtual void recordCol(uint64_t colNum, const BaseEntry * entries, int n)
    {
        for (unsigned i = 0;  i < n;  ++i) {
            BaseEntry e = entries[i];
            auto & row = (*written)[e.rowcol];
            e.rowcol = colNum;
            row.emplace_back(std::move(e));
        }
    }

    virtual void recordCol(uint64_t colNum, BaseEntry * entries, int n)
    {
        for (unsigned i = 0;  i < n;  ++i) {
            BaseEntry & e = entries[i];
            auto & row = (*written)[e.rowcol];
            e.rowcol = colNum;
            row.emplace_back(std::move(e));
        }
    }

    // Does this commit need to run in a separate thread?
    virtual bool commitNeedsThread() const
    {
        return written->size() * 2 > rows.entries.back()->size();
    }

    virtual void commit()
    {
        data->insert(written);
    }

    virtual std::shared_ptr<MatrixWriteTransaction> startWriteTransaction() const
    {
        return std::make_shared<MutableWriteTransaction>(*this);
    }

     virtual std::shared_ptr<MatrixReadTransaction::Stream> getStream() const
     {
        ExcAssert(false);
        return std::shared_ptr<MatrixReadTransaction::Stream>();
     }
};

struct MutableReadTransaction: public MatrixReadTransaction {
    MutableReadTransaction(std::shared_ptr<MutableBaseData> data)
        : data(data), repr(*data->repr()), rows(repr->rows)
    {
    }

    struct Stream : public MatrixReadTransaction::Stream {

        Stream(const MutableReadTransaction* source) : innerStream(&(source->rows)), source(source)
        {
        }

        virtual std::shared_ptr<MatrixReadTransaction::Stream> clone() const
        {
            return make_shared<MutableReadTransaction::Stream>(source);
        }

        virtual void initAt(size_t start)
        {
            innerStream.initAt(start);      
        }

        virtual uint64_t next()
        {
            return innerStream.next();
        }

        MutableBaseData::Rows::Stream innerStream;
        const MutableReadTransaction* source;
    };

    std::shared_ptr<MutableBaseData> data;
    std::shared_ptr<MutableBaseData::Repr> repr;
    const MutableBaseData::Rows & rows;

    virtual std::shared_ptr<MatrixReadTransaction::Stream> getStream() const {
        return make_shared<MutableReadTransaction::Stream>(this);
    }

    virtual bool iterateRow(uint64_t rowNum,
                            const std::function<bool (const BaseEntry & entry)> & onEntry)
    {
        return rows.iterateRow(rowNum, onEntry);
    }

    virtual bool iterateRows(const std::function<bool (uint64_t row)> & onRow)
    {
        return rows.iterateRows(onRow);
    }

    virtual bool knownRow(uint64_t rowNum)
    {
        return rows.knownRow(rowNum);
    }

    virtual size_t rowCount() const
    {
        return rows.rowCount();
    }

    virtual std::shared_ptr<MatrixWriteTransaction> startWriteTransaction() const
    {
        return std::make_shared<MutableWriteTransaction>(data);
    }

    virtual bool isSingleReadEntry() const
    {
        return rows.isSingleReadEntry();
    }
};

struct MutableBaseMatrix: public BaseMatrix {
    std::shared_ptr<MutableBaseData> data;
    
    MutableBaseMatrix(GcLock & gc, CommitMode commitMode)
        : data(new MutableBaseData(gc, commitMode))
    {
    }

    virtual ~MutableBaseMatrix()
    {
    }

    virtual std::shared_ptr<MatrixReadTransaction> startReadTransaction() const
    {
        return std::make_shared<MutableReadTransaction>(data);
    }

    virtual std::shared_ptr<MatrixWriteTransaction> startWriteTransaction()
    {
        return std::make_shared<MutableWriteTransaction>(data);
    }

    virtual void optimize()
    {
        data->optimize();
    }
};

/******************************************************************************/
/* MUTABLE SPARSE MATRIX DATASET CONFIG                                       */
/******************************************************************************/

DEFINE_ENUM_DESCRIPTION(WriteTransactionLevel);

WriteTransactionLevelDescription::
WriteTransactionLevelDescription()
{
    addValue("consistentAfterWrite", WT_READ_AFTER_WRITE,
             "A value written will be available immediately after writing.  "
             "This provides the most consistency as operations are "
             "serializable, at the expense of slower writes and reads.");
    addValue("consistentAfterCommit", WT_READ_AFTER_COMMIT,
             "A value written will only be guaranteed to be available after "
             "a `commit()` call has returned successfully, and may not be "
             "readable until that point.  This provides much faster write "
             "performance and should be used in any batch insertion scenario.");
}

DEFINE_ENUM_DESCRIPTION(TransactionFavor);

TransactionFavorDescription::
TransactionFavorDescription()
{
    addValue("favorReads", TF_FAVOR_READS,
             "Values will be written in an indexed manner that favors "
             "read speed over write speed.  This will reduce the write "
             "volume, but make reads fast.");
    addValue("favorWrites", TF_FAVOR_WRITES,
             "Values will be written quickly in a non-indexed manner that "
             "favors write speed over read speed.  Values written will "
             "still be readable, but reads may take longer as there are "
             "no indexes maintained on recent writes.");
}

MutableSparseMatrixDatasetConfig::
MutableSparseMatrixDatasetConfig()
    : timeQuantumSeconds(1.0),
      consistencyLevel(WT_READ_AFTER_COMMIT),
      favor(TF_FAVOR_READS)
{
}

DEFINE_STRUCTURE_DESCRIPTION(MutableSparseMatrixDatasetConfig);

MutableSparseMatrixDatasetConfigDescription::
MutableSparseMatrixDatasetConfigDescription()
{
    nullAccepted = true;
    addField("timeQuantumSeconds", &MutableSparseMatrixDatasetConfig::timeQuantumSeconds,
             "a number that controls the resolution of timestamps stored in the dataset, "
             "in seconds. 1 means one second, 0.001 means one millisecond, 60 means one minute. "
             "Higher resolution requires more memory to store timestamps.", 1.0);

    addField("consistencyLevel", &MutableSparseMatrixDatasetConfig::consistencyLevel,
             "Transaction level for reading of written values.  In the "
             "default level, which is `consistentAfterCommit`, a value is "
             "only guaranteed to be readable after a commit (so it may seem "
             "like data is being lost if read before a commit) but writes are fast. "
             "With the `consistentAfterWrite` level, a written value can "
             "immediately be read back but writes are slower.", 
             WT_READ_AFTER_COMMIT);
    addField("favor", &MutableSparseMatrixDatasetConfig::favor,
             "Whether to favor reads or writes.  Only has effect for when "
             "`consistencyLevel` is set to `consistentAfterWrite`.",
             TF_FAVOR_READS);
}

/*****************************************************************************/
/* MUTABLE SPARSE MATRIX DATASET                                             */
/*****************************************************************************/

struct MutableSparseMatrixDataset::Itl
    : public SparseMatrixDataset::Itl {

    GcLock gc;

    Itl(double timeQuantumSeconds,
        WriteTransactionLevel consistencyLevel,
        TransactionFavor favor) 
    {
        CommitMode mode;
        if (consistencyLevel == WT_READ_AFTER_COMMIT)
            mode = READ_ON_COMMIT;
        else if (favor == TF_FAVOR_READS)
            mode = READ_FAST;
        else mode = WRITE_FAST;

        SparseMatrixDataset::Itl::timeQuantumSeconds = timeQuantumSeconds;
        init(std::make_shared<MutableBaseMatrix>(gc, mode),
             std::make_shared<MutableBaseMatrix>(gc, mode),
             std::make_shared<MutableBaseMatrix>(gc, mode),
             std::make_shared<MutableBaseMatrix>(gc, mode));
    }
};

/** Live recordable and queryable sparse matrix dataset. */

MutableSparseMatrixDataset::
MutableSparseMatrixDataset(MldbServer * owner,
                           PolyConfig config,
                           const std::function<bool (const Json::Value &)> & onProgress)
    : SparseMatrixDataset(owner)
{
    auto params = config.params.convert<MutableSparseMatrixDatasetConfig>();
    itl.reset(new Itl(params.timeQuantumSeconds, params.consistencyLevel, params.favor));
}

static RegisterDatasetType<MutableSparseMatrixDataset,
                           MutableSparseMatrixDatasetConfig>
regSparseMatrix(builtinPackage(),
                "sparse.mutable",
                "Sparse dataset which can be recorded to",
                "datasets/MutableSparseMatrixDataset.md.html");


} // namespace MLDB
} // namespace Datacratic
