/** dataset.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for datasets into MLDB.
*/

#include "mldb/sql/dataset_fwd.h"
#include "mldb/sql/dataset_types.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/sql/cell_value.h"
#include "mldb/types/url.h"
#include "mldb/core/recorder.h"
#include "mldb/utils/progress.h"
#include <set>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


#pragma once


namespace MLDB {

struct MldbServer;

struct SqlExpression;
struct SqlRowExpression;
struct SqlBindingScope;
struct GenerateRowsWhereFunction;
struct BasicRowGenerator;
struct OrderByExpression;
struct SelectExpression;
struct TupleExpression;
struct WhenExpression;
struct RowValueInfo;
struct ExpressionValue;
struct BucketList;
struct BucketDescriptions;

typedef EntityType<Dataset> DatasetType;


/*****************************************************************************/
/* MATRIX VIEW                                                               */
/*****************************************************************************/

/** Encapsulates a view where each row contains an arbitrary number of
    (key, value, timestamp) pairs.

    This view allows for lookups on a row by row basis.  It doesn't allow
    for any kind of column indexing.

    Note that this interface does not have to allow anything but lookup of
    the (key, value) pairs that a given row has.
*/

struct MatrixView {
    virtual ~MatrixView();

    /**
    Return a list of all rownames.
    Rownames are always unique.

    The sorting criteria is the same as with RowStream:

    Row names can be returned in an arbitrary order as long as it is deterministic.
    I.e. Calling getRowPaths several times on the same (unchanged) dataset should return rownames
    in the same (arbitrary) order.

    The ordering needs to be preserved regardless of start and limit.
    */
    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const = 0;

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const = 0;

    virtual size_t getRowCount() const = 0;

    /// Can be implemented by getRowPathName() then knownRow().  Deprecated.
    //virtual bool knownRowHash(const RowHash & row) const = 0;
    
    virtual bool knownRow(const RowPath & row) const = 0;
    
    virtual MatrixNamedRow getRow(const RowPath & row) const = 0;

    virtual RowPath getRowPath(const RowHash & row) const = 0;

    //virtual bool knownColumn(ColumnHash column) const = 0;

    virtual bool knownColumn(const ColumnPath & column) const = 0;
    
    virtual ColumnPath getColumnPath(ColumnHash column) const = 0;

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnPaths() const = 0;

    /** Return the number of distinct columns known. */
    virtual size_t getColumnCount() const = 0;

    /** Return the column count for the row.  Default uses getRow() and
        counts them.
    */
    virtual uint64_t getRowColumnCount(const RowPath & row) const;
};

struct CellValueStats {
    uint64_t rowCount() const
    {
        return rowCount_;
    }

    uint64_t rowCount_;
};

struct ColumnStats {
    ColumnStats()
        : isNumeric_(false), atMostOne_(false), rowCount_(0)
    {
    }

    bool isNumeric() const
    {
        return isNumeric_;
    }

    bool atMostOne() const
    {
        return atMostOne_;
    }

    uint64_t rowCount() const
    {
        return rowCount_;
    }

    std::map<CellValue, CellValueStats> values;

    bool isNumeric_;
    bool atMostOne_;
    uint64_t rowCount_;
};

/*****************************************************************************/
/* COLUMN INDEX                                                              */
/*****************************************************************************/

/** Encapsulates information allowing rows containing a given value in a
    given column.
*/

struct ColumnIndex {
    virtual ~ColumnIndex();

    /** Get stats for an individual column.  Default uses getColumn() and
        calculates from there.
    */
    virtual const ColumnStats &
    getColumnStats(const ColumnPath & column, ColumnStats & toStoreResult) const;

    /** Return the value of the column for all rows and timestamps. */
    /** Will throw if column is unknown                              */
    virtual MatrixColumn getColumn(const ColumnPath & column) const = 0;

    /** Return a dense column, with one value for every row in the same order as
        getRowPaths().

        Default builts on top of getColumn() and getRowPaths(), but is
        quite inefficient.
    */
    virtual std::vector<CellValue>
    getColumnDense(const ColumnPath & column) const;

    /** Return the set of distinct values in the column. */
    virtual std::vector<CellValue>
    getColumnDistinctValues(const ColumnPath & column) const;

    /** Return a bucketed dense column, with one value for every row in the same
        order as rowNames().  Numerical values will be split into a maximum of
        maxNumBuckets buckets, with split points as described in the
        return value.  

        Default builds on top of getColumnDense().
    */
    virtual std::tuple<BucketList, BucketDescriptions>
    getColumnBuckets(const ColumnPath & column,
                     int maxNumBuckets = -1) const;

    /** Return the value of the column for all rows, ignoring timestamps. 
        Default implementation is based on getColumn
        Will throw if column is unknown
    */
    virtual std::vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & column,
                    const std::function<bool (const CellValue &)> & filter = nullptr) const;

    /** Is this column known? */
    virtual bool knownColumn(const ColumnPath & column) const = 0;

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnPaths() const = 0;

    /** Return the number of rows that have this column set.  Default
        implementation uses getColumnStats.
    */
    virtual uint64_t getColumnRowCount(const ColumnPath & column) const;

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const = 0;
};


/*****************************************************************************/
/* ROW STREAM                                                                */
/*****************************************************************************/

/** This structure is used for streaming queries to generate a set of
    matching row names one at a time.

    Row names can be streamed in an arbitrary order as long as it is deterministic.
    I.e. using different rowstreams on the same (unchanged) dataset should return rownames
    in the same (arbitrary) order.
*/

struct RowStream {

    virtual ~RowStream()
    {
    }

    /** Clone the stream with just enough information to use the initAt 
        clones streams should be un-initialized                        */
    virtual std::shared_ptr<RowStream> clone() const = 0;

    /* Set where the stream should start.  This is called after
       clone() to initialize the stream at a given position.  Note
       that the parallelize() interface is better, and eventually
       clone() / initAt() will be replaced by that interface.
    */
    virtual void initAt(size_t start) = 0;

    /* Return the current RowPath and move the stream forward 
       for performance, this method shall NOT do bound checking 
       so be sure to obtain the maximum number of rows beforehand 
       using MatrixView::getRowCount for example.
    */
    virtual RowPath next() = 0;

    /** The methods below this one are only supported if this function
        returns true.  Note that eventually the above methods will be
        removed along with this function.

        Default returns false.  Datasets that support this interface
        should override and return true.
    */
    virtual bool supportsExtendedInterface() const;

    /// Constant for an automatic number of child streams (whatever
    /// makes sense to the implementation).
    static constexpr ssize_t AUTO = -1;

    /** Parallelize the given stream into about n sub-streams, each of
        which can be run in parallel.
        
        Default implementation will use a combination of clone() and
        initAt(), but specialization will help greatly.

        If streamOffsets is non-null, it will be filled in with the
        starting offset of each of the chunks.  It will contain one
        more entry than the number of streams returned, with the last
        entry being the total number of rows.
    */
    virtual std::vector<std::shared_ptr<RowStream> >
    parallelize(int64_t rowStreamTotalRows,
                ssize_t approxNumberOfChildStreams = AUTO,
                std::vector<size_t> * streamOffsets = nullptr) const;

    /** Return the rowName() at the current position of the
        stream.  This may be called as many times as required.
        Undefined behaviour if it is called on a stream without
        initAt() having been called or having advanced to the
        end.
    */
    virtual const RowPath & rowName(RowPath & storage) const = 0;

    /** Advance by a single position, but without returning a
        rowName().  More efficient than next() when just skipping
        ahead.
    */
    virtual void advance();

    /** Advance by a number of steps.  Equivalent to calling advance()
        n times, but may be more efficient for some row streams.
    */
    virtual void advanceBy(size_t n);

    /** Extract the given set of columns for the given stream,
        as atoms, for numRows rows.  This will fill in a
        numRows x columnNames.size() matrix pointed to by
        output with the values.

        Any column that is not present will fill in nulls.

        Any column that does not have a single, scalar value
        will throw an exception.
        
        It will also advance the rowStream by n rows.
    */
    virtual void
    extractColumns(size_t numRows,
                   const std::vector<ColumnPath> & columnNames,
                   CellValue * output);
    
    /** Extract the given set of columns for the given stream,
        as atoms, for numRows rows.  This will fill in a
        numRows x columnNames.size() matrix pointed to by
        output with the values.

        Any column that is not present or null will fill in with
        NaN.

        Any column that does not have a single, numeric value
        will throw an exception.
        
        It will also advance the rowStream by n rows.
    */
    virtual void
    extractNumbers(size_t numRows,
                   const std::vector<ColumnPath> & columnNames,
                   double * output);
    
};


/*****************************************************************************/
/* DATASET RECORDER                                                          */
/*****************************************************************************/

/** This is a recorder that forwards directly its records to a dataset. */

struct DatasetRecorder: public Recorder {
    
    DatasetRecorder(Dataset * dataset);

    virtual ~DatasetRecorder();

    virtual void
    recordRowExpr(const RowPath & rowName,
                  const ExpressionValue & expr) override;
    virtual void
    recordRow(const RowPath & rowName,
              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override;

    virtual void
    recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) override;

    virtual void
    recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue > > & rows) override;

private:
    Dataset * dataset;
    struct Itl;
    std::unique_ptr<Itl> itl;
};



/*****************************************************************************/
/* DATASET                                                                   */
/*****************************************************************************/

struct Dataset: public MldbEntity {
    Dataset(MldbServer * server);

    virtual ~Dataset();

    MldbServer * server;
    
    virtual Any getStatus() const = 0;

    virtual std::string getKind() const
    {
        return "dataset";
    }

    /** Base database methods require us to be able to iterate through rows.
        All other views are built on top of this.

        This function is not virtual as it performs commom validation for all
        recording operation.  Datasets must implement recordRowItl.
    */
    void recordRow(const RowPath & rowName,
                   const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    /** Internal handler for recording rows.  Default implementation throws that
        this dataset type does not support recording.
    */
    virtual void recordRowItl(const RowPath & rowName,
                              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    static void validateNames(const RowPath & rowName,
                      const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    static void validateNames(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    /** Record multiple rows in a single transaction.  Default implementation
        forwards to recordRow.

        If you override this call make sure to do handle the validation of
        column names and row names (see validateNames)

        This function must be thread safe with respect to concurrent calls to
        all other functions.
    */
    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    /** Record a column.  Default will forward to recordRows after transposing
        the input data.

        This function must be thread safe with respect to concurrent calls to
        all other functions.
    */
    virtual void recordColumn(const ColumnPath & columnName,
                              const std::vector<std::tuple<RowPath, CellValue, Date> > & vals);
    
    /** Record multiple columns in a single transaction.  Default implementation
        forwards to recordRow.

        This function must be thread safe with respect to concurrent calls to
        all other functions.
    */
    virtual void recordColumns(const std::vector<std::pair<ColumnPath, std::vector<std::tuple<RowPath, CellValue, Date> > > > & cols);

    /** Record an expression value as a row.  This will be flattened by
        datasets that require flattening.
        
        Default will flatten and call recordRow().

        This function must be thread safe with respect to concurrent calls to
        all other functions.
    */
    virtual void recordRowExpr(const RowPath & rowName,
                               const ExpressionValue & expr);

    /** Record an expression value as a row.  This will be flattened by
        datasets that require flattening.
        
        Default will flatten and call recordRows().

        This function must be thread safe with respect to concurrent calls to
        all other functions.
    */
    virtual void recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue> > & rows);

    struct MultiChunkRecorder {
        std::function<std::unique_ptr<Recorder> (size_t chunkIndex)> newChunk;
        std::function<void ()> commit;
    };

    /** Set up for a multithreaded record.  This returns an object that can
        generate a recorder for each chunk of an input.  Those chunks can
        be recorded into in a multithreaded manner, and finally all committed
        to the dataset at once.

        This allows for deterministic, multithreaded recording from bulk
        insert scenarios.

        The default will return an object that simply forwards to the
        record* methods.  Dataset types that support chunked recording can
        override.
    */
    virtual MultiChunkRecorder getChunkRecorder();

    /** Return what is known about the given column.  Default returns
        an "any value" result, ie nothing is known about the column.
    */
    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const;

    /** Return what is known about the given columns.  Default forwards
        to getKnownColumnInfo.  Some datasets can do a batch much more
        efficiently, so this function should be preferred if possible.
    */
    virtual std::vector<KnownColumn>
    getKnownColumnInfos(const std::vector<ColumnPath> & columnNames) const;

    /** Record multiple embedding rows.  This forwards to recordRows in the
        default implementation, but is much more efficient in datasets that
        are designed for embeddings.
    */
    virtual void recordEmbedding(const std::vector<ColumnPath> & columnNames,
                                 const std::vector<std::tuple<RowPath, std::vector<float>, Date> > & rows);

    /** Return a RowValueInfo that describes all rows that could be returned
        from the dataset.

        This function must be thread safe with respect to concurrent calls to
        all other functions.
    */
    virtual std::shared_ptr<RowValueInfo> getRowInfo() const;

    /** Return a row as an expression value.  Default forwards to the matrix
        view's getRow() function.
    */
    virtual ExpressionValue getRowExpr(const RowPath & row) const;


    /** Commit changes to the database.  Default is a no-op.

        This function must be thread safe with respect to concurrent calls to
        all other functions.  In particular, it must be safe to call commit()
        from multiple threads, and to call commit() in parallel with
        recordXxx() operations.
    */
    virtual void commit();

    /** Select from the database. */
    virtual std::vector<MatrixNamedRow>
    queryStructured(const SelectExpression & select,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    const OrderByExpression & orderBy,
                    const TupleExpression & groupBy,
                    const std::shared_ptr<SqlExpression> having,
                    const std::shared_ptr<SqlExpression> rowName,
                    ssize_t offset,
                    ssize_t limit,
                    Utf8String alias = "") const;

    std::tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
    queryStructuredExpr(const SelectExpression & select,
                        const WhenExpression & when,
                        const SqlExpression & where,
                        const OrderByExpression & orderBy,
                        const TupleExpression & groupBy,
                        const std::shared_ptr<SqlExpression> having,
                        const std::shared_ptr<SqlExpression> rowName,
                        ssize_t offset,
                        ssize_t limit,
                        Utf8String alias = "",
                        const ProgressFunc & onProgress = nullptr) const;

    /** Select from the database. */
    virtual bool
    queryStructuredIncremental(std::function<bool (Path &, ExpressionValue &)> & onRow,
                               const SelectExpression & select,
                               const WhenExpression & when,
                               const SqlExpression & where,
                               const OrderByExpression & orderBy,
                               const TupleExpression & groupBy,
                               const std::shared_ptr<SqlExpression> having,
                               const std::shared_ptr<SqlExpression> rowName,
                               ssize_t offset,
                               ssize_t limit,
                               Utf8String alias = "") const;

    /** Select from the database. */
    virtual std::vector<MatrixNamedRow>
    queryString(const Utf8String & query) const;
    
    /** Explain parsing of a select */
    virtual Json::Value
    selectExplainString(const Utf8String & select,
                        const Utf8String & where) const;

    /** Return a list of the column names in the dataset, with the given offset
        and limit.
    */
    virtual std::vector<ColumnPath>
    getColumnPaths(ssize_t offset = 0, ssize_t limit = -1) const;

    /** Return a list of flattened column names in the dataset
    */
    virtual std::vector<ColumnPath> 
    getFlattenedColumnNames() const;

    /** Return the number of distinct flattened known columns
        Defaults to getColumnCount (in matrix interface)
    */
    virtual size_t getFlattenedColumnCount() const;

    /** Return whether or not all columns names and info are known.
        Defaults to true
    */
    virtual bool hasColumnNames() const { return true; }

    /** Allow the dataset to intercept the binding of a function.  This allows
        it to use a locally defined or optimized version.  Should return a null
        function if there is no override.

        Default implementation returns a null function.
    */
    virtual BoundFunction
    overrideFunction(const Utf8String & tableName,
                     const Utf8String & functionName,
                     SqlBindingScope & context) const;

    /** Allow the dataset to override the generation of row IDs for a
        from (this) where (predicate) offset (offset) limit (limit)

        This is otherwise known as "predicate pushdown".  It can be used
        where the dataset includes an index which is suitable to quickly
        generate the values indicated.
        
        It may be called multiple times during the evaluation of an expression.
        
        The default implementation will look for common optimizable
        cases, and if none are found will perform a table scan.

        The where expression must only refer to columns in the dataset,
        or variables that are available in the context.

        The alias is the potential alias of the dataset in the context

        This is called by queryStructured and queryBasic, so cannot use those
        functions.

        Must return the *exact* set of rows or a stream that will do the same
        because the where expression will not be evaluated outside of this method
        if this method is called.

        Ordering can be arbitrary but needs to be deterministic, and there must not
        be duplicated rows.
    */    

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const Utf8String& alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const;

    /** Perform the guts of a select statement.  This will perform a single-
        table SELECT, with the given WHERE clause, ORDER BY, offset and limit.
        
        Note that the select can only return values of variables, not full
        expressions.

        This function is called by queryStructured, so cannot use that
        function.

        The allowParallel controls whether multiple threads may be used to
        run the query.
    */
    virtual BasicRowGenerator
    queryBasic(const SqlBindingScope & context,
               const SelectExpression & select,
               const WhenExpression & when,
               const SqlExpression & where,
               const OrderByExpression & orderBy,
               ssize_t offset,
               ssize_t limit) const;

    /** Method to overwrite to handle a request.  By default, the dataset
        will return that it can't handle any requests.  Used to expose
        dataset type-specific functionality.
    */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;


    /** In the case that a dataset's columns reference the name of other dataset,
        this will append the list of those aliases.
    */
    virtual void getChildAliases(std::vector<Utf8String>&) const {};

    virtual std::shared_ptr<MatrixView> getMatrixView() const = 0;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const = 0;
    virtual std::shared_ptr<RowStream> getRowStream() const { return std::shared_ptr<RowStream>(); } //optional but recommanded for performance

    /** Return the range of timestamps in the file.  The default implementation
        will scan the whole dataset, but other implementions may override for
        a more rapid result.

        Positive and negative infinity in a recorded timestamp must be
        represented in the result.  If timestamps are recorded with notADate(),
        then they should be *ignored* in the calculation of the result.

        If there are no rows in the dataset, or rows but no columns, then
        it must return notADate() for both values.

        In all cases, the output of this function should be the same as

        SELECT min(min_timestamp({*})), max(max_timestamp({*})) FROM dataset

        where nulls are converted to notADate().
    */
    virtual std::pair<Date, Date> getTimestampRange() const;

    /** Perform any internal quantization on the given timestamp.  This should
        transform a timestamp into exactly the timestamp that would be read
        back from the dataset on a query, were it recorded into the dataset.

        The default will throw that this dataset cannot record data, and so
        the query is inappropriate.
    */
    virtual Date quantizeTimestamp(Date timestamp) const;

    /* In the case of a dataset with rows composed from other datasets (i.e., joins)
       This will return the name that the row has in the table with this alias*/
    virtual RowPath getOriginalRowName(const Utf8String& tableName,
                                       const RowPath & name) const;

    virtual uint64_t getRowCount() const;
};


/*****************************************************************************/
/* PERSISTENT DATASET CONFIG                                                 */
/*****************************************************************************/

/** Configuration for a dataset that is persistent. */

struct PersistentDatasetConfig {
    Url dataFileUrl;  ///< Address (URI) of artifact to load the data from
};

DECLARE_STRUCTURE_DESCRIPTION(PersistentDatasetConfig);


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

std::shared_ptr<Dataset>
obtainDataset(MldbServer * server,
              const PolyConfig & config,
              const std::function<bool (const Json::Value & progress)> & onProgress
                  = nullptr);

std::shared_ptr<Dataset>
createDataset(MldbServer * server,
              const PolyConfig & config,
              const std::function<bool (const Json::Value & progress)> & onProgress
              = nullptr,
              bool overwrite = false);


DECLARE_STRUCTURE_DESCRIPTION_NAMED(DatasetPolyConfigDescription, PolyConfigT<Dataset>);
DECLARE_STRUCTURE_DESCRIPTION_NAMED(ConstDatasetPolyConfigDescription, PolyConfigT<const Dataset>);

std::shared_ptr<DatasetType>
registerDatasetType(const Package & package,
                    const Utf8String & name,
                    const Utf8String & description,
                    std::function<Dataset * (RestDirectory *,
                                             PolyConfig,
                                             const std::function<bool (const Json::Value)> &)>
                        createEntity,
                    TypeCustomRouteHandler docRoute,
                    TypeCustomRouteHandler customRoute,
                    std::shared_ptr<const ValueDescription> config,
                    std::set<std::string> registryFlags);

/** Register a new dataset kind.  This takes care of registering everything behind
    the scenes.
*/
template<typename DatasetT, typename Config>
std::shared_ptr<DatasetType>
registerDatasetType(const Package & package,
                    const Utf8String & name,
                    const Utf8String & description,
                    const Utf8String & docRoute,
                    TypeCustomRouteHandler customRoute = nullptr,
                    std::set<std::string> registryFlags = {})
{
    return registerDatasetType
        (package, name, description,
         [] (RestDirectory * server,
             PolyConfig config,
             const std::function<bool (const Json::Value)> & onProgress)
         {
             std::shared_ptr<spdlog::logger> logger = MLDB::getMldbLog<DatasetT>();
             ConvertProgressToJson convertProgressToJson(onProgress);
             auto dataset = new DatasetT(DatasetT::getOwner(server), config, convertProgressToJson);
             dataset->logger = std::move(logger); // noexcept
             return dataset;
         },
         makeInternalDocRedirect(package, docRoute),
         customRoute,
         getDefaultDescriptionSharedT<Config>(),
         registryFlags);
}

template<typename DatasetT, typename Config>
struct RegisterDatasetType {
    RegisterDatasetType(const Package & package,
                        const Utf8String & name,
                        const Utf8String & description,
                        const Utf8String & docRoute,
                        TypeCustomRouteHandler customRoute = nullptr,
                        std::set<std::string> registryFlags = {})
    {
        handle = registerDatasetType<DatasetT, Config>
            (package, name, description, docRoute, customRoute, registryFlags);
    }

    std::shared_ptr<DatasetType> handle;
};

} // namespace MLDB

