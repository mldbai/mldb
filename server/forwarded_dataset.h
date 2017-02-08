/** forwarded_dataset.h                                            -*- C++ -*-
    Jeremy Barnes, 6 October 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that forwards to another, after setup is done.  Used to provide an
    adaptor on top of an existing dataset implementation.
*/

#include "mldb/core/dataset.h"

#pragma once


namespace MLDB {


/*****************************************************************************/
/* FORWARDED DATASET                                                         */
/*****************************************************************************/

/** This abstract dataset implementation forwards all operations to a concrete
    dataset implementation behind the scenes.  Note that the concrete
    implementation is not changeable after the initialization of the
    dataset.
*/

struct ForwardedDataset: public Dataset {

    ForwardedDataset(MldbServer * server);
    ForwardedDataset(std::shared_ptr<Dataset> underlying);

    virtual ~ForwardedDataset();

    /** Set the underlying dataset.  This can only be done once, at
        initialization time, as there is no locking associated with
        the pointer for performance reasons.
    */
    void setUnderlying(std::shared_ptr<Dataset> underlying);
    
    virtual Any getStatus() const;

    virtual void recordRowItl(const RowPath & rowName,
                              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    virtual void recordColumn(const ColumnPath & columnName,
                              const std::vector<std::tuple<RowPath, CellValue, Date> > & vals);
    
    virtual void recordColumns(const std::vector<std::pair<ColumnPath, std::vector<std::tuple<RowPath, CellValue, Date> > > > & rows);

    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const;

    virtual std::shared_ptr<RowValueInfo> getRowInfo() const;

    virtual void commit();

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

    virtual std::vector<MatrixNamedRow>
    queryString(const Utf8String & query) const;
    
    virtual Json::Value
    selectExplainString(const Utf8String & select,
                        const Utf8String & where) const;

    virtual std::vector<ColumnPath>
    getColumnPaths(ssize_t offset = 0, ssize_t limit = -1) const;

    virtual BoundFunction
    overrideFunction(const Utf8String & tableName, 
                     const Utf8String & functionName,
                     SqlBindingScope & context) const;

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const Utf8String& alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const;

    virtual BasicRowGenerator
    queryBasic(const SqlBindingScope & context,
               const SelectExpression & select,
               const WhenExpression & when,
               const SqlExpression & where,
               const OrderByExpression & orderBy,
               ssize_t offset,
               ssize_t limit) const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;


    virtual void getChildAliases(std::vector<Utf8String>&) const;

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

private:
    std::shared_ptr<Dataset> underlying;
};


} // namespace MLDB

