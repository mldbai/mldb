// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** forwarded_dataset.cc                                           -*- C++ -*-
    Jeremy Barnes, 6 October 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Implementation of forwarded dataset.
*/

#include "forwarded_dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {


/*****************************************************************************/
/* FORWARDED DATASET                                                         */
/*****************************************************************************/

ForwardedDataset::
ForwardedDataset(MldbServer * server)
    : Dataset(server)
{
}

/// Simple function to forward a pointer, checking it's not null.  Turns a
/// segfault into an exception.
template<typename Ptr>
const Ptr & notNull(const Ptr & p)
{
    ExcAssert(p);
    return p;
}

ForwardedDataset::
ForwardedDataset(std::shared_ptr<Dataset> forwardTo)
    : Dataset(notNull(forwardTo)->server),
      underlying(std::move(forwardTo))
{
}
    
ForwardedDataset::
~ForwardedDataset()
{
}

void
ForwardedDataset::
setUnderlying(std::shared_ptr<Dataset> underlying)
{
    /// We can only set the underlying dataset once, as we don't do any kind
    /// of locking.  Make sure that the user isn't misusing the dataset.
    ExcAssert(!this->underlying);
    this->underlying = std::move(underlying);
}

Any
ForwardedDataset::
getStatus() const
{
    ExcAssert(underlying);
    return underlying->getStatus();
}

void
ForwardedDataset::
recordRowItl(const RowPath & rowName,
             const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
{
    ExcAssert(underlying);
    underlying->recordRowItl(rowName, vals);
}

void
ForwardedDataset::
recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows)
{
    ExcAssert(underlying);
    underlying->recordRows(rows);
}

void
ForwardedDataset::
recordColumn(const ColumnPath & columnName,
             const std::vector<std::tuple<RowPath, CellValue, Date> > & vals)
{
    ExcAssert(underlying);
    underlying->recordColumn(columnName, vals);
}
    
void
ForwardedDataset::
recordColumns(const std::vector<std::pair<ColumnPath, std::vector<std::tuple<RowPath, CellValue, Date> > > > & cols)
{
    ExcAssert(underlying);
    underlying->recordColumns(cols);
}

KnownColumn
ForwardedDataset::
getKnownColumnInfo(const ColumnPath & columnName) const
{
    ExcAssert(underlying);
    return underlying->getKnownColumnInfo(columnName);
}

std::shared_ptr<RowValueInfo>
ForwardedDataset::
getRowInfo() const
{
    ExcAssert(underlying);
    return underlying->getRowInfo();
}

void
ForwardedDataset::
commit()
{
    ExcAssert(underlying);
    underlying->commit();
}

std::vector<MatrixNamedRow>
ForwardedDataset::
queryStructured(const SelectExpression & select,
                const WhenExpression & when,
                const SqlExpression & where,
                const OrderByExpression & orderBy,
                const TupleExpression & groupBy,
                const std::shared_ptr<SqlExpression> having,
                const std::shared_ptr<SqlExpression> rowName,
                ssize_t offset,
                ssize_t limit,
                Utf8String alias) const
{
    ExcAssert(underlying);
    return underlying->queryStructured(select, when, where, orderBy,
            groupBy, having, rowName, offset, limit, alias);
}

std::vector<MatrixNamedRow>
ForwardedDataset::
queryString(const Utf8String & query) const
{
    ExcAssert(underlying);
    return underlying->queryString(query);
}
    
Json::Value
ForwardedDataset::
selectExplainString(const Utf8String & select,
                    const Utf8String & where) const
{
    ExcAssert(underlying);
    return underlying->selectExplainString(select, where);
}

std::vector<ColumnPath>
ForwardedDataset::
getColumnPaths(ssize_t offset, ssize_t limit) const
{
    ExcAssert(underlying);
    return underlying->getColumnPaths(offset, limit);
}

BoundFunction
ForwardedDataset::
overrideFunction(const Utf8String & tableName, 
                 const Utf8String & functionName,
                 SqlBindingScope & context) const
{
    ExcAssert(underlying);
    return underlying->overrideFunction(tableName, functionName, context);
}

GenerateRowsWhereFunction
ForwardedDataset::
generateRowsWhere(const SqlBindingScope & context,
                  const Utf8String& alias,
                  const SqlExpression & where,
                  ssize_t offset,
                  ssize_t limit) const
{
    ExcAssert(underlying);
    return underlying->generateRowsWhere(context, alias, where, offset, limit);
}

BasicRowGenerator
ForwardedDataset::
queryBasic(const SqlBindingScope & context,
           const SelectExpression & select,
           const WhenExpression & when,
           const SqlExpression & where,
           const OrderByExpression & orderBy,
           ssize_t offset,
           ssize_t limit) const
{
    ExcAssert(underlying);
    return underlying->queryBasic(context, select, when, where, orderBy, offset, limit);
}

RestRequestMatchResult
ForwardedDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    ExcAssert(underlying);
    return underlying->handleRequest(connection, request, context);
}

void
ForwardedDataset::
getChildAliases(std::vector<Utf8String> & aliases) const
{
    ExcAssert(underlying);
    underlying->getChildAliases(aliases);
}

std::pair<Date, Date>
ForwardedDataset::
getTimestampRange() const
{
    ExcAssert(underlying);
    return underlying->getTimestampRange();
}

Date
ForwardedDataset::
quantizeTimestamp(Date timestamp) const
{
    ExcAssert(underlying);
    return underlying->quantizeTimestamp(timestamp);
}

std::shared_ptr<MatrixView>
ForwardedDataset::
getMatrixView() const
{
    ExcAssert(underlying);
    return underlying->getMatrixView();
}

std::shared_ptr<ColumnIndex>
ForwardedDataset::
getColumnIndex() const
{
    ExcAssert(underlying);
    return underlying->getColumnIndex();
}

std::shared_ptr<RowStream> 
ForwardedDataset::
getRowStream() const
{
    return underlying->getRowStream();
}


} // namespace MLDB

