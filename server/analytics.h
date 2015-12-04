// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** analytics.h                                                    -*- C++ -*-
    Jeremy Barnes, 30 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Analytics queries for mldb.
*/

#pragma once

#include <vector>
#include <memory>
#include <string>
#include <functional>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/value_description.h"
#include "mldb/sql/sql_expression_operations.h"

namespace Datacratic {

struct Id;

namespace MLDB {

struct Dataset;
struct SqlExpression;
struct Function;
struct MatrixNamedRow;
struct MatrixNamedEvent;
struct ExpressionValue;
struct ColumnInfo;
struct KnownColumn;
struct OrderByExpression;
struct SelectExpression;
struct TupleExpression;
struct NamedRowValue;
struct SelectStatement;
struct SqlExpressionMldbContext;

extern const OrderByExpression ORDER_BY_NOTHING;

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
void iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    std::shared_ptr<SqlExpression> where,
                    std::function<bool (const NamedRowValue & output)> aggregator,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    std::function<bool (const Json::Value &)> onProgress);

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
void iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String& alias,
                    const WhenExpression & when,
                    std::shared_ptr<SqlExpression> where,
                    std::vector<std::shared_ptr<SqlExpression> > calc,
                    std::function<bool (const NamedRowValue & output,
                                        const std::vector<ExpressionValue> & calc)> aggregator,
                    const OrderByExpression & orderBy = ORDER_BY_NOTHING,
                    ssize_t offset = 0 /* start at start */,
                    ssize_t limit = -1 /* all */,
                    std::function<bool (const Json::Value &)> onProgress = nullptr);

/** Full select function, with grouping. */
void iterateDatasetGrouped(const SelectExpression & select,
                           const Dataset & from,
                           const Utf8String& alias,
                           const WhenExpression & when,
                           std::shared_ptr<SqlExpression> where,
                           const TupleExpression & groupBy,
                           const std::vector< std::shared_ptr<SqlExpression> >& aggregators,
                           const SqlExpression & having,
                           const SqlExpression & rowName,
                           std::function<bool (const NamedRowValue & output)> aggregator,
                           const OrderByExpression & orderBy = ORDER_BY_NOTHING,
                           ssize_t offset = 0 /* start at start */,
                           ssize_t limit = -1 /* all */,
                           std::function<bool (const Json::Value &)> onProgress = nullptr);


/** Create an embedding matrix, one embedding per row.  Returns both the embedding
    (tuples of row name, embedding, and extra parameters) and the
    list of variable info for the variables in the embedding.
*/
    std::pair<std::vector<std::tuple<RowHash, RowName, std::vector<double>, std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectExpression & select,
             const Dataset & from,
             const Utf8String& alias,
             const WhenExpression & when,
             std::shared_ptr<SqlExpression> where,
             std::vector<std::shared_ptr<SqlExpression> > calc,
             int maxDimensions = -1,
             const OrderByExpression & orderBy = ORDER_BY_NOTHING,
             int offset = 0,
             int limit = -1,
             const std::function<bool (const Json::Value &)> & onProgress = nullptr);

/* SELECT without FROM */
std::vector<MatrixNamedRow>
queryWithoutDataset(SelectStatement& stm, SqlExpressionMldbContext& mldbContext);

} // namespace MLDB
} // namespace Datacratic

