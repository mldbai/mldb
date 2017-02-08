/** analytics.h                                                    -*- C++ -*-
    Jeremy Barnes, 30 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Analytics queries for mldb.
*/

#pragma once

#include <vector>
#include <memory>
#include <string>
#include <functional>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/utils/progress.h"



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
struct SqlExpressionMldbScope;

extern const OrderByExpression ORDER_BY_NOTHING;

struct RowProcessor {
    std::function<bool (NamedRowValue & output)> processorfct;
    bool processInParallel;

    bool operator () (NamedRowValue & output) {return processorfct(output);}
};

struct RowProcessorExpr {
    std::function<bool (Path & rowName,
                        ExpressionValue & output,
                        std::vector<ExpressionValue> & calc)> processorfct;
    bool processInParallel;

    bool operator () (Path & rowName,
                      ExpressionValue & output,
                      std::vector<ExpressionValue> & calc)
    {
        return processorfct(rowName, output, calc);
    }
};

struct RowProcessorEx {
    std::function<bool (NamedRowValue & output, const std::vector<ExpressionValue> & calc)> processorfct;
    bool processInParallel;

    bool operator () (NamedRowValue & output, const std::vector<ExpressionValue> & calc) {return processorfct(output, calc);}
};

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    RowProcessor processor,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    const ProgressFunc & onProgress);

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDatasetExpr(const SelectExpression & select,
                        const Dataset & from,
                        const Utf8String & alias,
                        const WhenExpression & when,
                        const SqlExpression & where,
                        std::vector<std::shared_ptr<SqlExpression> > calc,
                        RowProcessorExpr processor,
                        const OrderByExpression & orderBy,
                        ssize_t offset,
                        ssize_t limit,
                        const ProgressFunc & onProgress);

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String& alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    std::vector<std::shared_ptr<SqlExpression> > calc,
                    RowProcessorEx processor,
                    const OrderByExpression & orderBy = ORDER_BY_NOTHING,
                    ssize_t offset = 0 /* start at start */,
                    ssize_t limit = -1 /* all */,
                    const ProgressFunc & onProgress = nullptr);

/** Full select function, with grouping. */
std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDatasetGrouped(const SelectExpression & select,
                           const Dataset & from,
                           const Utf8String& alias,
                           const WhenExpression & when,
                           const SqlExpression & where,
                           const TupleExpression & groupBy,
                           const std::vector< std::shared_ptr<SqlExpression> >& aggregators,
                           const SqlExpression & having,
                           const SqlExpression & rowName,
                           RowProcessor processor,
                           const OrderByExpression & orderBy = ORDER_BY_NOTHING,
                           ssize_t offset = 0 /* start at start */,
                           ssize_t limit = -1 /* all */,
                           const ProgressFunc & onProgress = nullptr);


/** Create an embedding matrix, one embedding per row.  Returns both the embedding
    (tuples of row name, embedding, and extra parameters) and the
    list of variable info for the variables in the embedding.
*/
std::pair<std::vector<std::tuple<RowHash, RowPath, std::vector<double>, std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectExpression & select,
             const Dataset & from,
             const Utf8String& alias,
             const WhenExpression & when,
             const SqlExpression & where,
             std::vector<std::shared_ptr<SqlExpression> > calc,
             int maxDimensions = -1,
             const OrderByExpression & orderBy = ORDER_BY_NOTHING,
             int offset = 0,
             int limit = -1,
             const ProgressFunc & onProgress = nullptr);

std::pair<std::vector<std::tuple<RowHash, RowPath, std::vector<double>, std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectStatement & stm,
             SqlExpressionMldbScope & context,
             int maxDimensions = -1,
             const ProgressFunc & onProgress = nullptr);

/** SELECT without FROM.
   
   WARNING: the SqlBindingScope must not require a row scope for ANY
   of the elements of the query, as it will be set up with an empty
   row scope.  If you get strange behaviour and segfaults running a
   query under this function, it's probably the case.  Most likely
   you should be running this function under an SqlExpressionMldbScope.
 */
std::vector<MatrixNamedRow>
queryWithoutDataset(const SelectStatement& stm, SqlBindingScope& scope);

std::tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
queryWithoutDatasetExpr(const SelectStatement& stm, SqlBindingScope& scope);

/** Select from the given statement.  This will choose the most
    appropriate execution method based upon what is in the query.

    The scope should be a clean scope, not requiring any row scope.
    See the comment above if you have errors inside this function.
*/
std::vector<MatrixNamedRow>
queryFromStatement(const SelectStatement & stm,
                   SqlBindingScope & scope,
                   BoundParameters params = nullptr,
                   const ProgressFunc & onProgress = nullptr);

std::tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
queryFromStatementExpr(const SelectStatement & stm,
                       SqlBindingScope & scope,
                       BoundParameters params = nullptr,
                       const ProgressFunc & onProgress = nullptr);

/** Select from the given statement.  This will choose the most
    appropriate execution method based upon what is in the query.

    The scope should be a clean scope, not requiring any row scope.
    See the comment above if you have errors inside this function.

    Will return the results one by one, and will stop when the
    onRow function returns false.  Returns false if one of the
    onRow calls returned false, or true otherwise.
*/
bool
queryFromStatement(std::function<bool (Path &, ExpressionValue &)> & onRow,
                   const SelectStatement & stm,
                   SqlBindingScope & scope,
                   BoundParameters params = nullptr,
                   const ProgressFunc & onProgress = nullptr);

/** Build a RowPath from an expression value and throw if
    it is not valid (row, empty, etc)
*/
RowPath getValidatedRowName(const ExpressionValue& rowNameEV);

} // namespace MLDB


