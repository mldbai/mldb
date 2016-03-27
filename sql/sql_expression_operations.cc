/** sql_expression_operations.cc
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "sql_expression_operations.h"
#include "mldb/server/function_contexts.h"
#include "mldb/http/http_exception.h"
#include <boost/algorithm/string.hpp>
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "table_expression_operations.h"
#include <unordered_set>
#include "mldb/server/dataset_context.h"
#include "mldb/base/scope.h"
#include "mldb/sql/sql_utils.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* COMPARISON EXPRESSION                                                     */
/*****************************************************************************/

ComparisonExpression::
ComparisonExpression(std::shared_ptr<SqlExpression> lhs,
                     std::shared_ptr<SqlExpression> rhs,
                     std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

ComparisonExpression::
~ComparisonExpression()
{
}

// Calculate the effective timstamps for an expression involving two
// operands.
static Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2)
{
    return std::max(v1.getEffectiveTimestamp(),
                    v2.getEffectiveTimestamp());
}

BoundSqlExpression
doComparison(const SqlExpression * expr,
             const BoundSqlExpression & boundLhs, const BoundSqlExpression & boundRhs,
             bool (ExpressionValue::* op)(const ExpressionValue &) const)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage, const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage, filter);
                const ExpressionValue & r = boundRhs(row, rstorage, filter);
                // cerr << "left " << l << " " << "right " << r << endl;
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = std::move(ExpressionValue::null(ts));
 
                return storage = std::move(ExpressionValue((l .* op)(r), ts));
            },
            expr,
            std::make_shared<BooleanValueInfo>()};
}

BoundSqlExpression
ComparisonExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs->bind(context);
    auto boundRhs = rhs->bind(context);

    if (op == "=" || op == "==") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator ==);
    }
    else if (op == "!=") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator !=);
    }
    else if (op == ">") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator > );
    }
    else if (op == "<") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator < );
    }
    else if (op == ">=") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator >=);
    }
    else if (op == "<=") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator <=);
    }
    else throw HttpReturnException(400, "Unknown comparison op " + op);
}

Utf8String
ComparisonExpression::
print() const
{
    return "compare(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
ComparisonExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ComparisonExpression>(*this);
    auto newArgs = transformArgs({ lhs, rhs });
    result->lhs = newArgs.at(0);
    result->rhs = newArgs.at(1);
    return result;
}

std::string
ComparisonExpression::
getType() const
{
    return "compare";
}

Utf8String
ComparisonExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
ComparisonExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}


/*****************************************************************************/
/* ARITHMETIC EXPRESSION                                                     */
/*****************************************************************************/

ArithmeticExpression::
ArithmeticExpression(std::shared_ptr<SqlExpression> lhs,
                     std::shared_ptr<SqlExpression> rhs,
                     std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

ArithmeticExpression::
~ArithmeticExpression()
{
}

template<typename Op>
struct BinaryOpHelper {

    // Context object for scalar operations on the LHS or RHS
    struct ScalarContext {
        template<typename RhsContext>
        static std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            return RhsContext::getInfoScalar(lhs, rhs);
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(std::shared_ptr<ExpressionValueInfo> lhs,
                      std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Scalar * scalar
            return Op::getInfo(lhs, rhs);
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(std::shared_ptr<ExpressionValueInfo> lhs,
                         std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Embedding * scalar
            auto inner = getValueInfoForStorage(lhs->getEmbeddingType());
            auto res = Op::getInfo(inner, rhs);
            return std::make_shared<EmbeddingValueInfo>(lhs->getEmbeddingShape(),
                                                        res->getEmbeddingType());
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoRow(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Row * scalar
            auto cols = lhs->getKnownColumns();
            for (auto & c: cols)
                c.valueInfo = Op::getInfo(c.valueInfo, rhs);
            return std::make_shared<RowValueInfo>(std::move(cols),
                                                  lhs->getSchemaCompleteness());
        }

        template<typename RhsContext>
        static const ExpressionValue &
        applyLhs(const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage)
        {
            return RhsContext::applyRhsScalar(lhs, rhs, storage);
        }

        static const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage)
        {
            // scalar * scalar; return it directly
            return storage
                = ExpressionValue(Op::apply(lhs.getAtom(),
                                            rhs.getAtom()),
                                  std::max(lhs.getEffectiveTimestamp(),
                                           rhs.getEffectiveTimestamp()));
        }

        static const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage)
        {
            // embedding * scalar
            std::vector<CellValue> lcells = lhs.getEmbeddingCell();
            const CellValue & r = rhs.getAtom();
            for (auto & c: lcells)
                c = Op::apply(c, r);
            Date ts = std::max(lhs.getEffectiveTimestamp(),
                               rhs.getEffectiveTimestamp());
            return storage = ExpressionValue(std::move(lcells), ts);
        }

        static const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage)
        {
            // row * scalar
            RowValue output;
            const CellValue & r = rhs.getAtom();
            Date rts = rhs.getEffectiveTimestamp();
            auto onVal = [&] (ColumnName columnName,
                              const ColumnName & prefix,
                              const CellValue & val,
                              Date ts)
                {
                    output.emplace_back(std::move(columnName),
                                        Op::apply(val, r),
                                        std::max(rts, ts));
                    return true;
                };
            lhs.forEachAtom(onVal);
            
            return storage = std::move(output);
        }
    };

    // Context object for embedding objects on the LHS or RHS
    struct EmbeddingContext {
        template<typename RhsContext>
        static std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            return RhsContext::getInfoEmbedding(lhs, rhs);
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(std::shared_ptr<ExpressionValueInfo> lhs,
                      std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Scalar * embedding
            auto inner = getValueInfoForStorage(rhs->getEmbeddingType());
            auto res = Op::getInfo(lhs, inner);
            return std::make_shared<EmbeddingValueInfo>(rhs->getEmbeddingShape(),
                                                        res->getEmbeddingType());
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(std::shared_ptr<ExpressionValueInfo> lhs,
                         std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Embedding * embedding
            auto innerl = getValueInfoForStorage(lhs->getEmbeddingType());
            auto innerr = getValueInfoForStorage(rhs->getEmbeddingType());
            auto res = Op::getInfo(innerl, innerr);
            return std::make_shared<EmbeddingValueInfo>(lhs->getEmbeddingShape(),
                                                        res->getEmbeddingType());
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoRow(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Row * embedding
            throw HttpReturnException(400, "Attempt to bind operation to "
                                      "embedding and row");
        }

        template<typename RhsContext>
        static const ExpressionValue &
        applyLhs(const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage)
        {
            // Extract the embedding.  Both must be embeddings.
            return RhsContext::applyRhsEmbedding(lhs, rhs, storage);
        }

        static const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage)
        {
            // Scalar * embedding
            std::vector<CellValue> rcells = rhs.getEmbeddingCell();
            const CellValue & l = lhs.getAtom();
            for (auto & c: rcells)
                c = Op::apply(l, c);
            Date ts = std::max(lhs.getEffectiveTimestamp(),
                               rhs.getEffectiveTimestamp());
            return storage = ExpressionValue(std::move(rcells), ts);
        }

        static const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage)
        {
            // embedding * embedding
            std::vector<CellValue> lcells = lhs.getEmbeddingCell();
            std::vector<CellValue> rcells = rhs.getEmbeddingCell();

            if (lcells.size() != rcells.size())
                throw HttpReturnException
                    (400, "Attempt to perform operation on incompatible shaped "
                     "embeddings",
                     "lhsShape", lhs.getEmbeddingShape(),
                     "rhsShape", rhs.getEmbeddingShape());
                     
            for (size_t i = 0;  i < lcells.size();  ++i)
                lcells[i] = Op::apply(lcells[i], rcells[i]);
                     
            Date ts = std::max(lhs.getEffectiveTimestamp(),
                               rhs.getEffectiveTimestamp());

            return storage = ExpressionValue(std::move(lcells), ts,
                                             lhs.getEmbeddingShape());
        }

        static const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage)
        {
            throw HttpReturnException(400, "Attempt to apply operation to "
                                      "embedding and row");
        }
    };

    // Context object for rows on the LHS or RHS
    struct RowContext {
        template<typename RhsContext>
        static std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            return RhsContext::getInfoRow(lhs, rhs);
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(std::shared_ptr<ExpressionValueInfo> lhs,
                      std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Scalar * row
            auto cols = rhs->getKnownColumns();
            for (auto & c: cols)
                c.valueInfo = Op::getInfo(rhs, c.valueInfo);
            return std::make_shared<RowValueInfo>(std::move(cols),
                                                  rhs->getSchemaCompleteness());
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(std::shared_ptr<ExpressionValueInfo> lhs,
                         std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Embedding * row
            throw HttpReturnException(400, "Attempt to bind operation to "
                                      "row and embedding");
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoRow(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Row * row
            auto onInfo = [] (const ColumnName &,
                              std::shared_ptr<ExpressionValueInfo> lhsInfo,
                              std::shared_ptr<ExpressionValueInfo> rhsInfo)
                {
                    static std::shared_ptr<ExpressionValueInfo> nullInfo
                        = std::make_shared<EmptyValueInfo>();

                    if (!lhsInfo)
                        lhsInfo = nullInfo;
                    if (!rhsInfo)
                        lhsInfo = nullInfo;
                    return Op::getInfo(lhsInfo, rhsInfo);
                };

            return ExpressionValueInfo::getMerged(lhs, rhs, onInfo);
        }

        template<typename RhsContext>
        static const ExpressionValue &
        applyLhs(const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage)
        {
            return RhsContext::applyRhsRow(lhs, rhs, storage);
        }

        static const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage)
        {
            // Scalar * row
            RowValue output;
            const CellValue & l = lhs.getAtom();
            Date lts = lhs.getEffectiveTimestamp();
            auto onVal = [&] (ColumnName columnName,
                              const ColumnName & prefix,
                              const CellValue & val,
                              Date ts)
                {
                    output.emplace_back(std::move(columnName),
                                        Op::apply(l, val),
                                        std::max(lts, ts));
                    return true;
                };
            rhs.forEachAtom(onVal);

            return storage = std::move(output);
        }

        static const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage)
        {
            return applyRhsRow(lhs, rhs, storage);
        }

        static const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage)
        {
            // row * row
            RowValue output;

            auto onColumn = [&] (ColumnName columnName,
                                 std::pair<CellValue, Date> * vals1,
                                 std::pair<CellValue, Date> * vals2,
                                 size_t n1, size_t n2)
                {
                    if (n1 == 1 && n2 == 1) {
                        // Common case; one of each value
                        output.emplace_back(std::move(columnName),
                                            Op::apply(vals1[0].first,
                                                      vals2[0].first),
                                            std::max(vals1[0].second,
                                                     vals2[0].second));
                    }
                    else if (n1 == 0) {
                        // Left value is null
                        for (size_t j = 0;  j < n2;  ++j) {
                            output.emplace_back(columnName,
                                                Op::apply(CellValue(),
                                                          vals2[j].first),
                                                vals2[j].second);
                        }
                    }
                    else if (n2 == 0) {
                        // Right value is null
                        for (size_t i = 0;  i < n1;  ++i) {
                            output.emplace_back(columnName,
                                                Op::apply(vals1[i].first,
                                                          CellValue()),
                                                vals1[i].second);
                        }
                    }
                    else {
                        // Multiple values for each. Calculate each
                        // combination.  Note that we could potentially
                        // do this at each time step rather than for
                        // every possible combination as we do here
                        for (size_t i = 0;  i < n1;  ++i) {
                            for (size_t j = 0;  j < n2;  ++j) {
                                output.emplace_back(columnName,
                                                    Op::apply(vals1[i].first,
                                                              vals2[j].first),
                                                    std::max(vals1[i].second,
                                                             vals2[j].second));
                            }
                        }
                    }
                        
                    return true;
                };

            ExpressionValue::joinColumns(lhs, rhs, onColumn,
                                         ExpressionValue::OUTER);
            
            return storage = std::move(output);
        }

    };

    // Context object for unknown types on the LHS or RHS.  They
    // switch at runtime.
    struct UnknownContext {
        template<typename RhsContext>
        static std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            return std::make_shared<AnyValueInfo>();
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(std::shared_ptr<ExpressionValueInfo> lhs,
                      std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Scalar * unknown.  No idea if it's scalar or vector
            // until runtime.
            return std::make_shared<AnyValueInfo>();
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoUnknown(std::shared_ptr<ExpressionValueInfo> lhs,
                       std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // unknown * unknown.  No idea if it's scalar or vector
            // until runtime.
            return std::make_shared<AnyValueInfo>();
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(std::shared_ptr<ExpressionValueInfo> lhs,
                         std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Embedding * unknown.  It must be an embedding.
            auto inner = getValueInfoForStorage(lhs->getEmbeddingType());
            auto res = Op::getInfo(inner, rhs);
            return std::make_shared<EmbeddingValueInfo>(lhs->getEmbeddingShape(),
                                                        res->getEmbeddingType());
        }

        static std::shared_ptr<ExpressionValueInfo>
        getInfoRow(std::shared_ptr<ExpressionValueInfo> lhs,
                   std::shared_ptr<ExpressionValueInfo> rhs)
        {
            // Row * unknown.  It must be a row
            auto cols = lhs->getKnownColumns();
            for (auto & c: cols)
                c.valueInfo = Op::getInfo(c.valueInfo, rhs);
            return std::make_shared<RowValueInfo>(std::move(cols),
                                                  lhs->getSchemaCompleteness());
        }

        template<typename RhsContext>
        static const ExpressionValue &
        applyLhs(const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage)
        {
            if (lhs.isAtom()) {
                return ScalarContext::template applyLhs<RhsContext>(lhs, rhs, storage);
            }
            else if (lhs.isEmbedding()) {
                return EmbeddingContext::template applyLhs<RhsContext>(lhs, rhs, storage);
            }
            else if (lhs.isRow()) {
                return RowContext::template applyLhs<RhsContext>(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

        static const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage)
        {
            if (rhs.isAtom()) {
                return ScalarContext::applyRhsScalar(lhs, rhs, storage);
            }
            else if (rhs.isEmbedding()) {
                return EmbeddingContext::applyRhsScalar(lhs, rhs, storage);
            }
            else if (rhs.isRow()) {
                return RowContext::applyRhsScalar(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

        static const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage)
        {
            if (rhs.isAtom()) {
                return ScalarContext::applyRhsEmbedding(lhs, rhs, storage);
            }
            else if (rhs.isEmbedding()) {
                return EmbeddingContext::applyRhsEmbedding(lhs, rhs, storage);
            }
            else if (rhs.isRow()) {
                return RowContext::applyRhsEmbedding(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

        static const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage)
        {
            if (rhs.isAtom()) {
                return ScalarContext::applyRhsRow(lhs, rhs, storage);
            }
            else if (rhs.isEmbedding()) {
                return EmbeddingContext::applyRhsRow(lhs, rhs, storage);
            }
            else if (rhs.isRow()) {
                return RowContext::applyRhsRow(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

    };

    template<class LhsContext, class RhsContext>
    static const ExpressionValue &
    apply(const BoundSqlExpression & boundLhs,
          const BoundSqlExpression & boundRhs,
          const SqlRowScope & row, ExpressionValue & storage, 
          const VariableFilter & filter)
    {
        ExpressionValue lstorage, rstorage;
        const ExpressionValue & lhs = boundLhs(row, lstorage, filter);
        const ExpressionValue & rhs = boundRhs(row, rstorage, filter);
        
        return LhsContext::template applyLhs<RhsContext>(lhs, rhs, storage);
    }
    
    template<class LhsContext, class RhsContext>
    static BoundSqlExpression
    bindAll(const SqlExpression * expr,
            const BoundSqlExpression & boundLhs,
            const BoundSqlExpression & boundRhs)
    {
        BoundSqlExpression result;
        result.exec = std::bind(apply<LhsContext, RhsContext>,
                                boundLhs,
                                boundRhs,
                                std::placeholders::_1,
                                std::placeholders::_2,
                                GET_LATEST);
        result.info = LhsContext::template getInfoLhs<RhsContext>(boundLhs.info,
                                                                  boundRhs.info);
        return result;
    }

    template<class LhsContext>
    static BoundSqlExpression
    bindRhs(const SqlExpression * expr,
            const BoundSqlExpression & boundLhs,
            const BoundSqlExpression & boundRhs)
    {
        if (boundRhs.info->isScalar()) {
            return bindAll<LhsContext, ScalarContext>(expr, boundLhs, boundRhs);
        }
        else if (boundLhs.info->isEmbedding()) {
            return bindAll<LhsContext, EmbeddingContext>(expr, boundLhs, boundRhs);
        }
        else if (boundLhs.info->isRow()) {
            return bindAll<LhsContext, RowContext>(expr, boundLhs, boundRhs);
        }
        else {
            return bindAll<LhsContext, UnknownContext>(expr, boundLhs, boundRhs);
        }
        
    }

    static BoundSqlExpression
    bind(const SqlExpression * expr,
         const BoundSqlExpression & boundLhs,
         const BoundSqlExpression & boundRhs)
    {
        if (boundLhs.info->isScalar()) {
            return bindRhs<ScalarContext>(expr, boundLhs, boundRhs);
        }
        else if (boundLhs.info->isEmbedding()) {
            return bindRhs<EmbeddingContext>(expr, boundLhs, boundRhs);
        }
        else if (boundLhs.info->isRow()) {
            return bindRhs<RowContext>(expr, boundLhs, boundRhs);
        }
        else {
            return bindRhs<UnknownContext>(expr, boundLhs, boundRhs);
        }
    }
};

template<typename ReturnInfo, typename Op>
BoundSqlExpression
doBinaryArithmetic(const SqlExpression * expr,
                   const BoundSqlExpression & boundLhs,
                   const BoundSqlExpression & boundRhs,
                   const Op & op)
{
    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage, filter);
                const ExpressionValue & r = boundRhs(row, rstorage, filter);
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = std::move(ExpressionValue::null(ts));
                return storage = std::move(ExpressionValue(op(l.getAtom(),
                                                              r.getAtom()), ts));
            },
            expr,
            std::make_shared<ReturnInfo>()};
}

template<typename ReturnInfo, typename Op>
BoundSqlExpression
doUnaryArithmetic(const SqlExpression * expr,
                  const BoundSqlExpression & boundRhs,
                  const Op & op)
{
    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue rstorage;
                const ExpressionValue & r = boundRhs(row, rstorage, filter);
                if (r.empty())
                    return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                return storage
                    = ExpressionValue(std::move(op(r.getAtom())),
                                      r.getEffectiveTimestamp());
            },
            expr,
            std::make_shared<ReturnInfo>()};
}

static CellValue
binaryPlusOnTimestamp(const CellValue & l, const CellValue & r)
{
    ExcAssert(l.isTimestamp());

    if (r.isInteger())
    {
        //when no interval is specified (integer), operation is done in days
        return std::move(CellValue(l.toTimestamp().addDays(r.toInt())));
    }
    else if (r.isTimeinterval())
    {
        //date + interval will give a date
        uint16_t months = 0, days = 0;
        float seconds = 0;

        std::tie(months, days, seconds) = r.toMonthDaySecond();

        if (seconds < 0)
            return std::move(CellValue(l.toTimestamp().minusMonthDaySecond(months, days, fabs(seconds))));
        else
            return std::move(CellValue(l.toTimestamp().plusMonthDaySecond(months, days, seconds)));
    }

    throw HttpReturnException(400, "Adding unsupported type to timetamp");

    return std::move(CellValue(l.toTimestamp()));

}

static CellValue binaryPlus(const CellValue & l, const CellValue & r)
{
    if (l.isString() || r.isString())
    {
       return std::move(CellValue(l.toUtf8String() + r.toUtf8String())); 
    }
    else if (l.isTimestamp())
    {
        return binaryPlusOnTimestamp(l, r);
    }
    else if (r.isTimestamp())
    {
        // + is commutative
        return binaryPlusOnTimestamp(r, l);
    }
    else if (l.isTimeinterval())
    {
        int64_t lmonths = 0, ldays = 0, rmonths = 0, rdays = 0;
        float lseconds = 0, rseconds = 0;

        std::tie(lmonths, ldays, lseconds) = l.toMonthDaySecond();
        std::tie(rmonths, rdays, rseconds) = r.toMonthDaySecond();

        lmonths += rmonths;
        ldays += rdays;
        lseconds += rseconds;

        //no implicit quantization;
        
        return std::move(CellValue::fromMonthDaySecond(lmonths, ldays, lseconds));
    }
    else
    {
        return std::move(CellValue(l.toDouble() + r.toDouble()));
    }
}

static CellValue binaryMinusOnTimestamp(const CellValue & l, const CellValue & r)
{
    ExcAssert(l.isTimestamp());

    if (r.isInteger())
    {
        //when no interval is specified (integer), operation is done in days
        return std::move(CellValue(l.toTimestamp().addDays(-r.toInt())));
    }
    else if (r.isTimeinterval())
    {
        //date - interval will give a date
        //date + interval will give a date
        int64_t months = 0, days = 0;
        float seconds = 0;

        std::tie(months, days, seconds) = r.toMonthDaySecond();

        if (seconds >= 0)
            return std::move(CellValue(l.toTimestamp().minusMonthDaySecond(months, days, fabs(seconds))));
        else
            return std::move(CellValue(l.toTimestamp().plusMonthDaySecond(months, days, seconds)));
    }
    else if (r.isTimestamp())
    {
        //date - date gives us an interval
        int64_t days = 0;
        float seconds = 0;
        std::tie(days, seconds) = l.toTimestamp().getDaySecondInterval(r.toTimestamp());
        return std::move(CellValue::fromMonthDaySecond(0, days, seconds));
    }

    throw HttpReturnException(400, "Substracting unsupported type to timetamp");
    return std::move(CellValue(l.toTimestamp()));

}

static CellValue binaryMinus(const CellValue & l, const CellValue & r)
{
    if (l.isTimestamp())
    {
        return binaryMinusOnTimestamp(l, r);
    }
    else if (l.isTimeinterval() && r.isTimeinterval())
    {
        int64_t lmonths = 0, ldays = 0, rmonths = 0, rdays = 0;
        float lseconds = 0, rseconds = 0;

        std::tie(lmonths, ldays, lseconds) = l.toMonthDaySecond();
        std::tie(rmonths, rdays, rseconds) = r.toMonthDaySecond();

        lmonths -= rmonths;
        ldays -= rdays;
        lseconds -= rseconds;

        //no implicit quantization;
        
        return std::move(CellValue::fromMonthDaySecond(lmonths, ldays, lseconds));
    }

    return std::move(CellValue(l.toDouble() - r.toDouble()));
}

static CellValue unaryMinus(const CellValue & r)
{
    if (r.isInteger())
        return -r.toInt();
    else if (r.isTimeinterval())
    {
        int64_t months = 0, days = 0;
        float seconds = 0.0f;

        std::tie(months, days, seconds) = r.toMonthDaySecond();

        if (seconds == 0.0f)
            seconds = -0.0f;
        else if (seconds == -0.0f)
            seconds = 0.0f;
        else 
            seconds = -seconds;

        return std::move(CellValue::fromMonthDaySecond(months, days, seconds));
    }
    else return -r.toDouble();
}

static CellValue multiplyInterval(const CellValue & l, double rvalue)
{
    int64_t months, days;
    float seconds;

    std::tie(months, days, seconds) = l.toMonthDaySecond();
    months *= rvalue;

    double ddays = days * rvalue;    
    double fractionalDays = modf(ddays, &ddays); //carry the remainder of days into hours/minute/seconds.
    days = ddays;
    
    seconds *= rvalue;
    seconds += fractionalDays*24.0f*60*60;

    return std::move(CellValue::fromMonthDaySecond(months, days, seconds));
}

static CellValue binaryMultiplication(const CellValue & l, const CellValue & r)
{
    if (l.isTimeinterval() && r.isNumber())
    {
        return multiplyInterval(l,r.toDouble());
    }
    else if (r.isTimeinterval() && l.isNumber())
    {
        return multiplyInterval(r,l.toDouble());
    }
    else return l.toDouble() * r.toDouble();
}

static CellValue binaryDivision(const CellValue & l, const CellValue & r)
{
    if (l.isTimeinterval() && r.isNumber())
    {
        int64_t months, days;
        float seconds;

        double rvalue = r.toDouble();

        std::tie(months, days, seconds) = l.toMonthDaySecond();
        months /= rvalue;

        double ddays = days / rvalue;    
        double fractionalDays = modf(ddays, &ddays); //carry the remainder of days into hours/minute/seconds.
        days = ddays;
        
        seconds /= rvalue;
        seconds += fractionalDays*24.0f*60*60;

        return std::move(CellValue::fromMonthDaySecond(months, ddays, seconds));
    }
    else return l.toDouble() / r.toDouble();
}

template<class T1, class T2>
CellValue safeIntegerMod(const T1 a, const T2 b)
{
    if (b == 0)
    {
        throw HttpReturnException(400, "Integer Modulus by a zero dividend");
    }
    else
    {
        return a % b;
    }
}

static CellValue binaryModulus(const CellValue & la, const CellValue & ra)
{
    if (la.isInteger() && ra.isInteger()) {
        if (la.isUInt64() && ra.isUInt64()) {
            return safeIntegerMod<uint64_t, uint64_t>(la.toUInt(), ra.toUInt());
        }
        else if (la.isUInt64()) {
            return safeIntegerMod<uint64_t, int64_t>(la.toUInt(), ra.toInt());
        }
        else if (ra.isUInt64()) {
            return safeIntegerMod<int64_t, uint64_t>(la.toInt(), ra.toUInt());
        }
        else {
            return safeIntegerMod<int64_t, int64_t>(la.toInt(), ra.toInt());
        }
    }
    else return fmod(la.toDouble(), ra.toDouble());
}

struct BinaryPlusOp {
    static CellValue apply(const CellValue & l, const CellValue & r)
    {
        if (l.empty() || r.empty())
            return l;
        return binaryPlus(l, r);
    }

    static std::shared_ptr<ExpressionValueInfo>
    getInfo(const std::shared_ptr<ExpressionValueInfo> & lhs,
            const std::shared_ptr<ExpressionValueInfo> & rhs)
    {
        return std::make_shared<AtomValueInfo>();
    }
};

struct BinaryMinusOp {
    static CellValue apply(const CellValue & l, const CellValue & r)
    {
        if (l.empty() || r.empty())
            return l;
        return binaryMinus(l, r);
    }

    static std::shared_ptr<ExpressionValueInfo>
    getInfo(const std::shared_ptr<ExpressionValueInfo> & lhs,
            const std::shared_ptr<ExpressionValueInfo> & rhs)
    {
        return std::make_shared<AtomValueInfo>();
    }
};

struct BinaryMultiplicationOp {
    static CellValue apply(const CellValue & l, const CellValue & r)
    {
        if (l.empty() || r.empty())
            return l;
        return binaryMultiplication(l, r);
    }

    static std::shared_ptr<ExpressionValueInfo>
    getInfo(const std::shared_ptr<ExpressionValueInfo> & lhs,
            const std::shared_ptr<ExpressionValueInfo> & rhs)
    {
        return std::make_shared<AtomValueInfo>();
    }
};

struct BinaryDivisionOp {
    static CellValue apply(const CellValue & l, const CellValue & r)
    {
        if (l.empty() || r.empty())
            return l;
        return binaryDivision(l, r);
    }

    static std::shared_ptr<ExpressionValueInfo>
    getInfo(const std::shared_ptr<ExpressionValueInfo> & lhs,
            const std::shared_ptr<ExpressionValueInfo> & rhs)
    {
        return std::make_shared<AtomValueInfo>();
    }
};

struct BinaryModulusOp {
    static CellValue apply(const CellValue & l, const CellValue & r)
    {
        if (l.empty() || r.empty())
            return l;
        return binaryModulus(l, r);
    }

    static std::shared_ptr<ExpressionValueInfo>
    getInfo(const std::shared_ptr<ExpressionValueInfo> & lhs,
            const std::shared_ptr<ExpressionValueInfo> & rhs)
    {
        return std::make_shared<AtomValueInfo>();
    }
};

BoundSqlExpression
ArithmeticExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs ? lhs->bind(context) : BoundSqlExpression();
    auto boundRhs = rhs->bind(context);

    if (op == "+" && lhs) {
        return BinaryOpHelper<BinaryPlusOp>::bind(this, boundLhs, boundRhs);
    }
    else if (op == "-" && lhs) {
        return BinaryOpHelper<BinaryMinusOp>::bind(this, boundLhs, boundRhs);
    }
    else if (op == "-" && !lhs) {
        return doUnaryArithmetic<AtomValueInfo>(this, boundRhs, &unaryMinus);
    }
    else if (op == "*" && lhs) {
        return BinaryOpHelper<BinaryMultiplicationOp>
            ::bind(this, boundLhs, boundRhs);
    }
    else if (op == "/" && lhs) {
        return BinaryOpHelper<BinaryDivisionOp>
            ::bind(this, boundLhs, boundRhs);
    }
    else if (op == "%" && lhs) {
        return BinaryOpHelper<BinaryModulusOp>
            ::bind(this, boundLhs, boundRhs);
    }
    else throw HttpReturnException(400, "Unknown arithmetic op " + op
                                   + (lhs ? " binary" : " unary"));
}

Utf8String
ArithmeticExpression::
print() const
{
    if (lhs)
        return "arith(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
    else
        return "arith(\"" + op + "\"," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
ArithmeticExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ArithmeticExpression>(*this);
    if (lhs) {
        auto newArgs = transformArgs({ lhs, rhs });
        result->lhs = newArgs.at(0);
        result->rhs = newArgs.at(1);
    }
    else {
        auto newArgs = transformArgs({ rhs });
        result->rhs = newArgs.at(0);
    }
    return result;
}

std::string
ArithmeticExpression::
getType() const
{
    return "arith";
}

Utf8String
ArithmeticExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
ArithmeticExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}


/*****************************************************************************/
/* BITWISE EXPRESSION                                                        */
/*****************************************************************************/

BitwiseExpression::
BitwiseExpression(std::shared_ptr<SqlExpression> lhs,
                     std::shared_ptr<SqlExpression> rhs,
                     std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

BitwiseExpression::
~BitwiseExpression()
{
}

template<typename Op>
BoundSqlExpression
doBinaryBitwise(const SqlExpression * expr,
                const BoundSqlExpression & boundLhs, const BoundSqlExpression & boundRhs,
                const Op & op)
{
    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage, filter);
                const ExpressionValue & r = boundRhs(row, rstorage, filter);
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = std::move(ExpressionValue::null(ts));
                return storage = std::move(ExpressionValue(op(l.toInt(), r.toInt()), ts));
            },
            expr,
            std::make_shared<IntegerValueInfo>()};
}

template<typename Op>
BoundSqlExpression
doUnaryBitwise(const SqlExpression * expr,
               const BoundSqlExpression & boundRhs,
               const Op & op)
{
    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue rstorage;
                const ExpressionValue & r = boundRhs(row, rstorage, filter);
                if (r.empty())
                    return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                return storage = std::move(ExpressionValue(std::move(op(r.toInt())), r.getEffectiveTimestamp()));
            },
            expr,
            std::make_shared<IntegerValueInfo>()};
}

static CellValue doBitwiseAnd(int64_t v1, int64_t v2)
{
    return v1 & v2;
}

static CellValue doBitwiseOr(int64_t v1, int64_t v2)
{
    return v1 | v2;
}

static CellValue doBitwiseXor(int64_t v1, int64_t v2)
{
    return v1 | v2;
}

static CellValue doBitwiseNot(int64_t v1)
{
    return ~v1;
}

BoundSqlExpression
BitwiseExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs ? lhs->bind(context) : BoundSqlExpression();
    auto boundRhs = rhs->bind(context);

    if (op == "&" && lhs) {
        return doBinaryBitwise(this, boundLhs, boundRhs, &doBitwiseAnd);
    }
    else if (op == "|" && lhs) {
        return doBinaryBitwise(this, boundLhs, boundRhs, &doBitwiseOr);
    }
    else if (op == "^" && lhs) {
        return doBinaryBitwise(this, boundLhs, boundRhs, &doBitwiseXor);
    }
    else if (op == "~" && !lhs) {
        return doUnaryBitwise(this, boundRhs, &doBitwiseNot);
    }
    else throw HttpReturnException(400, "Unknown bitwise op " + op
                                   + (lhs ? " binary" : " unary"));
}

Utf8String
BitwiseExpression::
print() const
{
    if (lhs)
        return "bitwise(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
    else
        return "bitwise(\"" + op + "\"," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
BitwiseExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BitwiseExpression>(*this);
    if (lhs) {
        auto newArgs = transformArgs({ lhs, rhs });
        result->lhs = newArgs.at(0);
        result->rhs = newArgs.at(1);
    }
    else {
        auto newArgs = transformArgs({ rhs });
        result->rhs = newArgs.at(0);
    }
    return result;
}

std::string
BitwiseExpression::
getType() const
{
    return "bitwise";
}

Utf8String
BitwiseExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
BitwiseExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}

/*****************************************************************************/
/* READ VARIABLE EXPRESSION                                                  */
/*****************************************************************************/

ReadVariableExpression::
ReadVariableExpression(Utf8String tableName, Utf8String variableName)
    : tableName(std::move(tableName)), variableName(std::move(variableName))
{
}

ReadVariableExpression::
~ReadVariableExpression()
{
}

BoundSqlExpression
ReadVariableExpression::
bind(SqlBindingScope & context) const
{
    auto getVariable = context.doGetVariable(tableName, variableName);

    if (!getVariable.info) {
        throw HttpReturnException(400, "context " + ML::type_name(context)
                            + " getVariable '" + variableName
                            + "' didn't return info");
    }

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                // TODO: allow it access to storage
                return getVariable(row, storage, filter);
            },
            this,
            getVariable.info};
}

Utf8String
ReadVariableExpression::
print() const
{
    return "variable(\"" + variableName + "\")";
}

std::shared_ptr<SqlExpression>
ReadVariableExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ReadVariableExpression>(*this);
    return result;
}

std::string
ReadVariableExpression::
getType() const
{
    return "variable";
}

Utf8String
ReadVariableExpression::
getOperation() const
{
    return variableName;
}

std::vector<std::shared_ptr<SqlExpression> >
ReadVariableExpression::
getChildren() const
{
    return {};
}

std::map<ScopedName, UnboundVariable>
ReadVariableExpression::
variableNames() const
{
    return { { { tableName, variableName }, { std::make_shared<AnyValueInfo>() } } };
}


/*****************************************************************************/
/* CONSTANT EXPRESSION                                                       */
/*****************************************************************************/

ConstantExpression::
ConstantExpression(ExpressionValue constant)
    : constant(constant)
{
}

ConstantExpression::
~ConstantExpression()
{
}

BoundSqlExpression
ConstantExpression::
bind(SqlBindingScope & context) const
{
    ExpressionValue val = constant;

    return {[=] (const SqlRowScope &,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                return storage=val;
            },
            this,
            constant.getSpecializedValueInfo(),
            true /* is constant */};
}

Utf8String
ConstantExpression::
print() const
{
    return "constant(" + jsonEncodeUtf8(constant) + ")";
}

std::shared_ptr<SqlExpression>
ConstantExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ConstantExpression>(*this);
    return result;
}

bool
ConstantExpression::
isConstant() const
{
    return true;
}

ExpressionValue
ConstantExpression::
constantValue() const
{
    return constant;
}

std::string
ConstantExpression::
getType() const
{
    return "constant";
}

Utf8String
ConstantExpression::
getOperation() const
{
    return jsonEncodeUtf8(constant);
}

std::vector<std::shared_ptr<SqlExpression> >
ConstantExpression::
getChildren() const
{
    return {};
}


/*****************************************************************************/
/* SELECT WITHIN EXPRESSION                                                  */
/*****************************************************************************/

SelectWithinExpression::
SelectWithinExpression(std::shared_ptr<SqlRowExpression> select)
    : select(select)
{
}

SelectWithinExpression::
~SelectWithinExpression()
{
}

BoundSqlExpression
SelectWithinExpression::
bind(SqlBindingScope & context) const
{
    return select->bind(context);
}

Utf8String
SelectWithinExpression::
print() const
{
    return "select(" + select->print() + ")";
}

std::shared_ptr<SqlExpression>
SelectWithinExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<SelectWithinExpression>
        (std::dynamic_pointer_cast<SqlRowExpression>(select->transform(transformArgs)));
    return result;
}

std::string
SelectWithinExpression::
getType() const
{
    return "select";
}

Utf8String
SelectWithinExpression::
getOperation() const
{
    return select->print();
}

std::vector<std::shared_ptr<SqlExpression> >
SelectWithinExpression::
getChildren() const
{
    auto result = select->getChildren();
    result.push_back(select);
    return result;
}


/*****************************************************************************/
/* EMBEDDING EXPRESSION                                                      */
/*****************************************************************************/

EmbeddingLiteralExpression::
EmbeddingLiteralExpression(vector<std::shared_ptr<SqlExpression> >& clauses)
    : clauses(clauses)
{
}

EmbeddingLiteralExpression::
~EmbeddingLiteralExpression()
{
}

BoundSqlExpression
EmbeddingLiteralExpression::
bind(SqlBindingScope & context) const
{
    if (clauses.empty()) {
        return BoundSqlExpression([] (const SqlRowScope &, 
                                      ExpressionValue & storage, 
                                      const VariableFilter & filter) 
                                  { return storage = std::move(ExpressionValue::null(Date::notADate())); },
                                  this,
                                  std::make_shared<EmptyValueInfo>(),
                                  true /* is constant */);
    }

    vector<BoundSqlExpression> boundClauses;
    vector<size_t> knownDims = {clauses.size()};

    std::vector<std::shared_ptr<ExpressionValueInfo> > clauseInfo;

    for (auto & c: clauses) {
        boundClauses.emplace_back(std::move(c->bind(context)));
        clauseInfo.push_back(boundClauses.back().info);
    }   

    auto outputInfo
        = std::make_shared<EmbeddingValueInfo>(clauseInfo);

    if (outputInfo->shape.at(0) != -1)
        ExcAssertEqual(outputInfo->shape.at(0), clauses.size());

    bool lastLevel = outputInfo->numDimensions() == 1;

    auto exec = [=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {  
            Date ts = Date::negativeInfinity();
            std::vector<CellValue> cells;

            if (lastLevel) {
                cells.reserve(boundClauses.size());

                for (auto & c: boundClauses) {
                    ExpressionValue v = c(context, filter);
                    ts.setMax(v.getEffectiveTimestamp());
                    cells.emplace_back(v.stealAtom());
                }

                ExpressionValue result(std::move(cells), ts);
                return storage = std::move(result);
            }
            else {

                std::vector<size_t> dims = { boundClauses.size() };

                for (unsigned i = 0;  i < boundClauses.size();  ++i) {
                    auto & c = boundClauses[i];
                    ExpressionValue v = c(context, GET_LATEST);
                    
                    // Get the number of dimensions in the embedding
                    if (i == 0) {
                        for (size_t d: v.getEmbeddingShape())
                            dims.emplace_back(d);
                    }
                    // TODO: check for the correct size of cells

                    ts.setMin(v.getEffectiveTimestamp());

                    std::vector<CellValue> valueCells
                        = v.getEmbeddingCell(v.rowLength());

                    if (valueCells.size() != v.rowLength())
                        throw HttpReturnException(400, "Embeddings don't contain the same number of elements");

                    cells.insert(cells.end(),
                                 std::make_move_iterator(valueCells.begin()),
                                 std::make_move_iterator(valueCells.end()));
                }

                // This one weird trick will convert a vector<X> to a
                // shared_ptr<X> without copying.
                auto dataOwner = std::make_shared<std::vector<CellValue> >(std::move(cells));
                std::shared_ptr<const CellValue> data(dataOwner, &(*dataOwner)[0]);
                
                return storage = ExpressionValue::embedding(ts, std::move(data), ST_ATOM, dims);
            }
        };

    return BoundSqlExpression(exec, this, outputInfo, false);
}

Utf8String
EmbeddingLiteralExpression::
print() const
{
    Utf8String output =  "embed[" + clauses[0]->print();

    for (int i = 1; i < clauses.size(); ++i)
    {
        output += "," + clauses[i]->print();
    }

    output += "]";

    return output;
}

std::shared_ptr<SqlExpression>
EmbeddingLiteralExpression::
transform(const TransformArgs & transformArgs) const
{
    vector<std::shared_ptr<SqlExpression> > transformclauses;

    for (auto& c : clauses)
    {
        transformclauses.push_back(c->transform(transformArgs));
    }

    auto result = std::make_shared<EmbeddingLiteralExpression>(transformclauses);

    return result;
}

std::string
EmbeddingLiteralExpression::
getType() const
{
    return "embedding";
}

Utf8String
EmbeddingLiteralExpression::
getOperation() const
{
    Utf8String result("[");
    for (unsigned i = 0;  i < clauses.size();  ++i) {
        if (i > 0)
            result += ", ";
        result += clauses[i]->print();
    }

    result += "]";
    
    return result;
}

std::vector<std::shared_ptr<SqlExpression> >
EmbeddingLiteralExpression::
getChildren() const
{
    return clauses;
}


/*****************************************************************************/
/* BOOLEAN OPERATOR EXPRESSION                                               */
/*****************************************************************************/

BooleanOperatorExpression::
BooleanOperatorExpression(std::shared_ptr<SqlExpression> lhs,
                          std::shared_ptr<SqlExpression> rhs,
                          std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

BooleanOperatorExpression::
~BooleanOperatorExpression()
{
}

BoundSqlExpression
BooleanOperatorExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs ? lhs->bind(context) : BoundSqlExpression();
    auto boundRhs = rhs->bind(context);

    if (op == "AND" && lhs) {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue lstorage, rstorage;
                    const ExpressionValue & l = boundLhs(row, lstorage, filter);
                    const ExpressionValue & r = boundRhs(row, rstorage, filter);
                    if (l.isFalse() && r.isFalse()) {
                        Date ts = std::min(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue(false, ts));
                    }
                    else if (l.isFalse()) {
                        return storage = std::move(ExpressionValue(false, l.getEffectiveTimestamp()));
                    }
                    else if (r.isFalse()) {
                        return storage = std::move(ExpressionValue(false, r.getEffectiveTimestamp()));
                    }
                    else if (l.empty() && r.empty()) {
                        Date ts = std::min(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue::null(ts));
                    }
                    else if (l.empty())
                        return storage = std::move(ExpressionValue::null(l.getEffectiveTimestamp()));
                    else if (r.empty())
                        return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                    Date ts = std::max(l.getEffectiveTimestamp(),
                                       r.getEffectiveTimestamp());
                    return storage = std::move(ExpressionValue(true, ts));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else if (op == "OR" && lhs) {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
                -> const ExpressionValue &
                {
                    ExpressionValue lstorage, rstorage;

                    const ExpressionValue & l = boundLhs(row, lstorage, filter);
                    const ExpressionValue & r = boundRhs(row, rstorage, filter);

                    if (l.isTrue() && r.isTrue()) {
                        Date ts = std::max(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue(true, ts));
                    }
                    else if (l.isTrue()) {
                        return storage = std::move(ExpressionValue(true, l.getEffectiveTimestamp()));
                    }
                    else if (r.isTrue()) {
                        return storage = std::move(ExpressionValue(true, r.getEffectiveTimestamp()));
                    }
                    else if (l.empty() && r.empty()) {
                        Date ts = std::max(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue::null(ts));
                    }
                    else if (l.empty())
                        return storage = std::move(ExpressionValue::null(l.getEffectiveTimestamp()));
                    else if (r.empty())
                        return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                    Date ts = std::min(l.getEffectiveTimestamp(),
                                       r.getEffectiveTimestamp());
                    return storage = std::move(ExpressionValue(false, ts));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else if (op == "NOT" && !lhs) {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
                -> const ExpressionValue &
                {
                    ExpressionValue rstorage;
                    const ExpressionValue & r = boundRhs(row, rstorage, filter);
                    if (r.empty())
                        return storage = std::move(r);
                    return storage = std::move(ExpressionValue(!r.isTrue(), r.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else throw HttpReturnException(400, "Unknown boolean op " + op
                             + (lhs ? " binary" : " unary"));
}

Utf8String
BooleanOperatorExpression::
print() const
{
    if (lhs)
        return "boolean(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
    else
        return "boolean(\"" + op + "\"," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
BooleanOperatorExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BooleanOperatorExpression>(*this);
    if (lhs) {
        auto newArgs = transformArgs({ lhs, rhs });
        result->lhs = newArgs.at(0);
        result->rhs = newArgs.at(1);
    }
    else {
        auto newArgs = transformArgs({ rhs });
        result->rhs = newArgs.at(0);
    }
    return result;
}

std::string
BooleanOperatorExpression::
getType() const
{
    return "boolean";
}

Utf8String
BooleanOperatorExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
BooleanOperatorExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}


/*****************************************************************************/
/* IS TYPE EXPRESSION                                                        */
/*****************************************************************************/

IsTypeExpression::
IsTypeExpression(std::shared_ptr<SqlExpression> expr,
                 bool notType,
                 std::string type)
    : expr(std::move(expr)),
      notType(notType),
      type(type)
{
}

IsTypeExpression::
~IsTypeExpression()
{
}

BoundSqlExpression
IsTypeExpression::
bind(SqlBindingScope & context) const
{
    auto boundExpr = expr->bind(context);

    bool (ExpressionValue::* fn) () const;
    
    if (type == "string") {
        fn = &ExpressionValue::isString;
    }
    else if (type == "integer") {
        fn = &ExpressionValue::isInteger;
    }
    else if (type == "number") {
        fn = &ExpressionValue::isNumber;
    }
    else if (type == "timestamp") {
        fn = &ExpressionValue::isTimestamp;
    }
    else if (type == "null") {
        fn = &ExpressionValue::empty;
    }
    else if (type == "true") {
        fn = &ExpressionValue::isTrue;
    }
    else if (type == "false") {
        fn = &ExpressionValue::isFalse;
    }
    else if (type == "interval") {
        fn = &ExpressionValue::isTimeinterval;
    }
    else throw HttpReturnException(400, "Unknown type `" + type + "' for IsTypeExpression");

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto v = boundExpr(row, filter);
                bool val = (v .* fn) ();
                return storage = std::move(ExpressionValue(notType ? !val : val,
                                                           v.getEffectiveTimestamp()));
            },
            this,
            std::make_shared<BooleanValueInfo>()};
}

Utf8String
IsTypeExpression::
print() const
{
    if (notType) {
        return "istype(" + expr->print() + ",\"" + type + "\",0)";
    }
    else {
        return "istype(" + expr->print() + ",\"" + type + "\",1)";
    }
}

std::shared_ptr<SqlExpression>
IsTypeExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<IsTypeExpression>(*this);
    result->expr = transformArgs({ expr }).at(0);
    return result;
}

std::string
IsTypeExpression::
getType() const
{
    return notType ? "nottype" : "type";
}

Utf8String
IsTypeExpression::
getOperation() const
{
    return Utf8String(type);
}

std::vector<std::shared_ptr<SqlExpression> >
IsTypeExpression::
getChildren() const
{
    return { expr };
}


/*****************************************************************************/
/* FUNCTION CALL WRAPPER                                                     */
/*****************************************************************************/

FunctionCallWrapper::
FunctionCallWrapper(Utf8String tableName,
                        Utf8String function,
                        std::vector<std::shared_ptr<SqlExpression> > args,
                        std::shared_ptr<SqlExpression> extract)
    : tableName(tableName), functionName(function), args(args), extract(extract)
{

}

FunctionCallWrapper::
~FunctionCallWrapper()
{

}

BoundSqlExpression
FunctionCallWrapper::
bind(SqlBindingScope & context) const
{
    //check whether it is a builtin or not
    if (context.functionStackDepth > 100)
            throw HttpReturnException(400, "Reached a stack depth of over 100 functions while analysing query, possible infinite recursion");

    ++context.functionStackDepth;
    Scope_Exit(--context.functionStackDepth);

    std::vector<BoundSqlExpression> boundArgs;
    for (auto& arg : args)
    {
        boundArgs.emplace_back(std::move(arg->bind(context)));
    }

    BoundFunction fn = context.doGetFunction(tableName, functionName, boundArgs, context);
    BoundSqlExpression boundOutput;

    if (fn)
    {
        //context confirm it is builtin
        boundOutput = bindBuiltinFunction(context, boundArgs, fn);
    }
    else
    {
        //assume user
        boundOutput = bindUserFunction(context);
    }
    return boundOutput;
}

BoundSqlExpression
(*bindSelectApplyFunctionExpressionFn) (std::shared_ptr<Function> function,
                                        const SelectExpression & with,
                                           const SqlRowExpression * expr,
                                        const SqlBindingScope & context);

BoundSqlExpression
(*bindApplyFunctionExpressionFn) (std::shared_ptr<Function> function,
                                  const SelectExpression & with,
                                  const SqlExpression & extract,
                                  const SqlExpression * expr,
                                  SqlBindingScope & context);

BoundSqlExpression
FunctionCallWrapper::
bindUserFunction(SqlBindingScope & context) const
{
    //first check that the function exist, else it really confounds people if we throw errors about arguments for a non-existing function...
    std::shared_ptr<Function> function = context.doGetFunctionEntity(functionName);

    std::vector<std::shared_ptr<SqlRowExpression> > clauses;
    if (args.size() > 0)
    {
         if (args.size() > 1)
            throw HttpReturnException(400, "User function " + functionName + " expected a single row { } argument");

         auto result = std::dynamic_pointer_cast<SelectWithinExpression>(args[0]);
         if (result)
         {
            clauses.push_back(result->select);
         }
         else
         {
            auto functionresult = std::dynamic_pointer_cast<FunctionCallWrapper>(args[0]);

            if (functionresult)
                clauses.push_back(functionresult);
            else
                throw HttpReturnException(400, "User function " + functionName
                                       + " expect a row argument ({ }) or a user function, got " + args[0]->print() );
         }
    }    

    SelectExpression with(clauses);

    if (extract)
    {
        //extract, return the single value
        return bindApplyFunctionExpressionFn(function, with, *extract, this, context);
    }
    else
    {
        //no extract, return the whole row
       return bindSelectApplyFunctionExpressionFn(function, with, this, context);
    }
}

BoundSqlExpression
FunctionCallWrapper::
bindBuiltinFunction(SqlBindingScope & context, std::vector<BoundSqlExpression>& boundArgs, BoundFunction& fn) const
{
    if (extract)
        throw HttpReturnException(400, "Builtin function " + functionName
                                  + " should not have an extract [] expression, got " + extract->print() );
   
    bool isAggregate = tryLookupAggregator(functionName) != nullptr;	
    if (isAggregate) {
        return {[=] (const SqlRowScope & row,		
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &		
                {		
                    std::vector<ExpressionValue> evaluatedArgs;		
                    //Don't evaluate the args for aggregator		
                    evaluatedArgs.resize(boundArgs.size());		
                    return storage = std::move(fn(evaluatedArgs, row));		
                },		
                this,		
                fn.resultInfo};		
    }		
    else {		
        return {[=] (const SqlRowScope & row,		
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &		
                {
                    std::vector<ExpressionValue> evaluatedArgs;
                    evaluatedArgs.reserve(boundArgs.size());
                    for (auto & a: boundArgs)		
                        evaluatedArgs.emplace_back(std::move(a(row, fn.filter)));
                    
                    // TODO: function call that allows function to own its args & have		
                    // storage		
                    return storage = std::move(fn(evaluatedArgs, row));		
                },		         
                this,
                fn.resultInfo};
    }
}

Utf8String
FunctionCallWrapper::
print() const
{
    Utf8String result = "function(\"" + functionName + "\"";

    for (auto & a : args)
    {
        result += "," + a->print();
    }

    if (extract)
        result += "," + extract->print();

    result += ")";

    return result;   
}

std::shared_ptr<SqlExpression>
FunctionCallWrapper::
transform(const TransformArgs & transformArgs) const
{
    auto newArgs = transformArgs(args);
    auto result = std::make_shared<FunctionCallWrapper>(*this);
    result->args = newArgs;
    if (extract)
        result->extract = transformArgs({result->extract}).at(0);

    return result;
}

std::string
FunctionCallWrapper::
getType() const
{
    return "function";
}

Utf8String
FunctionCallWrapper::
getOperation() const
{
    return functionName;
}

std::vector<std::shared_ptr<SqlExpression> >
FunctionCallWrapper::
getChildren() const
{   
    std::vector<std::shared_ptr<SqlExpression> > res = args;
   
    if (extract)
        res.push_back(extract);

    return res;
}

std::map<ScopedName, UnboundFunction>
FunctionCallWrapper::
functionNames() const
{
    std::map<ScopedName, UnboundFunction> result;
    // TODO: actually get arguments
    result[ScopedName(tableName, functionName)].argsForArity[args.size()] = {};
    
    // Now go into our arguments and also extract the functions called
    for (auto & a: args) {
        std::map<ScopedName, UnboundFunction> argF = a->functionNames();
        for (auto & a: argF) {
            result[a.first].merge(a.second);
        }
    }

    return result;
}


/*****************************************************************************/
/* CASE EXPRESSION                                                           */
/*****************************************************************************/

CaseExpression::
CaseExpression(std::shared_ptr<SqlExpression> expr,
               std::vector<std::pair<std::shared_ptr<SqlExpression>,
                                     std::shared_ptr<SqlExpression> > > when,
               std::shared_ptr<SqlExpression> elseExpr)
    : expr(std::move(expr)),
      when(std::move(when)),
      elseExpr(std::move(elseExpr))
{
}

BoundSqlExpression
CaseExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundElse;
    if (elseExpr)
        boundElse = elseExpr->bind(context);

    std::vector<std::pair<BoundSqlExpression, BoundSqlExpression> > boundWhen;

    for (auto & w: when) {
        boundWhen.emplace_back(w.first->bind(context), w.second->bind(context));
    }

    if (expr) {
        // Simple CASE expression

        auto boundExpr = expr->bind(context);

        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
                    -> const ExpressionValue &
                {
                    ExpressionValue vstorage;
                    const ExpressionValue & v = boundExpr(row, vstorage, filter);
                    
                    if (!v.empty()) {
                        for (auto & w: boundWhen) {
                            ExpressionValue wstorage;
                            const ExpressionValue & v2 = w.first(row, wstorage, filter);
                            if (v2.empty())
                                continue;
                            if (v2 == v)
                                return w.second(row, storage, filter);
                        }
                    }

                    if (elseExpr)
                        return boundElse(row, storage, filter);
                    else return storage = std::move(ExpressionValue());
                },
                this,
                // TODO: infer the type
                std::make_shared<AnyValueInfo>()};
    }
    else {
        // Searched CASE expression
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
                    -> const ExpressionValue &
                {
                    for (auto & w: boundWhen) {
                        ExpressionValue wstorage;
                        const ExpressionValue & c = w.first(row, wstorage, filter);
                        if (c.isTrue())
                            return w.second(row, storage, filter);
                    }

                    if (elseExpr)
                        return boundElse(row, storage, filter);
                    else return storage = std::move(ExpressionValue());
                },
                this,
                std::make_shared<AnyValueInfo>()};
    }
}

Utf8String
CaseExpression::
print() const
{
    Utf8String result
        = "case(\""
        + (expr ? expr->print() : Utf8String("null"))
        + ",[";

    bool first = true;
    for (auto & w: when) {
        if (first) first = false;
        else result += ",";
        result += "[" + w.first->print() + "," + w.second->print() + "]";
    }
    result += "]";
    if (elseExpr) {
        result += "," + elseExpr->print();
    }
    result += ")";
    return result;
}

std::shared_ptr<SqlExpression>
CaseExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<CaseExpression>(*this);

    if (expr)
        result->expr = result->expr->transform(transformArgs);
    for (auto & w: result->when) {
        w.first = w.first->transform(transformArgs);
        w.second = w.second->transform(transformArgs);
    }
    if (elseExpr)
        result->elseExpr = result->elseExpr->transform(transformArgs);

    return result;
}

std::string
CaseExpression::
getType() const
{
    return "case";
}

Utf8String
CaseExpression::
getOperation() const
{
    return expr ? Utf8String("simple"): Utf8String("matched");
}

std::vector<std::shared_ptr<SqlExpression> >
CaseExpression::
getChildren() const
{
    std::vector<std::shared_ptr<SqlExpression> > res;
    if (expr)
        res.emplace_back(expr);
    for (auto & w: when) {
        res.emplace_back(w.first);
        res.emplace_back(w.second);
    }
    if (elseExpr)
        res.emplace_back(elseExpr);

    return res;
}


/*****************************************************************************/
/* BETWEEN EXPRESSION                                                        */
/*****************************************************************************/

BetweenExpression::
BetweenExpression(std::shared_ptr<SqlExpression> expr,
                  std::shared_ptr<SqlExpression> lower,
                  std::shared_ptr<SqlExpression> upper,
                  bool notBetween)
    : expr(std::move(expr)),
      lower(std::move(lower)),
      upper(std::move(upper)),
      notBetween(notBetween)
{
}

BoundSqlExpression
BetweenExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundExpr  = expr->bind(context);
    BoundSqlExpression boundLower = lower->bind(context);
    BoundSqlExpression boundUpper = upper->bind(context);

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                ExpressionValue vstorage, lstorage, ustorage;

                const ExpressionValue & v = boundExpr(row, vstorage, filter);
                const ExpressionValue & l = boundLower(row, lstorage, filter);
                const ExpressionValue & u = boundUpper(row, ustorage, filter);
                if (v.empty())
                    return storage = v;

                if (l.empty())
                    return storage = l;

                if (v < l)
                    return storage = std::move(ExpressionValue(notBetween,
                                                               std::max(v.getEffectiveTimestamp(),
                                                                        l.getEffectiveTimestamp())));

                if (u.empty())
                    return storage = u;
                if (v > u)
                    return storage = std::move(ExpressionValue(notBetween,
                                           std::max(v.getEffectiveTimestamp(),
                                                    u.getEffectiveTimestamp())));

                return storage = std::move(ExpressionValue(!notBetween,
                                                           std::max(std::max(v.getEffectiveTimestamp(),
                                                                             u.getEffectiveTimestamp()),
                                                                    l.getEffectiveTimestamp())));
            },
            this,
            std::make_shared<BooleanValueInfo>()};
}

Utf8String
BetweenExpression::
print() const
{
    Utf8String result
        = "between(\""
        + expr->print()
        + ","
        + lower->print()
        + ","
        + upper->print()
        + ")";
    return result;
}

std::shared_ptr<SqlExpression>
BetweenExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BetweenExpression>(*this);
    result->expr  = result->expr->transform(transformArgs);
    result->lower = result->lower->transform(transformArgs);
    result->upper = result->upper->transform(transformArgs);
    return result;
}

std::string
BetweenExpression::
getType() const
{
    return "between";
}

Utf8String
BetweenExpression::
getOperation() const
{
    return Utf8String();
}

std::vector<std::shared_ptr<SqlExpression> >
BetweenExpression::
getChildren() const
{
    return { expr, lower, upper };
}

/*****************************************************************************/
/* IN EXPRESSION                                                        */
/*****************************************************************************/

InExpression::
InExpression(std::shared_ptr<SqlExpression> expr,
             std::shared_ptr<TupleExpression> tuple,
             bool negative)
    : expr(std::move(expr)),
      tuple(std::move(tuple)),
      isnegative(negative),
      kind(TUPLE)
{
}

InExpression::
InExpression(std::shared_ptr<SqlExpression> expr,
             std::shared_ptr<SelectSubtableExpression> subtable,
             bool negative)
    : expr(std::move(expr)),
      subtable(std::move(subtable)),
      isnegative(negative),
      kind(SUBTABLE)
{

}

InExpression::
InExpression(std::shared_ptr<SqlExpression> expr,
             std::shared_ptr<SqlExpression> setExpr,
             bool negative,
             Kind kind)
    : expr(std::move(expr)),
      setExpr(std::move(setExpr)),
      isnegative(negative),
      kind(kind)
{
    ExcAssert(kind == KEYS || kind == VALUES);
}

BoundSqlExpression
InExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundExpr  = expr->bind(context);

    //cerr << "boundExpr: " << expr->print() << " has subtable " << subtable
    //     << endl;

    switch (kind) {
    case SUBTABLE: {
        BoundTableExpression boundTable = subtable->bind(context);

        // TODO: we need to detect a correlated subquery.  This means that
        // the query depends upon variables from the surrounding scope.
        // This should be done with subtable->getUnbound(), but currently
        // that function isn't implemented for table expressions.

        bool correlatedSubquery = false;

        if (correlatedSubquery) {
            // We re-execute on each call, since the results change
            throw HttpReturnException(500, "Correlated subqueries not supported yet");
        }
        else {
            // POTENTIAL OPT: a subquery with no GROUP BY could be run directly
            // without binding, avoiding the need to create a subtable.

            // non-corelated subquery; we can execute the subquery once and
            // for all.  We do this by getting the first column and making
            // it into a set

            static const SelectExpression select = SelectExpression::STAR;
            static const WhenExpression when = WhenExpression::TRUE;
            static const OrderByExpression orderBy;
            static const std::shared_ptr<SqlExpression> where = SqlExpression::TRUE;
            ssize_t offset = 0;
            ssize_t limit = -1;

            BasicRowGenerator generator
                = boundTable.table.runQuery(context, SelectExpression::STAR,
                                            WhenExpression::TRUE,
                                            *SqlExpression::TRUE,
                                            orderBy,
                                            offset, limit, true /* allowParallel */);
            
            // This is a set of all values we can search for in our expression
            auto valsPtr = std::make_shared<std::unordered_set<ExpressionValue> >();

            // NOTE: this is where we REQUIRE that the subquery is non-
            // correlated.  We can only pass a naked SqlRowScope like this
            // to those that are non-correlated... if there are crashes in
            // the generation, it's because we're executing a correlated
            // subquery as if it were non-correlated, and it's trying to
            // look up a variable in the wrong place.  The solution is to
            // fix detection of non-correlated subqueries above.
            SqlRowScope fakeRowScopeForConstantSubqueryGeneration;


            // Generate all outputs of the query
            std::vector<NamedRowValue> rowOutputs
                = generator(-1, fakeRowScopeForConstantSubqueryGeneration);
            
            // Scan them to add to our set
            for (auto & row: rowOutputs) {
                for (auto & col: row.columns) {
                    const ExpressionValue & val = std::get<1>(col);
                    if (!val.empty())
                        valsPtr->insert(val);
                }
            }

            auto exec = [=] (const SqlRowScope & rowScope,
                             ExpressionValue & storage,
                             const VariableFilter & filter) -> const ExpressionValue &
                { 
                    // 1.  What are we looking to see if it's in
                    ExpressionValue vstorage;
                    const ExpressionValue & v = boundExpr(rowScope, vstorage, filter);

                    // 2.  If we have a null, we return a null
                    if (v.empty())
                        return storage = v;
          
                    // 3.  Lookup in our set of values
                    bool found = valsPtr->count(v);

                    // 4.  Return our result
                    return storage = std::move(ExpressionValue(isnegative ? !found : found,
                                                               v.getEffectiveTimestamp()));
                };
            
            return { exec, this, std::make_shared<BooleanValueInfo>() };
        }
    }
    case TUPLE: {
        // We have an explicit tuple, not a subquery.

        std::vector<BoundSqlExpression> tupleExpressions;
        tupleExpressions.reserve(tuple->clauses.size());

        for (auto & tupleItem: tuple->clauses) {
            tupleExpressions.emplace_back(tupleItem->bind(context));
        }

        return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            ExpressionValue vstorage, istorage;

            const ExpressionValue & v = boundExpr(rowScope, vstorage, filter);

            if (v.empty())
                return storage = v;



            for (auto & item : tupleExpressions)
            {
                const ExpressionValue & itemValue = item(rowScope, istorage, filter);

                if (itemValue.empty())
                    continue;

                if ((v == itemValue))
                {
                    return storage = std::move(ExpressionValue(!isnegative,
                                                           std::max(v.getEffectiveTimestamp(),
                                                                    itemValue.getEffectiveTimestamp())));
                }
            }

            return storage = std::move(ExpressionValue(isnegative, v.getEffectiveTimestamp()));
        },
        this,
        std::make_shared<BooleanValueInfo>()};
    }
    case KEYS: {
        BoundSqlExpression boundSet = setExpr->bind(context);

        return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            ExpressionValue vstorage, istorage;

            const ExpressionValue & s = boundSet(rowScope, istorage, filter);

            if (s.empty())
                return storage = s;

            const ExpressionValue & v = boundExpr(rowScope, vstorage, filter);

            std::pair<bool, Date> found = s.hasKey(v.toUtf8String());
            
            if (found.first) {
                return storage = std::move(ExpressionValue(!isnegative,
                                                           std::max(v.getEffectiveTimestamp(),
                                                                    found.second)));
            }

            return storage = std::move(ExpressionValue(isnegative, v.getEffectiveTimestamp()));
        
        },
        this,
        std::make_shared<BooleanValueInfo>()};
    }
    case VALUES: {
        BoundSqlExpression boundSet = setExpr->bind(context);

        return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            ExpressionValue vstorage, istorage;

            const ExpressionValue & s = boundSet(rowScope, istorage, filter);

            if (s.empty())
                return storage = s;

            const ExpressionValue & v = boundExpr(rowScope, vstorage, filter);

            std::pair<bool, Date> found = s.hasValue(v);
            
            if (found.first) {
                return storage = std::move(ExpressionValue(!isnegative,
                                                           std::max(v.getEffectiveTimestamp(),
                                                                    found.second)));
            }

            return storage = std::move(ExpressionValue(isnegative, v.getEffectiveTimestamp()));
        },
        this,
        std::make_shared<BooleanValueInfo>()};
    }
    }
    throw HttpReturnException(500, "Unknown IN expression type");
}

Utf8String
InExpression::
print() const
{
    Utf8String result
        = "in(\""
        + expr->print()
        + ",";

    switch (kind) {
    case SUBTABLE:
        result += "subtable," + subtable->print();  break;
    case TUPLE:
        result += "tuple," + tuple->print();  break;
    case KEYS:
        result += "keys," + setExpr->print();  break;
    case VALUES:
        result += "values," + setExpr->print();  break;
    }
    return result + ")";
}

std::shared_ptr<SqlExpression>
InExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<InExpression>(*this);
    result->expr  = result->expr->transform(transformArgs);

    switch (kind) {
    case SUBTABLE:
        result->subtable = std::make_shared<SelectSubtableExpression>(*(result->subtable));
        break;
    case TUPLE:
        result->tuple = std::make_shared<TupleExpression>(result->tuple->transform(transformArgs));
        break;
    case KEYS:
    case VALUES:
        result->setExpr = setExpr->transform(transformArgs);
        break;
    }

    return result;
}

std::string
InExpression::
getType() const
{
    return "in";
}

Utf8String
InExpression::
getOperation() const
{
    switch (kind) {
    case SUBTABLE:
        return "subtable";
    case TUPLE:
        return "tuple";
    case KEYS:
        return "keys";
    case VALUES:
        return "values";
    }

    throw HttpReturnException(500, "Unknown IN expression type");
}

std::vector<std::shared_ptr<SqlExpression> >
InExpression::
getChildren() const
{
    std::vector<std::shared_ptr<SqlExpression> > children;

    children.emplace_back(std::move(expr));

    switch (kind) {
    case SUBTABLE:
        return children;
    case TUPLE:
        children.insert(children.end(), tuple->clauses.begin(), tuple->clauses.end());
        return children;
    case KEYS:
    case VALUES:
        children.emplace_back(setExpr);
        return children;
    }

    throw HttpReturnException(500, "Unknown IN expression type");
}

/*****************************************************************************/
/* LIKE EXPRESSION                                                           */
/*****************************************************************************/

LikeExpression::
LikeExpression(std::shared_ptr<SqlExpression> left,
               std::shared_ptr<SqlExpression> right,
               bool negative)
    : left(std::move(left)),
      right(std::move(right)),
      isnegative(negative)
{
}

BoundSqlExpression
LikeExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundLeft  = left->bind(context);
    BoundSqlExpression boundRight  = right->bind(context);

    return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {
            ExpressionValue vstorage, fstorage;

            const ExpressionValue & value = boundLeft(rowScope, vstorage, filter);

            if (!value.isString())
                throw HttpReturnException(400, "LIKE expression expected its left hand value to be a string, got " + value.getTypeAsString());

            const ExpressionValue & filterEV = boundRight(rowScope, fstorage, filter);

            if (!filterEV.isString())
                throw HttpReturnException(400, "LIKE expression expected its right hand value to be a string, got " + filterEV.getTypeAsString());

            Utf8String valueString = value.toUtf8String();
            Utf8String filterString = filterEV.toUtf8String();

            bool matched = matchSqlFilter(valueString, filterString);

            return storage = std::move(ExpressionValue(matched != isnegative, std::max(value.getEffectiveTimestamp(), filterEV.getEffectiveTimestamp())));
        },
        this,
        std::make_shared<BooleanValueInfo>()};
}

Utf8String
LikeExpression::
print() const
{
    Utf8String result
        = (isnegative ? "not like(" : "like(")
        + left->print()
        + ","
        + right->print()
        + ")";

    return std::move(result);
}

std::shared_ptr<SqlExpression>
LikeExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<LikeExpression>(*this);
    result->left  = result->left->transform(transformArgs);
    result->right  = result->right->transform(transformArgs);

    return result;
}

std::string
LikeExpression::
getType() const
{
    return "like";
}

Utf8String
LikeExpression::
getOperation() const
{
    return Utf8String();
}

std::vector<std::shared_ptr<SqlExpression> >
LikeExpression::
getChildren() const
{
    std::vector<std::shared_ptr<SqlExpression> > children;

    children.emplace_back(std::move(left));
    children.emplace_back(std::move(right));

    return std::move(children);

}

/*****************************************************************************/
/* CAST EXPRESSION                                                           */
/*****************************************************************************/

CastExpression::
CastExpression(std::shared_ptr<SqlExpression> expr,
               std::string type)
    : expr(std::move(expr)), type(std::move(type))
{
    boost::algorithm::to_lower(type);
}

BoundSqlExpression
CastExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundExpr  = expr->bind(context);

    if (type == "string") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = std::move(ExpressionValue(val.coerceToString(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<StringValueInfo>()};
    }
    else if (type == "integer") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = std::move(ExpressionValue(val.coerceToInteger(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<IntegerValueInfo>()};
    }
    else if (type == "number") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = std::move(ExpressionValue(val.coerceToNumber(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<Float64ValueInfo>()};
    }
    else if (type == "timestamp") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = std::move(ExpressionValue(val.coerceToTimestamp(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<IntegerValueInfo>()};
    }
    else if (type == "boolean") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = std::move(ExpressionValue(val.coerceToBoolean(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else if (type == "blob") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = std::move(ExpressionValue(val.coerceToBlob(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                    std::make_shared<BooleanValueInfo>()};
    }
    else throw HttpReturnException(400, "Unknown type '" + type
                                   + "' for CAST (" + expr->surface
                                   + " AS " + type + ")");
}

Utf8String
CastExpression::
print() const
{
    Utf8String result
        = "cast(\""
        + expr->print()
        + ",\""
        + type
        + "\")";
    return result;
}

std::shared_ptr<SqlExpression>
CastExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<CastExpression>(*this);
    result->expr = transformArgs({expr}).at(0);
    return result;
}

std::string
CastExpression::
getType() const
{
    return "cast";
}

Utf8String
CastExpression::
getOperation() const
{
    return Utf8String(type);
}

std::vector<std::shared_ptr<SqlExpression> >
CastExpression::
getChildren() const
{
    return { expr };
}


/*****************************************************************************/
/* BOUND PARAMETER EXPRESSION                                                */
/*****************************************************************************/

BoundParameterExpression::
BoundParameterExpression(Utf8String paramName)
    : paramName(std::move(paramName))
{
}

BoundSqlExpression
BoundParameterExpression::
bind(SqlBindingScope & context) const
{
    auto getParam = context.doGetBoundParameter(paramName);

    if (!getParam.info) {
        throw HttpReturnException(400, "context " + ML::type_name(context)
                            + " getBoundParameter '" + paramName
                            + "' didn't return info");
    }

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                return getParam(row, storage, filter);
            },
            this,
            getParam.info};
}

Utf8String
BoundParameterExpression::
print() const
{
    return "param(\"" + paramName + "\")";
}

std::map<Utf8String, UnboundVariable>
BoundParameterExpression::
parameterNames() const
{
    return { { paramName, { std::make_shared<AnyValueInfo>() } } };
}

std::shared_ptr<SqlExpression>
BoundParameterExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BoundParameterExpression>(*this);
    return result;
}

std::string
BoundParameterExpression::
getType() const
{
    return "param";
}

Utf8String
BoundParameterExpression::
getOperation() const
{
    return paramName;
}

std::vector<std::shared_ptr<SqlExpression> >
BoundParameterExpression::
getChildren() const
{
    return {};
}

/*****************************************************************************/
/* WILDCARD EXPRESSION                                                       */
/*****************************************************************************/

WildcardExpression::
WildcardExpression(Utf8String tableName,
                   Utf8String prefix,
                   Utf8String asPrefix,
                   std::vector<std::pair<Utf8String, bool> > excluding)
    : tableName(tableName), prefix(prefix), asPrefix(asPrefix),
      excluding(excluding)
{

}

BoundSqlExpression
WildcardExpression::
bind(SqlBindingScope & context) const
{
    Utf8String simplifiedPrefix = prefix;
    Utf8String resolvedTableName = tableName;
    if (tableName.empty() && !prefix.empty())
        simplifiedPrefix = context.doResolveTableName(prefix, resolvedTableName);

    // This function figures out the new name of the column.  If it's excluded,
    // then it returns the empty string
    auto newColumnName = [&, simplifiedPrefix] (const Utf8String & inputColumnName) -> Utf8String
        {
            // First, check it matches the prefix
            if (!inputColumnName.startsWith(simplifiedPrefix))
                return Utf8String();

            // Second, check it doesn't match an exclusion
            for (auto & ex: excluding) {
                if (ex.second) {
                    // prefix
                    if (inputColumnName.startsWith(ex.first))
                        return Utf8String();
                }
                else {
                    // exact match
                    if (inputColumnName == ex.first)
                        return Utf8String();
                }
            }

            // Finally, replace the prefix with the new prefix
            Utf8String result = inputColumnName;
            result.replace(0, simplifiedPrefix.length(), asPrefix);
            return result;
        };

    auto allColumns = context.doGetAllColumns(resolvedTableName, newColumnName);

    auto exec = [=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
        -> const ExpressionValue &
        {
            if (filter == GET_ALL)
                return storage = allColumns.exec(scope);
            else {
                ExpressionValue expr =  std::move(allColumns.exec(scope));
                return storage = expr.getFilteredDestructive(filter);
            }
        };

    BoundSqlExpression result(exec, this, allColumns.info);

    return result;
}

Utf8String
WildcardExpression::
print() const
{
    Utf8String result = "columns(\"" + tableName + "\",\"" + prefix + "\",\"" + asPrefix + "\",[";

    bool first = true;
    for (auto ex: excluding) {
        if (!first) {
            result += ",";
        }
        first = false;
        if (ex.second)
            result += "wildcard(\"" + ex.first + "\")";
        else 
            result += "column(\"" + ex.first + "\")";
    }
    result += "])";

    return result;
}

std::shared_ptr<SqlExpression>
WildcardExpression::
transform(const TransformArgs & transformArgs) const
{
    return std::make_shared<WildcardExpression>(*this);
}

std::vector<std::shared_ptr<SqlExpression> >
WildcardExpression::
getChildren() const
{
    // tough to do without a context...
    return {};
}

std::map<ScopedName, UnboundWildcard>
WildcardExpression::
wildcards() const
{
    std::map<ScopedName, UnboundWildcard> result;
    result[{tableName, prefix + "*"}].prefix = prefix;
    return result;
}

bool
WildcardExpression::
isIdentitySelect(SqlExpressionDatasetContext & context) const
{
    // A select * is identified like this
    return prefix.empty() && asPrefix.empty() && excluding.empty() && (tableName.empty() || context.childaliases.empty());
}


/*****************************************************************************/
/* COMPUTED VARIABLE                                                         */
/*****************************************************************************/

ComputedVariable::
ComputedVariable(Utf8String alias,
                 std::shared_ptr<SqlExpression> expression)
    : alias(alias),
      expression(expression)
{
}

BoundSqlExpression
ComputedVariable::
bind(SqlBindingScope & context) const
{
    auto exprBound = expression->bind(context);

    if (!exprBound.info)
        cerr << "expression didn't return info: " << expression->print() << endl;
    ExcAssert(exprBound.info);

    //cerr << "binding " << print() << " surface " << surface << " " << jsonEncode(exprBound.info)
    //<< endl;

    if (alias.empty()) {
        // This must be a row-returning function, and we merge the row with the
        // output.  Extract what the row is producing to be merged.
        // MLDB-763

        auto info = exprBound.info;
     
        auto exec = [=] (const SqlRowScope & context,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
            -> const ExpressionValue &
            {
                const ExpressionValue & val = exprBound(context, storage, filter);

                if (val.isAtom())
                    throw HttpReturnException(400, "Expression with AS * must return a row",
                                              "valueReturned", val,
                                              "ast", print(),
                                              "surface", surface);
                return val;
            };

        BoundSqlExpression result(exec, this, info);
        return result;
    }
    else {
        ColumnName aliasCol(alias);

        auto exec = [=] (const SqlRowScope & context,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
            -> const ExpressionValue &
            {
                const ExpressionValue & val = exprBound(context, storage, filter);

                if (&val == &storage) {
                    // We own the only copy; we can move it
                    StructValue row;
                    row.emplace_back(aliasCol, std::move(storage));
                    return storage = std::move(ExpressionValue(row));
                }
                else {
                    // We got a reference; copy it
                    StructValue row;
                    row.emplace_back(aliasCol, val);
                    return storage = std::move(ExpressionValue(row));
                }
            };

        std::vector<KnownColumn> knownColumns = {
            KnownColumn(aliasCol, exprBound.info, COLUMN_IS_DENSE) };
        
        auto info = std::make_shared<RowValueInfo>(knownColumns, SCHEMA_CLOSED);

        BoundSqlExpression result(exec, this, info);
        
        return result;
    }
}

Utf8String
ComputedVariable::
print() const
{
    return "computed(\"" + alias + "\"," + expression->print() + ")";
}

std::shared_ptr<SqlExpression>
ComputedVariable::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ComputedVariable>(*this);
    result->expression = transformArgs({ expression}).at(0);
    return result;
}

std::vector<std::shared_ptr<SqlExpression> >
ComputedVariable::
getChildren() const
{
    return { expression };
}

/*****************************************************************************/
/* SELECT COLUMN EXPRESSION                                                  */
/*****************************************************************************/

SelectColumnExpression::
SelectColumnExpression(std::shared_ptr<SqlExpression> select,
                       std::shared_ptr<SqlExpression> as,
                       std::shared_ptr<SqlExpression> where,
                       OrderByExpression orderBy,
                       int64_t offset,
                       int64_t limit)
    : select(std::move(select)),
      as(std::move(as)),
      where(std::move(where)),
      orderBy(std::move(orderBy)),
      offset(offset),
      limit(limit)
{
}

BoundSqlExpression
SelectColumnExpression::
bind(SqlBindingScope & context) const
{
    // 1.  Get all columns
    auto allColumns = context.doGetAllColumns(Utf8String(""),
                                              [] (const Utf8String & name) { return name; });

    // Only known columns are kept.  For each one, we filter it then calculate the
    // order by expression.

    struct ColumnEntry {
        ColumnName inputColumnName;
        ColumnName columnName;
        ColumnHash columnHash;
        int columnNumber;
        std::vector<ExpressionValue> sortFields;
        std::shared_ptr<ExpressionValueInfo> valueInfo;
        ColumnSparsity sparsity;
    };

    std::vector<ColumnEntry> columns;

    // Bind those expressions that operate in the column context
    ColumnExpressionBindingContext colContext(context);

    auto boundWhere = where->bind(colContext);
    auto boundAs = as->bind(colContext);

    std::vector<BoundSqlExpression> boundOrderBy;
    for (auto & o: orderBy.clauses)
        boundOrderBy.emplace_back(o.first->bind(colContext));

    /// List of all functions to run in our run() operator
    std::vector<std::function<void (const SqlRowScope &, StructValue &)> > functionsToRun;

    std::vector<KnownColumn> knownColumns = allColumns.info->getKnownColumns();
   

    // For each group of columns, find which match
    for (unsigned j = 0;  j < knownColumns.size();  ++j) {
        const auto & col = knownColumns[j];

        const ColumnName & columnName = col.columnName;
            
        auto thisContext = colContext.getColumnContext(columnName);

        bool keep = boundWhere(thisContext, GET_LATEST).isTrue();

        if (!keep)
            continue;

        Utf8String newColName = boundAs(thisContext, GET_LATEST).toUtf8String();

        vector<ExpressionValue> orderBy;
        for (auto & c: boundOrderBy) {
            orderBy.emplace_back(c(thisContext, GET_LATEST));
        }

        ColumnEntry entry;
        entry.inputColumnName = columnName;
        entry.columnName = ColumnName(newColName);
        entry.columnHash = entry.columnName;
        entry.columnNumber = j;
        entry.sortFields = std::move(orderBy);
        entry.valueInfo = col.valueInfo;
        entry.sparsity = col.sparsity;

        columns.emplace_back(std::move(entry));
    }

    //cerr << "considering " << columns.size() << " columns" << endl;
    
    // Compare two columns according to the sort criteria
    auto compareColumns = [&] (const ColumnEntry & col1,
                               const ColumnEntry & col2)
        {
            for (unsigned i = 0;  i < boundOrderBy.size();  ++i) {
                const ExpressionValue & e1 = col1.sortFields[i];
                const ExpressionValue & e2 = col2.sortFields[i];
                int cmp = e1.compare(e2);
                //ExcAssertEqual(e1.compare(e1), 0);
                //ExcAssertEqual(e2.compare(e2), 0);
                //ExcAssertEqual(e2.compare(e1), -cmp);
                //cerr << "i = " << i << " cmp = " << cmp << " val1 = "
                //     << e1 << " val2 = " << e2 << endl;
                if (orderBy.clauses[i].second == DESC)
                    cmp *= -1;
                if (cmp == -1)
                    return true;
                else if (cmp == 1)
                    return false;
                //ExcAssertEqual(cmp, 0);
            }

            // No ordering otherwise
            return false;
            //return col1.second.colHash < col2.second.colHash;
        };

    std::sort(columns.begin(), columns.end(), compareColumns);

    //for (unsigned i = 0;  i < 10 && i < columns.size();  ++i) {
    //    cerr << "column " << i << " name " << columns[i].columnName
    //         << " sort " << jsonEncodeStr(columns[i].sortFields) << endl;
    //}

    // Now apply the windowing functions
    ssize_t offset = this->offset;
    ssize_t limit = this->limit;

    ExcAssertGreaterEqual(offset, 0);
    if (offset > columns.size())
        offset = columns.size();

    columns.erase(columns.begin(), columns.begin() + offset);
    
    ExcAssertGreaterEqual(limit, -1);
    if (limit == -1 || limit > columns.size())
        limit = columns.size();

    columns.erase(columns.begin() + limit, columns.end());

    //cerr << "restricted set of columns has " << columns.size() << " entries" << endl;

    std::unordered_map<Utf8String, Utf8String> keepColumns;
    for (auto & c: columns)
        keepColumns[c.inputColumnName.toUtf8String()] = c.columnName.toUtf8String();
    
    auto filterColumns = [=] (const Utf8String & name) -> Utf8String
        {
            auto it = keepColumns.find(name);
            if (it == keepColumns.end())
                return Utf8String();

            return it->second;
        };
    
    // Finally, return a filtered set from the underlying dataset
    auto outputColumns
        = context.doGetAllColumns(Utf8String(""), filterColumns);

    auto exec = [=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
        -> const ExpressionValue &
        {
            return storage = std::move(outputColumns.exec(context));
        };

    BoundSqlExpression result(exec, this, outputColumns.info);
    
    return result;
}

Utf8String
SelectColumnExpression::
print() const
{
    return "columnExpr("
        + select->print() + ","
        + as->print() + ","
        + where->print() + ","
        + orderBy.print() + ","
        + std::to_string(offset) + ","
        + std::to_string(limit)
        + ")";
}

std::shared_ptr<SqlExpression>
SelectColumnExpression::
transform(const TransformArgs & transformArgs) const
{
    throw HttpReturnException(400, "SelectColumnExpression::transform()");
}

std::vector<std::shared_ptr<SqlExpression> >
SelectColumnExpression::
getChildren() const
{
    // NOTE: these are indirect children, in that they bind their own
    // variables.  So we may not want to return them.  Needs better
    // analysis and testing.
    //
    // If you are trying to debug errors with binding of aggregators
    // or similar functions that look in the expression tree to identify
    // particular constructs, try making this function return nothing
    // and see if that fixes the problem.  If that is the case, then
    // we will probably need to revisit the getChildren() and
    // unbound entity detection logic to differentiate between things
    // that are bound internally versus unbound externally.

    std::vector<std::shared_ptr<SqlExpression> > result;
    
    auto add = [&] (std::vector<std::shared_ptr<SqlExpression> > exprs)
        {
            result.insert(result.end(),
                          std::make_move_iterator(exprs.begin()),
                          std::make_move_iterator(exprs.end()));
        };

    add(select->getChildren());
    add(as->getChildren());
    add(where->getChildren());
    for (auto & c: orderBy.clauses)
        add(c.first->getChildren());

    return result;
}

std::map<ScopedName, UnboundWildcard>
SelectColumnExpression::
wildcards() const
{
    //COLMUN EXPR has an *implicit* wildcard because it reads all columns.
    std::map<ScopedName, UnboundWildcard> result;
    result[{"", "*"}].prefix = "";
    return result;
}


} // namespace MLDB
} // namespace Datacratic

