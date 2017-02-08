/** sql_expression_operations.cc
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "sql_expression_operations.h"
#include "mldb/http/http_exception.h"
#include <boost/algorithm/string.hpp>
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/compact_vector_description.h"
#include "table_expression_operations.h"
#include <unordered_set>
#include "mldb/server/dataset_context.h"
#include "mldb/base/scope.h"
#include "mldb/sql/sql_utils.h"
#include "mldb/jml/stats/distribution.h"

using namespace std;



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
             const BoundSqlExpression & boundLhs,
             const BoundSqlExpression & boundRhs,
             bool (ExpressionValue::* op)(const ExpressionValue &) const)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage, GET_LATEST);
                const ExpressionValue & r = boundRhs(row, rstorage, GET_LATEST);
                // cerr << "left " << l << " " << "right " << r << endl;
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = ExpressionValue::null(ts);
 
                return storage = ExpressionValue((l .* op)(r), ts);
            },
            expr,
            std::make_shared<BooleanValueInfo>(boundLhs.info->isConst() && boundRhs.info->isConst())};
}

BoundSqlExpression
ComparisonExpression::
bind(SqlBindingScope & scope) const
{
    auto boundLhs = lhs->bind(scope);
    auto boundRhs = rhs->bind(scope);

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

    struct ScalarContext;
    struct EmbeddingContext;
    struct RowScope;
    struct UnknownContext;

    static const ExpressionValue &
    genericApplyRowRowDynamic(const ExpressionValue & lhs,
                              const ExpressionValue & rhs,
                              ExpressionValue & storage)
    {
        // row * row
        RowValue output;

        auto onColumn = [&] (ColumnPath columnName,
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

    template<typename LhsContext, typename RhsContext>
    static std::shared_ptr<ExpressionValueInfo>
    genericGetInfoRowRow(const LhsContext & lhsContext,
                         const RhsContext & rhsContext)
    {
        // Row * row
        auto onInfo = [] (const ColumnPath &,
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

        return ExpressionValueInfo::getMerged(lhsContext.bound.info,
                                              rhsContext.bound.info,
                                              onInfo);
    }

    // Context object for scalar operations on the LHS or RHS
    struct ScalarContext {
        ScalarContext(BoundSqlExpression bound)
            : bound(std::move(bound))
        {
        }

        BoundSqlExpression bound;

        const ExpressionValue &
        operator () (const SqlRowScope & row,
                     ExpressionValue & storage, 
                     const VariableFilter & filter) const
        {
            return bound(row, storage, filter);
        }

        template<typename RhsContext>
        std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(RhsContext & rhsContext)
        {
            return rhsContext.getInfoScalar(*this);
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(const ScalarContext & lhsContext)
        {
            // Scalar * scalar
            return Op::getInfo(lhsContext.bound.info, this->bound.info);
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(const EmbeddingContext & lhsContext)
        {
            // Embedding * scalar
            auto inner = getValueInfoForStorage
                (lhsContext.bound.info->getEmbeddingType());
            auto res = Op::getInfo(inner, this->bound.info);
            return std::make_shared<EmbeddingValueInfo>
                (lhsContext.bound.info->getEmbeddingShape(),
                 res->getEmbeddingType());
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoRow(const RowScope & lhsContext)
        {
            // Row * scalar
            auto cols = lhsContext.bound.info->getKnownColumns();
            for (auto & c: cols)
                c.valueInfo = Op::getInfo(c.valueInfo, this->bound.info);
            return std::make_shared<RowValueInfo>
                (std::move(cols),
                 lhsContext.bound.info->getSchemaCompleteness());
        }

        template<typename RhsContext>
        const ExpressionValue &
        applyLhs(const RhsContext & rhsContext,
                 const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage) const
        {
            return rhsContext.applyRhsScalar(lhs, rhs, storage);
        }

        const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage) const
        {
            // scalar * scalar; return it directly
            return storage
                = ExpressionValue(Op::apply(lhs.getAtom(),
                                            rhs.getAtom()),
                                  std::max(lhs.getEffectiveTimestamp(),
                                           rhs.getEffectiveTimestamp()));
        }

        const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage) const
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

        const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage) const
        {
            // row * scalar
            RowValue output;
            const CellValue & r = rhs.getAtom();
            Date rts = rhs.getEffectiveTimestamp();
            auto onVal = [&] (ColumnPath columnName,
                              const ColumnPath & prefix,
                              const CellValue & val,
                              Date ts)
                {
                    output.emplace_back(prefix + columnName,
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

        EmbeddingContext(BoundSqlExpression bound)
            : bound(std::move(bound))
        {
        }

        BoundSqlExpression bound;
        ExpressionValueInfo::GetCompatibleDoubleEmbeddingsFn extract;
        ExpressionValueInfo::ReconstituteFromDoubleEmbeddingFn reconst;

        const ExpressionValue &
        operator () (const SqlRowScope & row,
                     ExpressionValue & storage, 
                     const VariableFilter & filter) const
        {
            return bound(row, storage, filter);
        }

        template<typename RhsContext>
        std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(RhsContext & rhsContext)
        {
            return rhsContext.getInfoEmbedding(*this);
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(const ScalarContext & lhsContext)
        {
            // Scalar * embedding
            auto inner = getValueInfoForStorage
                (this->bound.info->getEmbeddingType());
            auto res = Op::getInfo(lhsContext.bound.info, inner);
            return std::make_shared<EmbeddingValueInfo>
                (this->bound.info->getEmbeddingShape(),
                 res->getEmbeddingType());
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(const EmbeddingContext & lhsContext)
        {
            // Embedding * embedding
            auto innerl = getValueInfoForStorage
                (lhsContext.bound.info->getEmbeddingType());
            auto innerr = getValueInfoForStorage
                (this->bound.info->getEmbeddingType());
            auto res = Op::getInfo(innerl, innerr);
            return std::make_shared<EmbeddingValueInfo>
                (lhsContext.bound.info->getEmbeddingShape(),
                 res->getEmbeddingType());
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoRow(const RowScope & lhsContext)
        {
            // Row * embedding
            std::shared_ptr<ExpressionValueInfo> info;

            std::tie(this->extract, info, this->reconst)
                = this->bound.info
                ->getCompatibleDoubleEmbeddings(*lhsContext.bound.info);

            return info;
        }

        template<typename RhsContext>
        const ExpressionValue &
        applyLhs(const RhsContext & rhsContext,
                 const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage) const
        {
            // Extract the embedding.  Both must be embeddings.
            return rhsContext.applyRhsEmbedding(lhs, rhs, storage);
        }

        const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage) const
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

        const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage) const
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

        const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage) const
        {
            return genericApplyRowRowDynamic(lhs, rhs, storage);
        }
    };

    // Context object for rows on the LHS or RHS
    struct RowScope {

        RowScope(BoundSqlExpression bound)
            : bound(std::move(bound))
        {
        }

        BoundSqlExpression bound;
        ExpressionValueInfo::GetCompatibleDoubleEmbeddingsFn extract;
        ExpressionValueInfo::ReconstituteFromDoubleEmbeddingFn reconst;

        const ExpressionValue &
        operator () (const SqlRowScope & row,
                     ExpressionValue & storage, 
                     const VariableFilter & filter) const
        {
            return bound(row, storage, filter);
        }

        template<typename RhsContext>
        std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(RhsContext & rhsContext)
        {
            return rhsContext.getInfoRow(*this);
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(const ScalarContext & lhsContext)
        {
            // Scalar * row
            auto cols = this->bound.info->getKnownColumns();
            for (auto & c: cols)
                c.valueInfo = Op::getInfo(this->bound.info, c.valueInfo);
            return std::make_shared<RowValueInfo>
                (std::move(cols),
                 this->bound.info->getSchemaCompleteness());
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(const EmbeddingContext & lhsContext)
        {
            ExpressionValueInfo::GetCompatibleDoubleEmbeddingsFn extract;
            std::shared_ptr<ExpressionValueInfo> info;
            ExpressionValueInfo::ReconstituteFromDoubleEmbeddingFn reconst;

            std::tie(this->extract, info, this->reconst)
                = this->bound.info
                ->getCompatibleDoubleEmbeddings(*lhsContext.bound.info);

            return info;
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoRow(const RowScope & lhsContext)
        {
            return genericGetInfoRowRow(lhsContext, *this);
        }

        template<typename RhsContext>
        const ExpressionValue &
        applyLhs(const RhsContext & rhsContext,
                 const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage) const
        {
            return rhsContext.applyRhsRow(lhs, rhs, storage);
        }

        const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage) const
        {
            // Scalar * row
            RowValue output;
            const CellValue & l = lhs.getAtom();
            Date lts = lhs.getEffectiveTimestamp();
            auto onVal = [&] (ColumnPath columnName,
                              const ColumnPath & prefix,
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

        const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage) const
        {
            return genericApplyRowRowDynamic(lhs, rhs, storage);
        }

        const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage) const
        {
            return genericApplyRowRowDynamic(lhs, rhs, storage);
        }
    };

    // Context object for unknown types on the LHS or RHS.  They
    // switch at runtime.
    struct UnknownContext {

        UnknownContext(BoundSqlExpression bound)
            : bound(std::move(bound))
        {
        }

        BoundSqlExpression bound;

        const ExpressionValue &
        operator () (const SqlRowScope & row,
                     ExpressionValue & storage, 
                     const VariableFilter & filter) const
        {
            return bound(row, storage, filter);
        }
        
        template<typename RhsContext>
        std::shared_ptr<ExpressionValueInfo>
        getInfoLhs(RhsContext & context)
        {
            return std::make_shared<AnyValueInfo>();
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoScalar(const ScalarContext & lhsContext)
        {
            // Scalar * unknown.  No idea if it's scalar or vector
            // until runtime.
            return std::make_shared<AnyValueInfo>();
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoUnknown(const UnknownContext & lhsContext)
        {
            // unknown * unknown.  No idea if it's scalar or vector
            // until runtime.
            return std::make_shared<AnyValueInfo>();
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoEmbedding(const EmbeddingContext & lhsContext)
        {
            // Embedding * unknown.  It must be an embedding.
            auto inner = getValueInfoForStorage
                (lhsContext.bound.info->getEmbeddingType());
            auto res = Op::getInfo(inner, this->bound.info);
            return std::make_shared<EmbeddingValueInfo>
                (lhsContext.bound.info->getEmbeddingShape(),
                 res->getEmbeddingType());
        }

        std::shared_ptr<ExpressionValueInfo>
        getInfoRow(const RowScope & lhsContext)
        {
            // Row * unknown.  It must be a row
            auto cols = lhsContext.bound.info->getKnownColumns();
            for (auto & c: cols)
                c.valueInfo = Op::getInfo(c.valueInfo, this->bound.info);
            return std::make_shared<RowValueInfo>
                (std::move(cols),
                 lhsContext.bound.info->getSchemaCompleteness());
        }

        template<typename RhsContext>
        const ExpressionValue &
        applyLhs(const RhsContext & rhsContext,
                 const ExpressionValue & lhs,
                 const ExpressionValue & rhs,
                 ExpressionValue & storage) const
        {
            if (lhs.isAtom()) {
                ScalarContext lhsContext(bound);
                return lhsContext.applyLhs(rhsContext, lhs, rhs, storage);
            }
            else if (lhs.isEmbedding()) {
                EmbeddingContext lhsContext(bound);
                return lhsContext.applyLhs(rhsContext, lhs, rhs, storage);
            }
            else if (lhs.isRow()) {
                RowScope lhsContext(bound);
                return lhsContext.applyLhs(rhsContext, lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

        const ExpressionValue &
        applyRhsScalar(const ExpressionValue & lhs,
                       const ExpressionValue & rhs,
                       ExpressionValue & storage) const
        {
            if (rhs.isAtom()) {
                ScalarContext rhsContext(bound);
                return rhsContext.applyRhsScalar(lhs, rhs, storage);
            }
            else if (rhs.isEmbedding()) {
                EmbeddingContext rhsContext(bound);
                return rhsContext.applyRhsScalar(lhs, rhs, storage);
            }
            else if (rhs.isRow()) {
                RowScope rhsContext(bound);
                return rhsContext.applyRhsScalar(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

        const ExpressionValue &
        applyRhsEmbedding(const ExpressionValue & lhs,
                          const ExpressionValue & rhs,
                          ExpressionValue & storage) const
        {
            if (rhs.isAtom()) {
                ScalarContext rhsContext(bound);
                return rhsContext.applyRhsEmbedding(lhs, rhs, storage);
            }
            else if (rhs.isEmbedding()) {
                EmbeddingContext rhsContext(bound);
                return rhsContext.applyRhsEmbedding(lhs, rhs, storage);
            }
            else if (rhs.isRow()) {
                RowScope rhsContext(bound);
                return rhsContext.applyRhsEmbedding(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

        const ExpressionValue &
        applyRhsRow(const ExpressionValue & lhs,
                    const ExpressionValue & rhs,
                    ExpressionValue & storage) const
        {
            if (rhs.isAtom()) {
                ScalarContext rhsContext(bound);
                return rhsContext.applyRhsRow(lhs, rhs, storage);
            }
            else if (rhs.isEmbedding()) {
                EmbeddingContext rhsContext(bound);
                return rhsContext.applyRhsRow(lhs, rhs, storage);
            }
            else if (rhs.isRow()) {
                RowScope rhsContext(bound);
                return rhsContext.applyRhsRow(lhs, rhs, storage);
            }
            else {
                throw HttpReturnException(500, "Can't figure out type of expression",
                                          "expression", lhs);
            }
        }

    };

    template<class LhsContext, class RhsContext>
    static const ExpressionValue &
    apply(const LhsContext & lhsContext,
          const RhsContext & rhsContext,
          const SqlRowScope & row,
          ExpressionValue & storage, 
          const VariableFilter & filter)
    {
        ExpressionValue lstorage, rstorage;
        const ExpressionValue & lhs = lhsContext(row, lstorage, filter);
        const ExpressionValue & rhs = rhsContext(row, rstorage, filter);
        
        return lhsContext.applyLhs(rhsContext, lhs, rhs, storage);
    }
    
    template<class LhsContext, class RhsContext>
    static BoundSqlExpression
    bindAll(LhsContext lhsContext,
            RhsContext rhsContext,
            const SqlExpression * expr,
            bool isConstant)
    {
        BoundSqlExpression result;
        result.info = lhsContext.getInfoLhs(rhsContext);
        result.exec = std::bind(apply<LhsContext, RhsContext>,
                                lhsContext,
                                rhsContext,
                                std::placeholders::_1,
                                std::placeholders::_2,
                                GET_LATEST);
        result.expr = expr->shared_from_this();
        result.info = result.info->getConst(isConstant);

        return result;
    }

    template<class LhsContext>
    static BoundSqlExpression
    bindRhs(LhsContext lhsContext,
            const SqlExpression * expr,
            const BoundSqlExpression & boundRhs,
            bool isConstant)
    {
        int scalar = boundRhs.info->isScalar();
        int embedding = boundRhs.info->isEmbedding();
        int row = boundRhs.info->isRow();

        int total = scalar + embedding + row;

        if (total == 1 && scalar) {
            ScalarContext rhsContext(boundRhs);
            return bindAll(lhsContext, rhsContext, expr, isConstant);
        }
        else if (embedding && !scalar) {
            EmbeddingContext rhsContext(boundRhs);
            return bindAll(lhsContext, rhsContext, expr, isConstant);
        }
        else if (row && !scalar) {
            RowScope rhsContext(boundRhs);
            return bindAll(lhsContext, rhsContext, expr, isConstant);
        }
        else {
            UnknownContext rhsContext(boundRhs);
            return bindAll(lhsContext, rhsContext, expr, isConstant);
        }
    }

    static BoundSqlExpression
    bind(const SqlExpression * expr,
         const BoundSqlExpression & boundLhs,
         const BoundSqlExpression & boundRhs)
    {
        int scalar = boundLhs.info->isScalar();
        int embedding = boundLhs.info->isEmbedding();
        int row = boundLhs.info->isRow();

        int total = scalar + embedding + row;
        bool constant = boundLhs.info->isConst() && boundRhs.info->isConst();

        if (total == 1 && scalar) {
            ScalarContext lhsContext(boundLhs);
            return bindRhs(lhsContext, expr, boundRhs, constant);
        }
        else if (embedding && !scalar) {
            EmbeddingContext lhsContext(boundLhs);
            return bindRhs(lhsContext, expr, boundRhs, constant);
        }
        else if (row && !scalar) {
            RowScope lhsContext(boundLhs);
            return bindRhs(lhsContext, expr, boundRhs, constant);
        }
        else {
            UnknownContext lhsContext(boundLhs);
            return bindRhs(lhsContext, expr, boundRhs, constant);
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
                    return storage = ExpressionValue::null(ts);
                return storage = ExpressionValue(op(l.getAtom(),
                                                    r.getAtom()), ts);
            },
            expr,
            std::make_shared<ReturnInfo>(boundLhs.info->isConst() && boundRhs.info->isConst())};
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
                    return storage = ExpressionValue::null(r.getEffectiveTimestamp());
                return storage
                    = ExpressionValue(std::move(op(r.getAtom())),
                                      r.getEffectiveTimestamp());
            },
            expr,
            std::make_shared<ReturnInfo>(boundRhs.info->isConst())};
}

static CellValue
binaryPlusOnTimestamp(const CellValue & l, const CellValue & r)
{
    ExcAssert(l.isTimestamp());

    if (r.isInteger())
    {
        //when no interval is specified (integer), operation is done in days
        return CellValue(l.toTimestamp().addDays(r.toInt()));
    }
    else if (r.isTimeinterval())
    {
        //date + interval will give a date
        uint16_t months = 0, days = 0;
        float seconds = 0;

        std::tie(months, days, seconds) = r.toMonthDaySecond();

        if (seconds < 0)
            return CellValue(l.toTimestamp().minusMonthDaySecond(months, days, fabs(seconds)));
        else
            return CellValue(l.toTimestamp().plusMonthDaySecond(months, days, seconds));
    }

    throw HttpReturnException(400, "Adding unsupported type to timetamp");

    return CellValue(l.toTimestamp());

}

static CellValue binaryPlus(const CellValue & l, const CellValue & r)
{
    if (l.isString() || r.isString())
    {
       return CellValue(l.toUtf8String() + r.toUtf8String());
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
        
        return CellValue::fromMonthDaySecond(lmonths, ldays, lseconds);
    }
    else
    {
        return CellValue(l.toDouble() + r.toDouble());
    }
}

static CellValue binaryMinusOnTimestamp(const CellValue & l, const CellValue & r)
{
    ExcAssert(l.isTimestamp());

    if (r.isInteger())
    {
        //when no interval is specified (integer), operation is done in days
        return CellValue(l.toTimestamp().addDays(-r.toInt()));
    }
    else if (r.isTimeinterval())
    {
        //date - interval will give a date
        //date + interval will give a date
        int64_t months = 0, days = 0;
        float seconds = 0;

        std::tie(months, days, seconds) = r.toMonthDaySecond();

        if (seconds >= 0)
            return CellValue(l.toTimestamp().minusMonthDaySecond(months, days, fabs(seconds)));
        else
            return CellValue(l.toTimestamp().plusMonthDaySecond(months, days, seconds));
    }
    else if (r.isTimestamp())
    {
        //date - date gives us an interval
        int64_t days = 0;
        float seconds = 0;
        std::tie(days, seconds) = l.toTimestamp().getDaySecondInterval(r.toTimestamp());
        return CellValue::fromMonthDaySecond(0, days, seconds);
    }

    throw HttpReturnException(400, "Substracting unsupported type to timetamp");
    return CellValue(l.toTimestamp());

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
        
        return CellValue::fromMonthDaySecond(lmonths, ldays, lseconds);
    }

    return CellValue(l.toDouble() - r.toDouble());
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

        return CellValue::fromMonthDaySecond(months, days, seconds);
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

    return CellValue::fromMonthDaySecond(months, days, seconds);
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

        return CellValue::fromMonthDaySecond(months, ddays, seconds);
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
        if (l.empty() || r.empty()) {
            return CellValue();
        }
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
        if (l.empty() || r.empty()) {
            return CellValue();
        }
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
        if (l.empty() || r.empty()) {
            return CellValue();
        }
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
        if (l.empty() || r.empty()) {
            return CellValue();
        }
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
        if (l.empty() || r.empty()) {
            return CellValue();
        }
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
bind(SqlBindingScope & scope) const
{
    auto boundLhs = lhs ? lhs->bind(scope) : BoundSqlExpression();
    auto boundRhs = rhs->bind(scope);

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
                    return storage = ExpressionValue::null(ts);
                return storage = ExpressionValue(op(l.toInt(), r.toInt()), ts);
            },
            expr,
            std::make_shared<IntegerValueInfo>(boundLhs.info->isConst() && boundRhs.info->isConst())};
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
                    return storage = ExpressionValue::null(r.getEffectiveTimestamp());
                return storage = ExpressionValue(std::move(op(r.toInt())), r.getEffectiveTimestamp());
            },
            expr,
            std::make_shared<IntegerValueInfo>(boundRhs.info->isConst())};
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
bind(SqlBindingScope & scope) const
{
    auto boundLhs = lhs ? lhs->bind(scope) : BoundSqlExpression();
    auto boundRhs = rhs->bind(scope);

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

ReadColumnExpression::
ReadColumnExpression(ColumnPath columnName)
    : columnName(std::move(columnName))
{
}

ReadColumnExpression::
~ReadColumnExpression()
{
}

BoundSqlExpression
ReadColumnExpression::
bind(SqlBindingScope & scope) const
{
    auto getVariable = scope.doGetColumn("" /*tableName*/, columnName);

    if (!getVariable.info) {
        throw HttpReturnException(400, "scope " + MLDB::type_name(scope)
                                  + " getColumn '" + columnName.toUtf8String()
                                  + "' didn't return info");
    }

    //For now, read columns will never be considered const, because we wont have a row scope to 
    //evaluate them, even if it always returns the same value.
    auto info = getVariable.info->getConst(false);

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                // TODO: allow it access to storage
                return getVariable(row, storage, filter);
            },
            this,
            info};
}

Utf8String
ReadColumnExpression::
print() const
{
    return "column(\"" + columnName.toUtf8String() + "\")";
}

std::shared_ptr<SqlExpression>
ReadColumnExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ReadColumnExpression>(*this);
    return result;
}

std::string
ReadColumnExpression::
getType() const
{
    return "variable";
}

Utf8String
ReadColumnExpression::
getOperation() const
{
    return columnName.toUtf8String();
}

std::vector<std::shared_ptr<SqlExpression> >
ReadColumnExpression::
getChildren() const
{
    return {};
}

std::map<ScopedName, UnboundVariable>
ReadColumnExpression::
variableNames() const
{
    return { { { "", columnName }, { std::make_shared<AnyValueInfo>() } } };
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
bind(SqlBindingScope & scope) const
{
    ExpressionValue val = constant;

    return {[=] (const SqlRowScope &,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                return storage=val;
            },
            this,
            constant.getSpecializedValueInfo(true /* is constant */)};
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
bind(SqlBindingScope & scope) const
{
    return select->bind(scope);
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

bool
SelectWithinExpression::
isConstant() const
{
    return select->isConstant();
}

/*****************************************************************************/
/* EMBEDDING EXPRESSION                                                      */
/*****************************************************************************/

EmbeddingLiteralExpression::
EmbeddingLiteralExpression(vector<std::shared_ptr<SqlExpression> > clauses)
    : clauses(std::move(clauses))
{
}

EmbeddingLiteralExpression::
~EmbeddingLiteralExpression()
{
}

BoundSqlExpression
EmbeddingLiteralExpression::
bind(SqlBindingScope & scope) const
{
    if (clauses.empty()) {
        return BoundSqlExpression
            ([] (const SqlRowScope &, 
                 ExpressionValue & storage, 
                 const VariableFilter & filter) 
             -> const ExpressionValue &
             {
                 return storage = ExpressionValue::null(Date::notADate());
             },
             this,
             std::make_shared<EmptyValueInfo>()); //Note: Empty value info is always constant
    }

    bool isConstant = true;

    vector<BoundSqlExpression> boundClauses;
    DimsVector knownDims = {clauses.size()};

    std::vector<std::shared_ptr<ExpressionValueInfo> > clauseInfo;

    for (auto & c: clauses) {
        boundClauses.emplace_back(c->bind(scope));
        isConstant = isConstant && boundClauses.back().info->isConst();
        clauseInfo.push_back(boundClauses.back().info);
    }   

    auto outputInfo
        = std::make_shared<EmbeddingValueInfo>(clauseInfo, isConstant);

    if (outputInfo->shape.at(0) != -1)
        ExcAssertEqual(outputInfo->shape.at(0), clauses.size());

    bool lastLevel = outputInfo->numDimensions() == 1;

    auto exec = [=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
        {  
            Date ts = Date::negativeInfinity();

            if (lastLevel) {
                std::vector<CellValue> cells(boundClauses.size());

                for (size_t i = 0;  i < boundClauses.size();  ++i) {
                    auto & c = boundClauses[i];
                    ExpressionValue storage2;
                    const ExpressionValue & v = c(scope, storage2, filter);
                    ts.setMax(v.getEffectiveTimestamp());
                    if (&v == &storage2)
                        cells[i] = storage2.stealAtom();
                    else cells[i] = v.getAtom();
                }

                ExpressionValue result(std::move(cells), ts);
                return storage = std::move(result);
            }
            else {

                std::vector<CellValue> cells;

                DimsVector dims { boundClauses.size() };

                for (unsigned i = 0;  i < boundClauses.size();  ++i) {
                    auto & c = boundClauses[i];
                    ExpressionValue v = c(scope, GET_LATEST);
                    
                    // Get the number of dimensions in the embedding
                    if (i == 0) {
                        for (size_t d: v.getEmbeddingShape())
                            dims.emplace_back(d);
                    }
                    // TODO: check for the correct size of cells

                    ts.setMin(v.getEffectiveTimestamp());

                    std::vector<CellValue> valueCells
                        = v.getEmbeddingCell(v.getAtomCount());

                    if (valueCells.size() != v.getAtomCount())
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

    return BoundSqlExpression(exec, this, outputInfo);
}

Utf8String
EmbeddingLiteralExpression::
print() const
{
    Utf8String output =  "embed[";
    if (!clauses.empty())
        output += clauses[0]->print();

    for (size_t i = 1; i < clauses.size(); ++i)
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
    vector<std::shared_ptr<SqlExpression> > transformclauses = transformArgs(clauses);
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
bind(SqlBindingScope & scope) const
{
    auto boundLhs = lhs ? lhs->bind(scope) : BoundSqlExpression();
    auto boundRhs = rhs->bind(scope);

    BoundSqlExpression constFalse = {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    return storage = ExpressionValue(false, Date::negativeInfinity());
                },
                this,
                std::make_shared<BooleanValueInfo>(true)};

    BoundSqlExpression constTrue = {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    return storage = ExpressionValue(true, Date::negativeInfinity());
                },
                this,
                std::make_shared<BooleanValueInfo>(true)};

    BoundSqlExpression constNull = {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    return storage = ExpressionValue::null(Date::negativeInfinity());
                },
                this,
                std::make_shared<BooleanValueInfo>(true)};

    if (op == "AND" && lhs) {

        if ((boundLhs.info->isConst() && boundLhs.constantValue().isFalse())
            || (boundRhs.info->isConst() && boundRhs.constantValue().isFalse()) ) {
            return constFalse;
        }

        if ((boundLhs.info->isConst() && boundLhs.constantValue().empty())
            || (boundRhs.info->isConst() && boundRhs.constantValue().empty()) ) {
            return constNull;
        }

        bool constant = (boundLhs.info->isConst() && boundRhs.info->isConst());

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
                        return storage = ExpressionValue(false, ts);
                    }
                    else if (l.isFalse()) {
                        return storage = ExpressionValue(false, l.getEffectiveTimestamp());
                    }
                    else if (r.isFalse()) {
                        return storage = ExpressionValue(false, r.getEffectiveTimestamp());
                    }
                    else if (l.empty() && r.empty()) {
                        Date ts = std::min(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = ExpressionValue::null(ts);
                    }
                    else if (l.empty())
                        return storage = ExpressionValue::null(l.getEffectiveTimestamp());
                    else if (r.empty())
                        return storage = ExpressionValue::null(r.getEffectiveTimestamp());
                    Date ts = std::max(l.getEffectiveTimestamp(),
                                       r.getEffectiveTimestamp());
                    return storage = ExpressionValue(true, ts);
                },
                this,
                std::make_shared<BooleanValueInfo>(constant)};
    }
    else if (op == "OR" && lhs) {

        if ((boundLhs.info->isConst() && boundLhs.constantValue().isTrue())
            || (boundRhs.info->isConst() && boundRhs.constantValue().isTrue()) ) {
            return constTrue;
        }

        if ((boundLhs.info->isConst() && boundLhs.constantValue().empty())
            || (boundRhs.info->isConst() && boundRhs.constantValue().empty()) ) {
            return constNull;
        }


        bool constant = (boundLhs.info->isConst() && boundRhs.info->isConst());

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
                        return storage = ExpressionValue(true, ts);
                    }
                    else if (l.isTrue()) {
                        return storage = ExpressionValue(true, l.getEffectiveTimestamp());
                    }
                    else if (r.isTrue()) {
                        return storage = ExpressionValue(true, r.getEffectiveTimestamp());
                    }
                    else if (l.empty() && r.empty()) {
                        Date ts = std::max(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = ExpressionValue::null(ts);
                    }
                    else if (l.empty())
                        return storage = ExpressionValue::null(l.getEffectiveTimestamp());
                    else if (r.empty())
                        return storage =ExpressionValue::null(r.getEffectiveTimestamp());
                    Date ts = std::min(l.getEffectiveTimestamp(),
                                       r.getEffectiveTimestamp());
                    return storage = ExpressionValue(false, ts);
                },
                this,
                std::make_shared<BooleanValueInfo>(constant)};
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
                    return storage = ExpressionValue(!r.isTrue(), r.getEffectiveTimestamp());
                },
                this,
                std::make_shared<BooleanValueInfo>(boundRhs.info->isConst())};
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
bind(SqlBindingScope & scope) const
{
    auto boundExpr = expr->bind(scope);

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
                return storage = ExpressionValue(notType ? !val : val,
                                                 v.getEffectiveTimestamp());
            },
            this,
            std::make_shared<BooleanValueInfo>(boundExpr.info->isConst())};
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
/* FUNCTION CALL EXPRESSION                                                  */
/*****************************************************************************/

FunctionCallExpression::
FunctionCallExpression(Utf8String tableName,
                       Utf8String functionName,
                       std::vector<std::shared_ptr<SqlExpression> > args)
    : tableName(std::move(tableName)),
      functionName(std::move(functionName)),
      args(std::move(args))
{
    // Catch accidentally passing functionName as tableName
    ExcAssert(!this->functionName.empty());
}

FunctionCallExpression::
~FunctionCallExpression()
{
}

BoundSqlExpression
FunctionCallExpression::
bind(SqlBindingScope & scope) const
{
    //check whether it is a builtin or not
    if (scope.functionStackDepth > 100)
            throw HttpReturnException
                (400, "Reached a stack depth of over 100 functions while "
                 "analysing query, possible infinite recursion");
    
    ++scope.functionStackDepth;
    Scope_Exit(--scope.functionStackDepth);

    std::vector<BoundSqlExpression> boundArgs;
    for (auto& arg : args) {
        boundArgs.emplace_back(arg->bind(scope));
    }

    BoundFunction fn = scope.doGetFunction(tableName, functionName,
                                           boundArgs, scope);
    
    if (!fn && !fn.bindFunction) {
        Utf8String message = "Unable to find function '" + functionName + "'";
        if (!tableName.empty())
            message += " in dataset '" + tableName + "'";
        message += " binding function call '";
        message += (surface.empty() ? print() : surface);
        message += "'.  The function is not a built-in function, and either "
            "it's not a registered user function, or user functions are not "
            "available in the scope of the expression.";
        throw HttpReturnException(400, message,
                                  "functionName", functionName,
                                  "tableName", tableName,
                                  "scopeType", MLDB::type_name(scope),
                                  "expr", print(),
                                  "surface", surface);
    }

    if (fn.bindFunction) {
        return fn.bindFunction(scope, boundArgs, this);
    }
    return bindBuiltinFunction(scope, boundArgs, fn);
}

BoundSqlExpression
FunctionCallExpression::
bindBuiltinFunction(SqlBindingScope & scope,
                    std::vector<BoundSqlExpression>& boundArgs,
                    BoundFunction& fn) const
{
    bool isAggregate = tryLookupAggregator(functionName) != nullptr;

    if (isAggregate) {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    std::vector<ExpressionValue> evaluatedArgs;
                    // ??? BAD SMELL
                    //Don't evaluate the args for aggregator
                    evaluatedArgs.resize(boundArgs.size());
                    return storage = fn(evaluatedArgs, row);
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
                        evaluatedArgs.emplace_back(a(row, fn.filter));

                    return storage = fn(evaluatedArgs, row);
                },
                this,
                fn.resultInfo};
    }
}

Utf8String
FunctionCallExpression::
print() const
{
    Utf8String result = "function(" + jsonEncodeUtf8(tableName)
        + "," + jsonEncodeUtf8(functionName);
        
    for (auto & a : args) {
        result += "," + a->print();
    }

    result += ")";

    return result;   
}

std::shared_ptr<SqlExpression>
FunctionCallExpression::
transform(const TransformArgs & transformArgs) const
{
    auto newArgs = transformArgs(args);
    auto result = std::make_shared<FunctionCallExpression>(*this);
    result->args = newArgs;
    return result;
}

std::string
FunctionCallExpression::
getType() const
{
    return "function";
}

Utf8String
FunctionCallExpression::
getOperation() const
{
    return functionName;
}

std::vector<std::shared_ptr<SqlExpression> >
FunctionCallExpression::
getChildren() const
{   
    std::vector<std::shared_ptr<SqlExpression> > res = args;
   
    // We don't include extract here as the variables referred to
    // must be satisfied internally, so it's really an internal
    // part of the expression not a child expression.

    return res;
}

std::map<ScopedName, UnboundVariable>
FunctionCallExpression::
variableNames() const
{
    std::map<ScopedName, UnboundVariable> result;
    
    for (auto & c: args) {
        auto childVars = (*c).variableNames();
        for (auto & cv: childVars) {
            result[cv.first].merge(std::move(cv.second));
        }
    }

    // We don't include the extract values here
    
    return result;
}

std::map<ScopedName, UnboundFunction>
FunctionCallExpression::
functionNames() const
{
    std::map<ScopedName, UnboundFunction> result;
    // TODO: actually get arguments
    result[ScopedName(tableName, ColumnPath(functionName))]
        .argsForArity[args.size()] = {};
    
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
/* FUNCTION CALL EXPRESSION                                                  */
/*****************************************************************************/

ExtractExpression::
ExtractExpression(std::shared_ptr<SqlExpression> from,
                  std::shared_ptr<SqlExpression> extract)
    : from(std::move(from)), extract(std::move(extract))
{
    ExcAssert(this->from);
    ExcAssert(this->extract);
}

ExtractExpression::
~ExtractExpression()
{
}

BoundSqlExpression
ExtractExpression::
bind(SqlBindingScope & outerScope) const
{
    BoundSqlExpression fromBound = from->bind(outerScope);

    SqlExpressionExtractScope extractScope(outerScope, fromBound.info);

    BoundSqlExpression extractBound = extract->bind(extractScope);

    bool isConst = fromBound.info->isConst();

    //We know for sure the extract is const if it access no function, tables or wildcards *and*
    //all the variables are in the (const) lhs
    //This excludes many legit const cases.
    if (isConst) {
        auto columnNames = fromBound.info->allColumnNames();
        auto unbounds = extract->getUnbound();
        isConst = isConst && unbounds.tables.empty() && unbounds.wildcards.empty() && unbounds.funcs.empty();

        for (const auto& var : unbounds.vars) {
            const auto& varPath = var.first;
            isConst = std::find(columnNames.begin(), columnNames.end(), varPath) != columnNames.end();
        }
    }

    auto outputInfo = extractBound.info->getConst(isConst);

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {		
                ExpressionValue storage2;
                const ExpressionValue & fromOutput
                    = fromBound(row, storage2, filter);

                auto extractRowScope = extractScope.getRowScope(fromOutput);

                return extractBound(extractRowScope, storage, filter);
            },
            this,		
            outputInfo};
}

Utf8String
ExtractExpression::
print() const
{
    return "extract(\"" + from->print() + "," + extract->print() + "\")";
}

std::shared_ptr<SqlExpression>
ExtractExpression::
transform(const TransformArgs & transformArgs) const
{
    auto newArgs = transformArgs({from, extract});
    auto result = std::make_shared<ExtractExpression>(*this);
    result->from = newArgs[0];
    result->extract = newArgs[1];
    return result;
}

std::string
ExtractExpression::
getType() const
{
    return "extract";
}

Utf8String
ExtractExpression::
getOperation() const
{
    return "";
}

std::vector<std::shared_ptr<SqlExpression> >
ExtractExpression::
getChildren() const
{   
    // We don't include extract here as the variables referred to
    // must be satisfied internally, so it's really an internal
    // part of the expression not a child expression.

    return { from };
}

std::map<ScopedName, UnboundVariable>
ExtractExpression::
variableNames() const
{
    std::map<ScopedName, UnboundVariable> result;

    // This is:
    // - all variable names in the from
    // - plus all variable names in the extract not satisfied by the from
    // For the moment, assume that all in the extract are satisfied by
    // the from

    return from->variableNames();
}

std::map<ScopedName, UnboundFunction>
ExtractExpression::
functionNames() const
{
    std::map<ScopedName, UnboundFunction> result = from->functionNames();

    for (auto & f: extract->functionNames())
        result[f.first].merge(f.second);

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
bind(SqlBindingScope & scope) const
{
    std::shared_ptr<ExpressionValueInfo> info;
    bool isConst = true;

    BoundSqlExpression boundElse;
    if (elseExpr) {
        boundElse = elseExpr->bind(scope);
        info = boundElse.info;
        isConst = isConst && boundElse.info->isConst();
    }

    std::vector<std::pair<BoundSqlExpression, BoundSqlExpression> > boundWhen;

    for (auto & w: when) {
        boundWhen.emplace_back(w.first->bind(scope), w.second->bind(scope));
        if (info)
            info = VariantExpressionValueInfo::createVariantValueInfo(info, boundWhen.back().second.info);
        else
            info = boundWhen.back().second.info;

        isConst = isConst && boundWhen.back().first.info->isConst();
        isConst = isConst && boundWhen.back().second.info->isConst();
    }

    if (expr) {
        // Simple CASE expression

        auto boundExpr = expr->bind(scope);
        isConst = isConst && boundExpr.info->isConst();

        auto outputInfo = info->getConst(isConst);

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

                    if (elseExpr) {
                        return boundElse(row, storage, filter);
                    }

                    if (boundWhen.size() > 0 && boundWhen[0].second.info->isRow()) {
                        // No else defined, first when returned a row,
                        // return an empty row as default else
                        return storage = ExpressionValue(RowValue());
                    }

                    // default else returns an empty value
                    return storage = ExpressionValue();
                },
                this,
                outputInfo};
    }
    else {
        // Searched CASE expression

        auto outputInfo = info->getConst(isConst);

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
                    else return storage = ExpressionValue();
                },
                this,
                outputInfo};
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
        result->expr = transformArgs({result->expr})[0];

    for (auto & w: result->when) {
        w.first = transformArgs({ w.first })[0];
        w.second = transformArgs({ w.second })[0];
    }

    if (elseExpr)
        result->elseExpr = transformArgs({elseExpr })[0];

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
bind(SqlBindingScope & scope) const
{
    BoundSqlExpression boundExpr  = expr->bind(scope);
    BoundSqlExpression boundLower = lower->bind(scope);
    BoundSqlExpression boundUpper = upper->bind(scope);
    bool isConstant = boundExpr.info->isConst() && boundLower.info->isConst() && boundUpper.info->isConst();

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
                    return storage = ExpressionValue(notBetween,
                                                     std::max(v.getEffectiveTimestamp(),
                                                              l.getEffectiveTimestamp()));

                if (u.empty())
                    return storage = u;
                if (v > u)
                    return storage = ExpressionValue(notBetween,
                                                     std::max(v.getEffectiveTimestamp(),
                                                              u.getEffectiveTimestamp()));

                return storage = ExpressionValue(!notBetween,
                                                 std::max(std::max(v.getEffectiveTimestamp(),
                                                                   u.getEffectiveTimestamp()),
                                                          l.getEffectiveTimestamp()));
            },
            this,
            std::make_shared<BooleanValueInfo>(isConstant)};
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
    result->expr  = transformArgs({result->expr})[0];
    result->lower = transformArgs({result->lower})[0];
    result->upper = transformArgs({result->upper})[0];
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
      isNegative(negative),
      kind(TUPLE)
{
}

InExpression::
InExpression(std::shared_ptr<SqlExpression> expr,
             std::shared_ptr<SelectSubtableExpression> subtable,
             bool negative)
    : expr(std::move(expr)),
      subtable(std::move(subtable)),
      isNegative(negative),
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
      isNegative(negative),
      kind(kind)
{
    ExcAssert(kind == KEYS || kind == VALUES);
}

BoundSqlExpression
InExpression::
bind(SqlBindingScope & scope) const
{
    BoundSqlExpression boundExpr  = expr->bind(scope);
    bool isConstant = boundExpr.info->isConst();

    //cerr << "boundExpr: " << expr->print() << " has subtable " << subtable
    //     << endl;

    switch (kind) {
    case SUBTABLE: {
        BoundTableExpression boundTable = subtable->bind(scope, nullptr /*onProgress*/);

        isConstant = false; //TODO

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
                = boundTable.table.runQuery(scope, SelectExpression::STAR,
                                            WhenExpression::TRUE,
                                            *SqlExpression::TRUE,
                                            orderBy,
                                            offset, limit,
                                            nullptr /*onProgress*/);
            
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
                    return storage = ExpressionValue(isNegative ? !found : found,
                                                     v.getEffectiveTimestamp());
                };
            
            return { exec, this, std::make_shared<BooleanValueInfo>() };
        }
    }
    case TUPLE: {
        // We have an explicit tuple, not a subquery.

        std::vector<BoundSqlExpression> tupleExpressions;
        tupleExpressions.reserve(tuple->clauses.size());

        for (auto & tupleItem: tuple->clauses) {
            tupleExpressions.emplace_back(tupleItem->bind(scope));
            isConstant = isConstant && tupleExpressions.back().info->isConst();
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
                    return storage = ExpressionValue(!isNegative,
                                                     std::max(v.getEffectiveTimestamp(),
                                                              itemValue.getEffectiveTimestamp()));
                }
            }

            return storage = ExpressionValue(isNegative, v.getEffectiveTimestamp());
        },
        this,
        std::make_shared<BooleanValueInfo>(isConstant)};
    }
    case KEYS: {
        BoundSqlExpression boundSet = setExpr->bind(scope);

        isConstant = false; //TODO

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
                return storage = ExpressionValue(!isNegative,
                                                 std::max(v.getEffectiveTimestamp(),
                                                          found.second));
            }

            return storage = ExpressionValue(isNegative, v.getEffectiveTimestamp());
        
        },
        this,
        std::make_shared<BooleanValueInfo>(isConstant)};
    }
    case VALUES: {
        BoundSqlExpression boundSet = setExpr->bind(scope);

        isConstant = isConstant && boundSet.info->isConst();

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
                return storage = ExpressionValue(!isNegative,
                                                 std::max(v.getEffectiveTimestamp(),
                                                          found.second));
            }

            return storage = ExpressionValue(isNegative, v.getEffectiveTimestamp());
        },
        this,
        std::make_shared<BooleanValueInfo>(isConstant)};
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
    result->expr  = transformArgs({result->expr})[0];

    switch (kind) {
    case SUBTABLE:
        //SelectSubtableExpression is  a tableExpression, not an SQLExpression
        result->subtable = std::make_shared<SelectSubtableExpression>(*(result->subtable));
        break;
    case TUPLE:
        result->tuple->transform(transformArgs); //tuple is not an SQLExpression
        break;
    case KEYS:
    case VALUES:
        result->setExpr = transformArgs({result->setExpr})[0];
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
      isNegative(negative)
{
}

BoundSqlExpression
LikeExpression::
bind(SqlBindingScope & scope) const
{
    BoundSqlExpression boundLeft  = left->bind(scope);
    BoundSqlExpression boundRight  = right->bind(scope);

    auto applier = std::make_shared<ApplyLike>(boundRight, isNegative);
    bool isConst = boundLeft.info->isConst() && boundRight.info->isConst();

    if (applier->isPrecompiled) {
    
        return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue vstorage;
                    const ExpressionValue & value
                        = boundLeft(rowScope, vstorage, filter);
                    return storage = (*applier)({value, ExpressionValue()},
                                                rowScope);
                },
                this,
                std::make_shared<BooleanValueInfo>(isConst)};
    }
    else {
        return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue vstorage, fstorage;
                    const ExpressionValue & value
                        = boundLeft(rowScope, vstorage, filter);
                    const ExpressionValue & likeFilter
                        = boundRight(rowScope, fstorage, filter);
                    return storage = (*applier)({value, likeFilter}, rowScope);
                },
                this,
                std::make_shared<BooleanValueInfo>(isConst)};
    }
}

Utf8String
LikeExpression::
print() const
{
    Utf8String result
        = (isNegative ? "not like(" : "like(")
        + left->print()
        + ","
        + right->print()
        + ")";

    return result;
}

std::shared_ptr<SqlExpression>
LikeExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<LikeExpression>(*this);
    result->left  = transformArgs({result->left})[0];
    result->right  = transformArgs({result->right})[0];

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

    return children;

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
bind(SqlBindingScope & scope) const
{
    BoundSqlExpression boundExpr  = expr->bind(scope);

    if (type == "string") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(val.coerceToString(),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<StringValueInfo>(boundExpr.info->isConst())};
    }
    else if (type == "integer") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(val.coerceToInteger(),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<IntegerValueInfo>(boundExpr.info->isConst())};
    }
    else if (type == "number") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(val.coerceToNumber(),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<Float64ValueInfo>(boundExpr.info->isConst())};
    }
    else if (type == "timestamp") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(val.coerceToTimestamp(),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<IntegerValueInfo>(boundExpr.info->isConst())};
    }
    else if (type == "boolean") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(val.coerceToBoolean(),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<BooleanValueInfo>(boundExpr.info->isConst())};
    }
    else if (type == "blob") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(val.coerceToBlob(),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<BlobValueInfo>(boundExpr.info->isConst())};
    }
    else if (type == "path") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage, filter);
                    return storage = ExpressionValue(CellValue(val.coerceToPath()),
                                                     val.getEffectiveTimestamp());
                },
                this,
                std::make_shared<PathValueInfo>(boundExpr.info->isConst())};
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
bind(SqlBindingScope & scope) const
{
    auto getParam = scope.doGetBoundParameter(paramName);

    if (!getParam.info) {
        throw HttpReturnException(400, "scope " + MLDB::type_name(scope)
                            + " getBoundParameter '" + paramName
                            + "' didn't return info");
    }

    auto outputInfo = getParam.info->getConst(false);

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                return getParam(row, storage, filter);
            },
            this,
            outputInfo};
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
WildcardExpression(ColumnPath prefix,
                   ColumnPath asPrefix,
                   std::vector<std::pair<ColumnPath, bool> > excluding)
    : prefix(std::move(prefix)), asPrefix(std::move(asPrefix)),
      excluding(std::move(excluding))
{
}

BoundSqlExpression
WildcardExpression::
bind(SqlBindingScope & scope) const
{
    ColumnPath simplifiedPrefix = prefix;
    Utf8String resolvedTableName;

    //cerr << "binding wildcard expression " << print() << endl;
    //cerr << "prefix = " << prefix << endl;
    //cerr << "asPrefix = " << asPrefix << endl;

    ColumnFilter newColumnName = ColumnFilter::identity();

    if (!prefix.empty() || !excluding.empty() || !asPrefix.empty()){
       
        if (!prefix.empty())
            simplifiedPrefix = scope.doResolveTableName(prefix, resolvedTableName);

        //cerr << "tableName = " << resolvedTableName << endl;
        //cerr << "simplifiedPrefix = " << simplifiedPrefix << endl;

        // This function figures out the new name of the column.  If it's excluded,
        // then it returns the empty column name
        newColumnName = ColumnFilter([=] (const ColumnPath & inputColumnName) -> ColumnPath
            {
                //cerr << "input column name " << inputColumnName << endl;

                // First, check it matches the prefix
                // We have to check the simplified prefix for regular datasets
                // i.e select x.a.* from x returns a.b
                // But we have to check the initial prefix for joins
                // i.e select x.a.* from x join y returns x.a.b
                if (!inputColumnName.matchWildcard(simplifiedPrefix)
                    && !inputColumnName.matchWildcard(prefix)) {
                    //cerr << "rejected by prefix: " << simplifiedPrefix << "," << prefix << endl;
                    return ColumnPath();
                }

                // Second, check it doesn't match an exclusion
                for (auto & ex: excluding) {
                    if (ex.second) {
                        // prefix
                        if (inputColumnName.matchWildcard(ex.first))
                            return ColumnPath();
                    }
                    else {
                        // exact match
                        if (inputColumnName == ex.first)
                            return ColumnPath();
                    }
                }

                // Finally, replace the prefix with the new prefix
                if (!simplifiedPrefix.empty() || (prefix != asPrefix)) {

                    if (prefix != asPrefix) {
                        //cerr << "replacing wildcard " << prefix
                        // << " with " << asPrefix << " on " << inputColumnName << endl;
                        //cerr << "result: " << inputColumnName.replaceWildcard(prefix, asPrefix) << endl;

                        return inputColumnName.replaceWildcard(prefix, asPrefix);
                    }
                }

                //cerr << "kept" << endl;
                return inputColumnName;
            });
    }   

    auto allColumns = scope.doGetAllAtoms(resolvedTableName, newColumnName);

    auto exec = [=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
        -> const ExpressionValue &
        {
            return storage = allColumns.exec(scope, filter);
        };

    BoundSqlExpression result(exec, this, allColumns.info);

    return result;
}

Utf8String
WildcardExpression::
print() const
{
    Utf8String result = "columns(" + jsonEncodeUtf8(prefix) + ","
        + jsonEncodeUtf8(asPrefix) + ",[";
    
    bool first = true;
    for (auto ex: excluding) {
        if (!first) {
            result += ",";
        }
        first = false;
        if (ex.second)
            result += "wildcard(\"" + ex.first.toUtf8String() + "\")";
        else 
            result += "column(\"" + ex.first.toUtf8String() + "\")";
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
    // tough to do without a scope...
    return {};
}

std::map<ScopedName, UnboundWildcard>
WildcardExpression::
wildcards() const
{
    std::map<ScopedName, UnboundWildcard> result;
    result[{"" /*tableName*/, ColumnPath(prefix + "*")}].prefix = prefix;
    return result;
}

bool
WildcardExpression::
isIdentitySelect(SqlExpressionDatasetScope & scope) const
{
    // A select * is identified like this
    return prefix.empty()
        && asPrefix.empty()
        && excluding.empty();

    // TO RESOLVE BEFORE MERGE
    // && (tableName.empty() || scope.childaliases.empty());
}


/*****************************************************************************/
/* COMPUTED VARIABLE                                                         */
/*****************************************************************************/

NamedColumnExpression::
NamedColumnExpression(ColumnPath alias,
               std::shared_ptr<SqlExpression> expression)
    : alias(std::move(alias)),
      expression(std::move(expression))
{
}

BoundSqlExpression
NamedColumnExpression::
bind(SqlBindingScope & scope) const
{
    auto exprBound = expression->bind(scope);

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
     
        auto exec = [=] (const SqlRowScope & scope,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
            -> const ExpressionValue &
            {
                const ExpressionValue & val = exprBound(scope, storage, filter);

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
        auto exec = [=] (const SqlRowScope & scope,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue valStorage;
                const ExpressionValue & val
                    = exprBound(scope, valStorage, filter);

                // Recursive function to build up an ExpressionValue
                // with the same structure as the alias
                std::function<ExpressionValue (size_t i)>
                getRow = [&] (size_t i) -> ExpressionValue
                {
                    if (i == alias.size()) {
                        if (&val == &valStorage) {
                            // We own the only copy; we can move it
                            return std::move(valStorage);
                        }
                        else {
                            // We got a reference; copy it
                            return val;
                        }
                    }
                    else {
                        StructValue row;
                        row.emplace_back(alias[i], getRow(i + 1));
                        return std::move(row);
                    }
                };

                return storage = getRow(0);
            };

        std::vector<KnownColumn> knownColumns = {
            KnownColumn(alias, exprBound.info, COLUMN_IS_DENSE) };
        
        auto info = std::make_shared<RowValueInfo>
            (knownColumns, SCHEMA_CLOSED, exprBound.info->isConst());

        return BoundSqlExpression(exec, this, info);
    }
}

Utf8String
NamedColumnExpression::
print() const
{
    return "computed(\"" + alias.toUtf8String() + "\"," + expression->print() + ")";
}

std::shared_ptr<SqlExpression>
NamedColumnExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<NamedColumnExpression>(*this);
    result->expression = transformArgs({ expression}).at(0);
    return result;
}

std::vector<std::shared_ptr<SqlExpression> >
NamedColumnExpression::
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
                       int64_t limit,
                       bool isStructured)
    : select(std::move(select)),
      as(std::move(as)),
      where(std::move(where)),
      orderBy(std::move(orderBy)),
      offset(offset),
      limit(limit),
      isStructured(isStructured)
{
}

BoundSqlExpression
SelectColumnExpression::
bind(SqlBindingScope & scope) const
{
    // 1.  Get all columns
    ColumnFilter filter = ColumnFilter::identity();
    auto allColumns 
        = isStructured ? scope.doGetAllColumns("" /* table name */, filter) : scope.doGetAllAtoms("" /* table name */, filter);
    
    bool hasDynamicColumns
        = allColumns.info->getSchemaCompletenessRecursive() == SCHEMA_OPEN;
    
    // Only known columns are kept.  For each one, we filter it then calculate
    // the order by expression.

    struct ColumnEntry {
        ColumnPath inputColumnName;
        ColumnPath columnName;
        ColumnHash columnHash;
        int columnNumber;
        std::vector<ExpressionValue> sortFields;
        std::shared_ptr<ExpressionValueInfo> valueInfo;
        ColumnSparsity sparsity;
    };

    std::vector<ColumnEntry> columns;

    // Bind those expressions that operate in the column scope
    ColumnExpressionBindingScope colScope(scope);

    auto boundWhere = where->bind(colScope);
    auto boundAs = as->bind(colScope);

    std::vector<BoundSqlExpression> boundOrderBy;
    for (auto & o: orderBy.clauses)
        boundOrderBy.emplace_back(o.first->bind(colScope));

    /// List of all functions to run in our run() operator
    std::vector<std::function<void (const SqlRowScope &, StructValue &)> > functionsToRun;

    std::vector<KnownColumn> knownColumns;

    knownColumns = allColumns.info->getKnownColumns(); 

    // For each group of columns, find which match
    for (unsigned j = 0;  j < knownColumns.size();  ++j) {
        const auto & col = knownColumns[j];

        const ColumnPath & columnName = col.columnName;
            
        auto thisScope = colScope.getColumnScope(columnName);

        bool keep = boundWhere(thisScope, GET_LATEST).isTrue();

        if (!keep)
            continue;

        ColumnPath newColName = boundAs(thisScope, GET_LATEST).coerceToPath();

        vector<ExpressionValue> orderBy;
        for (auto & c: boundOrderBy) {
            orderBy.emplace_back(c(thisScope, GET_LATEST));
        }

        ColumnEntry entry;
        entry.inputColumnName = columnName;
        entry.columnName = ColumnPath(newColName);
        entry.columnHash = entry.columnName;
        entry.columnNumber = j;
        entry.sortFields = std::move(orderBy);
        entry.valueInfo = col.valueInfo;
        entry.sparsity = col.sparsity;

        columns.emplace_back(std::move(entry));
    }
   
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

    bool selectValue
        = select->getType() == "function"
        && select->getOperation() == "value";

    bool asColumnPath
        = as->getType() == "function"
        && as->getOperation() == "columnPath";

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

    std::unordered_map<ColumnPath, ColumnPath> keepColumns;
    for (auto & c: columns)
        keepColumns[c.inputColumnName]
            = c.columnName;

    if (selectValue && asColumnPath && !hasDynamicColumns) {

        ColumnFilter filterColumns([=] (const ColumnPath & name) -> ColumnPath
            {
                auto it = keepColumns.find(name);
                if (it == keepColumns.end()) {
                    return ColumnPath();
                }
                return it->second;
            });
    
        // Finally, return a filtered set from the underlying dataset
        auto outputColumns
            = isStructured ? scope.doGetAllColumns("" /* prefix */, filterColumns) : 
                             scope.doGetAllAtoms("" /* prefix */, filterColumns);

        auto exec = [=] (const SqlRowScope & scope,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
            -> const ExpressionValue &
            {
                return storage = outputColumns.exec(scope, filter);
            };

        BoundSqlExpression result(exec, this, outputColumns.info);
    
        return result;
    }
    else {

        ColumnFilter filterColumns = ColumnFilter::identity();

        BoundSqlExpression boundSelect = select->bind(colScope);

        auto exec = [=] (const SqlRowScope & scope,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExpressionValue input = allColumns.exec(scope, filter);

                RowValue output;
               
                auto onValue = [&] (const ColumnPath & columnName,
                                   ExpressionValue in)
                {
                    auto scope = ColumnExpressionBindingScope
                        ::getColumnScope(columnName, in);

                    bool keep = boundWhere(scope, GET_LATEST).isTrue();

                    if (!keep)
                        return true;

                    ExpressionValue storage;
                    if (selectValue)
                        storage = std::move(in);
                    const ExpressionValue & result
                        = selectValue
                            ? storage
                            : boundSelect(scope, storage, GET_ALL);

                    ColumnPath columnNameStorage;
                    const ColumnPath * columnNameOut = &columnName;
                    if (!asColumnPath) {
                        ExpressionValue tmp;
                        columnNameStorage
                            = boundAs(scope, tmp, GET_ALL).coerceToPath();
                        columnNameOut = &columnNameStorage;
                    }
                    
                    if (&result == &storage) {
                        if (storage.isAtom()) {
                            // Put directly in place
                            if (columnNameOut->empty()) {
                                throw HttpReturnException(400, "Cannot have a NULL column name");
                            }
                            output.emplace_back(std::move(*columnNameOut),
                                                storage.stealAtom(),
                                                storage.getEffectiveTimestamp());
                        }
                        else {
                            // The expression may produce more than one atom as an
                            // output, so take all of them.
                            auto onAtom2 = [&] (ColumnPath & columnName2,
                                                CellValue & val,
                                                Date ts)
                            {
                                output.emplace_back(columnName + columnName2,
                                                    std::move(val),
                                                    ts);
                                return true;
                            };

                            storage.forEachAtomDestructive(onAtom2);
                        }
                    }
                    else {

                        auto onAtom2 = [&] (const ColumnPath & prefix,
                                            const ColumnPath & suffix,
                                            const CellValue & val,
                                            Date ts)
                            {
                                output.emplace_back((*columnNameOut)
                                                    + prefix + suffix,
                                                    std::move(val),
                                                    ts);
                                return true;
                            };
                        
                        result.forEachAtom(onAtom2);
                    }

                    return true;
                };

                auto onAtom = [&] (ColumnPath & columnName,
                                   CellValue & val,
                                   Date ts) -> bool
                {
                    return onValue(columnName,ExpressionValue(val, ts));
                };

                auto onColumn = [&] (PathElement & columnName, ExpressionValue & val) -> bool
                {
                    return onValue(ColumnPath(columnName), val);
                };
                
                if (isStructured)
                    input.forEachColumnDestructive(onColumn);
                else 
                    input.forEachAtomDestructive(onAtom);

                return storage = std::move(output);
            };

        BoundSqlExpression result(exec, this, allColumns.info);
    
        return result;
    }
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
    result[{"", ColumnPath("*")}].prefix = ColumnPath();
    return result;
}


  
BoundSqlExpression
GroupByKeyExpression::
bind(SqlBindingScope & scope) const
{
    auto getGroupbyKey = scope.doGetGroupByKey(index);

    if (!getGroupbyKey.info) {
        throw HttpReturnException(400, "scope " + MLDB::type_name(scope)
                            + " doGetGroupByKey '"
                            + "' didn't return info");
    }

    //GroupByKeyExpression are never constant because we need the group by row
    //to evaluate them.
    auto outputInfo = getGroupbyKey.info->getConst(false);

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                return getGroupbyKey(row, storage, filter);
            },
            this,
            outputInfo};
}

Utf8String
GroupByKeyExpression::
print() const
{
    return "GroupBy Key [" + to_string(index) + "]";
}

std::shared_ptr<SqlExpression>
GroupByKeyExpression::
transform(const TransformArgs & transformArgs) const
{
    return make_shared<GroupByKeyExpression>(index);
}

std::vector<std::shared_ptr<SqlExpression> >
GroupByKeyExpression::
getChildren() const
{
    return {};
}

} // namespace MLDB


