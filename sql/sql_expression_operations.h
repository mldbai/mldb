/** sql_expression_operations.h                                    -*- C++ -*-
    Jeremy Barnes, 24 February 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

*/

#pragma once

#include "sql_expression.h"

namespace Datacratic {
namespace MLDB {

struct SelectSubtableExpression; 

/*****************************************************************************/
/* CONCRETE EXPRESSION TYPES                                                 */
/*****************************************************************************/

struct ComparisonExpression: public SqlExpression {
    ComparisonExpression(std::shared_ptr<SqlExpression> lhs,
                         std::shared_ptr<SqlExpression> rhs,
                         std::string op);

    virtual ~ComparisonExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> lhs;
    std::shared_ptr<SqlExpression> rhs;
    std::string op;
};

struct ArithmeticExpression: public SqlExpression {
    ArithmeticExpression(std::shared_ptr<SqlExpression> lhs,
                            std::shared_ptr<SqlExpression> rhs,
                            std::string op);

    virtual ~ArithmeticExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> lhs;
    std::shared_ptr<SqlExpression> rhs;
    std::string op;
};

struct BitwiseExpression: public SqlExpression {
    BitwiseExpression(std::shared_ptr<SqlExpression> lhs,
                            std::shared_ptr<SqlExpression> rhs,
                            std::string op);

    virtual ~BitwiseExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> lhs;
    std::shared_ptr<SqlExpression> rhs;
    std::string op;
};

struct BooleanOperatorExpression: public SqlExpression {
    BooleanOperatorExpression(std::shared_ptr<SqlExpression> lhs,
                              std::shared_ptr<SqlExpression> rhs,
                              std::string op);
    
    virtual ~BooleanOperatorExpression();
    
    virtual BoundSqlExpression bind(SqlBindingScope & context) const;
    
    virtual Utf8String print() const;
    
    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> lhs;
    std::shared_ptr<SqlExpression> rhs;
    std::string op;
};

struct ReadColumnExpression: public SqlExpression {
    ReadColumnExpression(ColumnName columnName);

    virtual ~ReadColumnExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::map<ScopedName, UnboundVariable>
    variableNames() const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;
    virtual bool isConstant() const { return false; }

    ColumnName columnName;
};

struct ConstantExpression: public SqlExpression {
    ConstantExpression(ExpressionValue constant);

    virtual ~ConstantExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    virtual bool isConstant() const;
    virtual ExpressionValue constantValue() const;

    ExpressionValue constant;
};

struct IsTypeExpression: public SqlExpression {
    IsTypeExpression(std::shared_ptr<SqlExpression> expr,
                     bool notExpr,
                     std::string type);

    virtual ~IsTypeExpression();

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> expr;
    bool notType;
    std::string type;
};

struct SelectWithinExpression: public SqlExpression {
    SelectWithinExpression(std::shared_ptr<SqlRowExpression> select);

    virtual ~SelectWithinExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlRowExpression> select;
};

struct EmbeddingLiteralExpression: public SqlExpression {
    EmbeddingLiteralExpression(std::vector<std::shared_ptr<SqlExpression> > clauses);

    virtual ~EmbeddingLiteralExpression();

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::vector<std::shared_ptr<SqlExpression> > clauses;
};

/** Represents CASE expr WHEN ... ELSE ... */

struct CaseExpression: public SqlExpression {
    CaseExpression(std::shared_ptr<SqlExpression> expr,
                   std::vector<std::pair<std::shared_ptr<SqlExpression>,
                                         std::shared_ptr<SqlExpression> > >
                   when,
                   std::shared_ptr<SqlExpression> elseExpr);

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    /// If expr is null, then it's a searched case expression.  Otherwise a simple
    /// case expression.
    std::shared_ptr<SqlExpression> expr;
    std::vector<std::pair<std::shared_ptr<SqlExpression>,
                          std::shared_ptr<SqlExpression> > > when;
    std::shared_ptr<SqlExpression> elseExpr;
};


/** Represents BETWEEN lower AND upper */

struct BetweenExpression: public SqlExpression {
    BetweenExpression(std::shared_ptr<SqlExpression> expr,
                      std::shared_ptr<SqlExpression> lower,
                      std::shared_ptr<SqlExpression> upper,
                      bool notBetween);

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> expr;
    std::shared_ptr<SqlExpression> lower;
    std::shared_ptr<SqlExpression> upper;
    bool notBetween;
};

/** Represents IN / NOT IN expressions */

struct InExpression: public SqlExpression {

    enum Kind {
        SUBTABLE,  ///< IN (select ...)
        TUPLE,     ///< IN (val1, val2, ...)
        KEYS,      ///< IN (KEYS OF expr)
        VALUES     ///< IN (VALUES OF expr)
    };

    // Constructor for IN (tuple)
    InExpression(std::shared_ptr<SqlExpression> expr,
                 std::shared_ptr<TupleExpression> tuple,
                 bool negative);

    // Constructor for IN (SELECT ...)
    InExpression(std::shared_ptr<SqlExpression> expr,
                 std::shared_ptr<SelectSubtableExpression> subtable,
                 bool negative);

    // Constructor for IN (KEYS OF ...) or IN (VALUES OF ...)
    InExpression(std::shared_ptr<SqlExpression> expr,
                 std::shared_ptr<SqlExpression> setExpr,
                 bool negative,
                 Kind kind);

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> expr;
    std::shared_ptr<TupleExpression> tuple;
    std::shared_ptr<SelectSubtableExpression> subtable;
    std::shared_ptr<SqlExpression> setExpr;

    bool isnegative;
    Kind kind;
};

struct LikeExpression: public SqlExpression {

    // Constructor for IN (tuple)
    LikeExpression(std::shared_ptr<SqlExpression> left,
                 std::shared_ptr<SqlExpression> right,
                 bool negative);

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> left;
    std::shared_ptr<SqlExpression> right;

    bool isnegative;
};

/** Represents CAST (expression AS type) */
struct CastExpression: public SqlExpression {
    CastExpression(std::shared_ptr<SqlExpression> expr,
                   std::string type);
    
    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::shared_ptr<SqlExpression> expr;
    std::string type;
};

/** Represents getting the value of a bound parameter. */
struct BoundParameterExpression: public SqlExpression {
    BoundParameterExpression(Utf8String paramName);
    
    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::map<Utf8String, UnboundVariable>
    parameterNames() const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;
    virtual bool isConstant() const { return false; }

    Utf8String paramName;
};


/*****************************************************************************/
/* SQL ROW EXPRESSIONS                                                       */
/*****************************************************************************/

/** Represents "SELECT tablename.* and SELECT *", as well as
    SELECT abc* AS def* and SELECT * EXCLUDING (bad*)
*/
struct WildcardExpression: public SqlRowExpression {
    WildcardExpression(ColumnName prefix,
                       ColumnName asPrefix,
                       std::vector<std::pair<ColumnName, bool> > excluding);

    ColumnName prefix;
    ColumnName asPrefix;
    std::vector<std::pair<ColumnName, bool> > excluding;

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const
    {
        return "selectWildcard";
    }

    virtual Utf8String getOperation() const
    {
        return prefix.toUtf8String();
    }

    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    virtual bool isConstant() const { return false; }

    std::map<ScopedName, UnboundWildcard>
    wildcards() const;

    virtual bool isIdentitySelect(SqlExpressionDatasetScope & context) const;
};

/** Represents "SELECT expression" */
struct ComputedColumn: public SqlRowExpression {
    ComputedColumn(ColumnName alias,
                   std::shared_ptr<SqlExpression>);

    ColumnName alias;  ///< Name of variable alias
    std::shared_ptr<SqlExpression> expression;

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const
    {
        return "selectExpr";
    }

    virtual Utf8String getOperation() const
    {
        return alias.toUtf8String();
    }

    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;
};

/** Wrapper when we dont know at parsing time if it is a user function
    or a built-in function.
*/
struct FunctionCallExpression: public SqlRowExpression {
    FunctionCallExpression(Utf8String tableName,
                           Utf8String functionName,
                           std::vector<std::shared_ptr<SqlExpression> > args);
    
    virtual ~FunctionCallExpression();
    
    Utf8String tableName;
    Utf8String functionName;
    std::vector<std::shared_ptr<SqlExpression> > args;

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;
    virtual bool isConstant() const { return false; } // TODO: not always

    virtual std::map<ScopedName, UnboundVariable>
    variableNames() const override;

    virtual std::map<ScopedName, UnboundFunction>
    functionNames() const override;

private:

    BoundSqlExpression
    bindBuiltinFunction(SqlBindingScope & context,
                        std::vector<BoundSqlExpression> & boundArgs,
                        BoundFunction& fn) const;
};

/** Represents extracting or rewriting an object. */
struct ExtractExpression: public SqlRowExpression {
    ExtractExpression(std::shared_ptr<SqlExpression> from,
                      std::shared_ptr<SqlExpression> extract);
    
    virtual ~ExtractExpression();
    
    std::shared_ptr<SqlExpression> from;
    std::shared_ptr<SqlExpression> extract;

    virtual BoundSqlExpression bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;
    virtual bool isConstant() const { return false; } // TODO: not always

    virtual std::map<ScopedName, UnboundVariable>
    variableNames() const override;

    virtual std::map<ScopedName, UnboundFunction>
    functionNames() const override;
};

/** Represents "SELECT COLUMNS expression" */
struct SelectColumnExpression: public SqlRowExpression {
    SelectColumnExpression(std::shared_ptr<SqlExpression> select,
                           std::shared_ptr<SqlExpression> as,
                           std::shared_ptr<SqlExpression> where,
                           OrderByExpression orderBy,
                           int64_t offset,
                           int64_t limit);

    std::shared_ptr<SqlExpression> select;
    std::shared_ptr<SqlExpression> as;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    int64_t offset;
    int64_t limit;

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual bool isConstant() const { return false; }

    virtual std::string getType() const
    {
        return "selectColumnExpr";
    }

    virtual Utf8String getOperation() const
    {
        return Utf8String();
    }

    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    std::map<ScopedName, UnboundWildcard> wildcards() const;
};

} // namespace MLDB
} // namespace Datacratic
