/** sql_expression_operations.h                                    -*- C++ -*-
    Jeremy Barnes, 24 February 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include "sql_expression.h"


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

/** Read a column from a dataset.  This corresponds
    to an expression `x` where `x` is a column name in a dataset.
    This expression is further wrapped into SqlRowExpressions
    to form the SELECT clauses.  For example, the expression
    `x AS y` is parsed as a NamedColumnExpression with surface
    `x AS y` containing a ReadColumnExpression with surface `x`.
*/
struct ReadColumnExpression: public SqlExpression {
    ReadColumnExpression(ColumnPath columnName);

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

    ColumnPath columnName;
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

    virtual bool isConstant() const;

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

    bool isNegative;
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

    bool isNegative;
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

    Utf8String paramName;
};


/*****************************************************************************/
/* SQL ROW EXPRESSIONS                                                       */
/*****************************************************************************/

/** Represents "SELECT tablename.* and SELECT *", as well as
    SELECT abc* AS def* and SELECT * EXCLUDING (bad*)
*/
struct WildcardExpression: public SqlRowExpression {
    WildcardExpression(ColumnPath prefix,
                       ColumnPath asPrefix,
                       std::vector<std::pair<ColumnPath, bool> > excluding);

    ColumnPath prefix;
    ColumnPath asPrefix;
    std::vector<std::pair<ColumnPath, bool> > excluding;

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

    virtual bool isWildcard() const {return true; }
};

/** Represents x AS y, y : x, x AS * or x that appears in a SELECT expression" */
struct NamedColumnExpression: public SqlRowExpression {
    NamedColumnExpression(ColumnPath alias,
                   std::shared_ptr<SqlExpression>);

    /** This value must be understood as follows:
        - the alias is set to the value of y in expression x AS y
        - it is empty in the case of x AS *
        - it contains the surface of x otherwise
    */
    ColumnPath alias;  ///< Name of variable alias
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

    virtual bool isConstant() const
    {
        return expression->isConstant();
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

    virtual BoundSqlExpression bind(SqlBindingScope & context) const override;

    virtual Utf8String print() const override;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const override;

    virtual std::string getType() const override;
    virtual Utf8String getOperation() const override;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const override;
    virtual bool isConstant() const override { return false; } // TODO: not always

    virtual std::map<ScopedName, UnboundVariable>
    variableNames() const override;

    virtual std::map<ScopedName, UnboundFunction>
    functionNames() const override;

    virtual bool isAggregator() const override
    {
        return !!tryLookupAggregator(functionName);
    }

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

    virtual BoundSqlExpression bind(SqlBindingScope & context) const override;

    virtual Utf8String print() const override;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const override;

    virtual std::string getType() const override;
    virtual Utf8String getOperation() const override;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const override;
    virtual bool isConstant() const override { return false; } // TODO: not always

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
                           int64_t limit,
                           bool isStructured);

    std::shared_ptr<SqlExpression> select;
    std::shared_ptr<SqlExpression> as;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    int64_t offset;
    int64_t limit;
    bool isStructured;

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

/** Read the value from a groupby key */
struct GroupByKeyExpression: public SqlRowExpression {
    GroupByKeyExpression(size_t index) : index(index) { }

    size_t index;

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const override;

    virtual Utf8String print() const override;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const override;

    virtual std::string getType() const override
    {
        return "groupByKey";
    }

    virtual Utf8String getOperation() const override
    {
        return getType() + "[" + std::to_string(index) + "]";
    }

    virtual bool isConstant() const override
    {
        return false;
    }

    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const override;
};

} // namespace MLDB
