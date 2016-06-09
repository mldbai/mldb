/** bound_queries.h                                                -*- C++ -*-
    Jeremy Barnes, 12 August 2015
    Bound form of SQL queries, that can be executed.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include "sql/sql_expression.h"
#include "server/analytics.h"


namespace Datacratic {
namespace MLDB {

struct GroupContext;
struct SqlExpressionDatasetScope;


/** This object is designed to track whether a thread is executing a
    query as a child thread (in which case it shouldn't create any
    extra threads) or as a parent (in which case it could).

*/
struct QueryThreadTracker {

    // Constructor for the parent thread
    QueryThreadTracker()
        : inParent(true)
    {
    }
    
    // Constructor for the child thread
    QueryThreadTracker child() const
    {
        QueryThreadTracker result;
        result.inParent = false;
        ++depth;
        return std::move(result);
    }

    // Destructor, which undoes the increment from the desctructor
    ~QueryThreadTracker()
    {
        if (!inParent)
            --depth;
    }
    
    static bool inChildThread() { return depth > 0; }

    bool inParent;
    static __thread int depth;

    QueryThreadTracker(const QueryThreadTracker &) = delete;
    void operator = (const QueryThreadTracker &) = delete;

    QueryThreadTracker(QueryThreadTracker && other)
    {
        *this = std::move(other);
    }

    QueryThreadTracker & operator = (QueryThreadTracker && other)
    {
        inParent = other.inParent;
        other.inParent = true;  // to avoid depth being decremented
        return *this;
    }
};


/*****************************************************************************/
/* BOUND SELECT QUERY                                                        */
/*****************************************************************************/

struct BoundSelectQuery {
    struct Executor;

    const SelectExpression & select;
    const Dataset & from;
    const WhenExpression & when;
    const SqlExpression & where;
    std::vector<std::shared_ptr<SqlExpression> > calc;
    const OrderByExpression & orderBy;
    std::shared_ptr<SqlExpressionDatasetScope> context;

    /** Note on the ordering of rows
     *  Users are expecting determinist results (e.g. repeated queries
     *  should return rows in the same order).  When creating this object
     *  one can decide if the rows will be unordered (faster) or ordered (slower)
     *  by setting the implicitOrderByRowHash parameter.  Note that this
     *  field only control part of the logic since a user might have passed
     *  an orderBy clause.  The orderBy clause is applied before any rowHash ordering.
     **/
    BoundSelectQuery(const SelectExpression & select,
                     const Dataset & from,
                     const Utf8String& alias,
                     const WhenExpression & when,
                     const SqlExpression & where,
                     const OrderByExpression & orderBy,
                     std::vector<std::shared_ptr<SqlExpression> > calc,
                     int numBuckets = -1);

    void execute(RowProcessorEx processor,
                 ssize_t offset,
                 ssize_t limit,
                 std::function<bool (const Json::Value &)> onProgress);

    void execute(std::function<bool (NamedRowValue & output,
                                     std::vector<ExpressionValue> & calcd, int rowNum)> processor,
                 bool processInParallel,
                 ssize_t offset,
                 ssize_t limit,
                 std::function<bool (const Json::Value &)> onProgress);

    std::shared_ptr<Executor> executor;

    std::shared_ptr<ExpressionValueInfo> getSelectOutputInfo() const;
};


/*****************************************************************************/
/* BOUND GROUP BY QUERY                                                      */
/*****************************************************************************/
struct BoundGroupByQuery {

   BoundGroupByQuery(const SelectExpression & select,
                     const Dataset & from,
                     const Utf8String& alias,
                     const WhenExpression & when,
                     const SqlExpression & where,
                     const TupleExpression & groupBy,
                     const std::vector< std::shared_ptr<SqlExpression> >& aggregatorsExpr,
                     const SqlExpression & having,
                     const SqlExpression & rowName,
                     const OrderByExpression & orderBy);

    void execute(RowProcessor processor,
            ssize_t offset, ssize_t limit,
            std::function<bool (const Json::Value &)> onProgress);

    const Dataset & from;
    WhenExpression when;
    const SqlExpression & where;
    std::shared_ptr<SqlExpressionDatasetScope> rowContext;
    std::shared_ptr<GroupContext> groupContext;
    TupleExpression groupBy;

    std::vector<std::shared_ptr<SqlExpression> > calc;

    // Bind in the order by expression
    BoundOrderByExpression boundOrderBy;

    // Bind the row name expression
    BoundSqlExpression boundRowName;

    // Select Expression to resolve
    const SelectExpression& select;

    // Having Expression to resolve
    const SqlExpression& having;

    // groupby Expression to resolve
    const OrderByExpression & orderBy;

    SelectExpression subSelectExpr;

    OrderByExpression subOrderBy;

    /// The group by query runs on top of a select query
    std::shared_ptr<BoundSelectQuery> subSelect;

    size_t numBuckets;

};

} // namespace MLDB
} // namespace Datacratic
