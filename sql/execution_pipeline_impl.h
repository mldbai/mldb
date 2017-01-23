/** execution_pipeline_impl.h                                      -*- C++ -*-
    Jeremy Barnes, 27 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "execution_pipeline.h"
#include "join_utils.h"
#include "mldb/utils/log_fwd.h"
#include <list>


namespace MLDB {

/*****************************************************************************/
/* TABLE LEXICAL SCOPE                                                       */
/*****************************************************************************/

struct TableLexicalScope: public LexicalScope {
    /** Lexical scope for a table.
        
        The fieldOffset passed in externally gives the offset in the row
        scope for the first of the two fields that a table row will add
        (the first is the rowName, the second is the actual row itself).
    */
    TableLexicalScope(std::shared_ptr<RowValueInfo> rowInfo, Utf8String asName);

    std::shared_ptr<RowValueInfo> rowInfo;
    Utf8String asName;

    std::vector<KnownColumn> knownColumns;
    bool hasUnknownColumns;

    static constexpr int ROW_PATH = 0;
    static constexpr int ROW_CONTENTS = 1;

    virtual ColumnGetter
    doGetColumn(const ColumnPath & columnName, int fieldOffset);

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep,
                    int fieldOffset);

    virtual BoundFunction
    doGetFunction(const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  int fieldOffset,
                  SqlBindingScope & argsScope);

    virtual Utf8String as() const;

    virtual std::vector<std::shared_ptr<ExpressionValueInfo> >
    outputAdded() const;
};


/*****************************************************************************/
/* GENERATE ROWS EXECUTOR                                                    */
/*****************************************************************************/

struct GenerateRowsExecutor: public ElementExecutor {
    GenerateRowsExecutor();

    std::shared_ptr<ElementExecutor> source;

    std::shared_ptr<Dataset> dataset;
    BasicRowGenerator generator;
    BoundParameters params;

    std::vector<NamedRowValue> current;
    size_t currentDone;
    bool finished;

    bool generateMore(SqlRowScope & scope);

    virtual std::shared_ptr<PipelineResults> take();

    virtual void restart();
};


/*****************************************************************************/
/* GENERATE ROWS ELEMENT                                                     */
/*****************************************************************************/

/** Pipeline element to generate rows from an actual dataset. */
struct GenerateRowsElement: public PipelineElement {

    GenerateRowsElement(std::shared_ptr<PipelineElement> root,
                        SelectExpression select,
                        TableOperations from,
                        Utf8String as,
                        WhenExpression when,
                        std::shared_ptr<SqlExpression> where,
                        OrderByExpression orderBy);
    
    std::shared_ptr<PipelineElement> root;
    SelectExpression select;
    TableOperations from;
    Utf8String as;
    WhenExpression when;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;

    struct Bound: public BoundPipelineElement {
        std::shared_ptr<const GenerateRowsElement> parent;
        std::shared_ptr<BoundPipelineElement> source_;
        std::shared_ptr<PipelineExpressionScope> inputScope_;
        std::shared_ptr<PipelineExpressionScope> outputScope_;

        Bound(const GenerateRowsElement * parent,
              std::shared_ptr<BoundPipelineElement> source);

        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        virtual std::shared_ptr<PipelineExpressionScope>
        outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement> bind() const;
};

/*****************************************************************************/
/* SUB SELECT LEXICAL SCOPE                                                  */
/*****************************************************************************/

/** Lexical scope for a sub select.  It allows for the output of the SELECT to be
    used in wildcards (SELECT * from (SELECT 1 AS X))

    It makes a bridge from a 'select' scope to a 'table scope.'

    Output added are those of the select statement, 2 outputs if the statement has
    no 'from' clause, 4 if it has one or more in cases of nested sub selects.
*/

struct SubSelectLexicalScope: public TableLexicalScope {

    SubSelectLexicalScope(std::shared_ptr<PipelineExpressionScope> inner, std::shared_ptr<RowValueInfo> selectInfo, Utf8String asName_);

    std::shared_ptr<PipelineExpressionScope> inner;
    std::shared_ptr<ExpressionValueInfo> selectInfo;

    virtual ColumnGetter
    doGetColumn(const ColumnPath & columnName, int fieldOffset);

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep,
                    int fieldOffset);

    virtual std::set<Utf8String> tableNames() const;

    virtual std::vector<std::shared_ptr<ExpressionValueInfo> >
    outputAdded() const;

    virtual BoundFunction
    doGetFunction(const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              int fieldOffset,
              SqlBindingScope & argScope);
};

/*****************************************************************************/
/* SUB SELECT EXECUTOR                                                       */
/*****************************************************************************/

struct SubSelectExecutor: public ElementExecutor {
    SubSelectExecutor(std::shared_ptr<BoundPipelineElement> boundSelect,
                      const BoundParameters & getParam);

    std::shared_ptr<ElementExecutor> source;
    BoundParameters params;
    std::shared_ptr<ElementExecutor> pipeline;

    virtual std::shared_ptr<PipelineResults> take();

    virtual void restart();
};


/*****************************************************************************/
/* SUB SELECT ELEMENT                                                        */
/*****************************************************************************/

/**
    An element that evaluates an Select Statement as part of a From statement
*/


struct SubSelectElement: public PipelineElement {

    SubSelectElement(std::shared_ptr<PipelineElement> root,
                     SelectStatement& statement,
                     OrderByExpression& orderBy,
                     GetParamInfo getParamInfo,
                     const Utf8String& asName);

     struct Bound: public BoundPipelineElement {

        std::shared_ptr<const SubSelectElement> parent;
        std::shared_ptr<BoundPipelineElement> source_;
        std::shared_ptr<PipelineExpressionScope> inputScope_;
        std::shared_ptr<PipelineExpressionScope> outputScope_;

        Bound(const SubSelectElement * parent,
              std::shared_ptr<BoundPipelineElement> source);

        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        virtual std::shared_ptr<PipelineExpressionScope>
        outputScope() const;

        std::shared_ptr<BoundPipelineElement> boundSelect;
    };

    std::shared_ptr<BoundPipelineElement>
    bind() const;

    std::shared_ptr<PipelineElement> root;
    std::shared_ptr<PipelineElement> pipeline;
    Utf8String asName;

};

/*****************************************************************************/
/* JOIN LEXICAL SCOPE                                                        */
/*****************************************************************************/

/** Lexical scope for a join.  It allows for elements of both subtables to
    be available for wildcards.
*/

struct JoinLexicalScope: public LexicalScope {

    JoinLexicalScope(std::shared_ptr<PipelineExpressionScope> inner,
                     std::shared_ptr<LexicalScope> left,
                     std::shared_ptr<LexicalScope> right);

    std::shared_ptr<PipelineExpressionScope> inner;
    std::shared_ptr<LexicalScope> left;
    std::shared_ptr<LexicalScope> right;
    int leftOutputAdded;

    int leftFieldOffset(int fieldOffset)
    {
        return fieldOffset;
    }

    int rightFieldOffset(int fieldOffset)
    {
        return leftFieldOffset(fieldOffset) + leftOutputAdded;
    }


    virtual ColumnGetter
    doGetColumn(const ColumnPath & columnName, int fieldOffset);

    /** For a join, we can select over the columns for either one or the
        other.
    */
    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep,
                    int fieldOffset);

    virtual BoundFunction
    doGetFunction(const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  int fieldOffset,
                  SqlBindingScope & argsScope);

    /** Joins don't introduce a scope name for the join. */
    virtual Utf8String as() const;

    virtual std::set<Utf8String> tableNames() const;

    /** Joins don't add anything to the output. */
    virtual std::vector<std::shared_ptr<ExpressionValueInfo> >
    outputAdded() const;
};


/*****************************************************************************/
/* JOIN ELEMENT                                                              */
/*****************************************************************************/

/** An element that joins two tables together.  This is typically implemented
    by generating both sides sorted on the join key, and then iterating
    through matching rows.
*/

struct JoinElement: public PipelineElement {
    /** Constructor for when the element is pre-bound. */
    JoinElement(std::shared_ptr<PipelineElement> root,
                std::shared_ptr<TableExpression> left,
                BoundTableExpression boundLeft,
                std::shared_ptr<TableExpression> right,
                BoundTableExpression boundRight,
                std::shared_ptr<SqlExpression> on,
                JoinQualification joinQualification,
                SelectExpression select,
                std::shared_ptr<SqlExpression> where,
                OrderByExpression orderBy);
    
    std::shared_ptr<PipelineElement> root;
    std::shared_ptr<TableExpression> left;
    BoundTableExpression boundLeft;
    std::shared_ptr<TableExpression> right;
    BoundTableExpression boundRight;
    std::shared_ptr<SqlExpression> on;
    SelectExpression select;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    AnnotatedJoinCondition condition;
    JoinQualification joinQualification;

    std::shared_ptr<PipelineElement> leftImpl;
    std::shared_ptr<PipelineElement> rightImpl;

    struct Bound;

    /** Execution runs over all left rows for each right row.  The complexity is
        therefore O(left rows) * O(right rows).  The canonical example of this
        is `SELECT * FROM t1 JOIN t2`.
    */
    struct CrossJoinExecutor: public ElementExecutor {
        CrossJoinExecutor(const Bound * parent,
                          std::shared_ptr<ElementExecutor> root,
                          std::shared_ptr<ElementExecutor> left,
                          std::shared_ptr<ElementExecutor> right,
                          size_t leftAdded,
                          size_t rightAdded);

        const Bound * parent;
        std::shared_ptr<ElementExecutor> root, left, right;

        bool wasOutput;
        
        std::shared_ptr<PipelineResults> l,r;     

        const size_t leftAdded, rightAdded;   
            
        virtual std::shared_ptr<PipelineResults> take();

        void restart();
    };

    struct FullCrossJoinExecutor: public ElementExecutor {
        FullCrossJoinExecutor(const Bound * parent,
                          std::shared_ptr<ElementExecutor> root,
                          std::shared_ptr<ElementExecutor> left,
                          std::shared_ptr<ElementExecutor> right,
                          size_t leftAdded,
                          size_t rightAdded);

        const Bound * parent;
        std::shared_ptr<ElementExecutor> root, left, right;
        virtual std::shared_ptr<PipelineResults> take();

        std::shared_ptr<PipelineResults> r;
        typedef std::list<std::pair<std::shared_ptr<PipelineResults>, bool > > bufferType;
        bufferType bufferedLeftValues;
        bufferType::iterator l;
        bool firstSpin;
        bool rightRowWasOutputted;
       
        const size_t leftAdded, rightAdded;

        void restart();
    };

    /** Execution runs on left rows and right rows together.  This requires to
        sort the value that will be compared (ie. the pivot).  The worse case
        complexity is O(left rows) * O(right rows) when the pivot value is a
        constant but in general the complexity should be closer to
        O(max(left rows, right rows)) when the pivot do not have too many 
        duplicated values.  The canonical example of this
        is `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id`.  Here `id` is the pivot
        and rows are sorted by id.
    */
    struct EquiJoinExecutor: public ElementExecutor {
        EquiJoinExecutor(const Bound * parent,
                         std::shared_ptr<ElementExecutor> root,
                         std::shared_ptr<ElementExecutor> left,
                         std::shared_ptr<ElementExecutor> right,
                         size_t leftAdded,
                         size_t rightAdded);

        const Bound * parent;
        std::shared_ptr<ElementExecutor> root, left, right;
        
        std::shared_ptr<PipelineResults> r;
        typedef std::list<std::pair<std::shared_ptr<PipelineResults>, bool > > bufferType;
        bufferType bufferedLeftValues;
        /** Note that the left-side values are buffered so that we can
            backtrack when we need to form the cross product on matching 
            values.
        */
        bufferType::iterator l, firstDuplicate;
        /** True if we have already seen this left row, ie, if we have rewinded 
            the left side. */
        ExpressionValue lastLeftValue;

        bool wasOutput;

        std::shared_ptr<spdlog::logger> logger;

        const size_t leftAdded, rightAdded;
    
        virtual std::shared_ptr<PipelineResults> take();

        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {

        /** Bind this in.  The main difficulty is with the output scope, which
            needs to add in two different table scopes (one for left, one for
            right) for the join, as well as some extra join variables.
        */
        Bound(std::shared_ptr<BoundPipelineElement> root,
              std::shared_ptr<BoundPipelineElement> left,
              std::shared_ptr<BoundPipelineElement> right,
              AnnotatedJoinCondition condition,
              JoinQualification joinQualification);

        std::shared_ptr<BoundPipelineElement> root_;
        std::shared_ptr<BoundPipelineElement> left_;
        std::shared_ptr<BoundPipelineElement> right_;
        std::shared_ptr<PipelineExpressionScope> outputScope_;
        BoundSqlExpression crossWhere_;
        AnnotatedJoinCondition condition_;
        JoinQualification joinQualification_;

        /** Our output scope has:
            - The left and right tables
            - A default scope for the common join
        */
        std::shared_ptr<PipelineExpressionScope>
        createOutputScope();
        
        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        /** The select expression doesn't bring anything new into scope, so its
            output context is the same as its input context.
        */
        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement>
    bind() const;
};


/*****************************************************************************/
/* ROOT ELEMENT                                                              */
/*****************************************************************************/

/** A root element, that provides access to the MLDB server and the
    default things that are within scope for MLDB.
*/

struct RootElement: public PipelineElement {
    RootElement(std::shared_ptr<SqlBindingScope> outer);
    
    struct Bound;

    struct Executor: public ElementExecutor {
        virtual std::shared_ptr<PipelineResults> take();
        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {

        Bound(std::shared_ptr<SqlBindingScope> outer);
        
        std::shared_ptr<PipelineExpressionScope> scope_;

        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<SqlBindingScope> outer;

    std::shared_ptr<BoundPipelineElement>
    bind() const;
};


/*****************************************************************************/
/* FROM ELEMENT                                                              */
/*****************************************************************************/

/** Element that generates rows according to the FROM clause. */

struct FromElement: public PipelineElement {

    /** Create the from clause, which will forward a query to the executor
        of the given table.  The table may be pre-bound (if boundFrom is
        filled in), or otherwise will be bound by this element (if a default
        constructed, or empty,  BoundTableExpression is passed in to
        boundFrom).
    */
    FromElement(std::shared_ptr<PipelineElement> root_,
                std::shared_ptr<TableExpression> from_,
                BoundTableExpression boundFrom_,
                WhenExpression when_,
                SelectExpression select_ = SelectExpression::parse("*"),
                std::shared_ptr<SqlExpression> where_ = SqlExpression::TRUE,
                OrderByExpression orderBy_ = OrderByExpression(),
                GetParamInfo params_ = nullptr);
    
    std::shared_ptr<PipelineElement> root;
    std::shared_ptr<TableExpression> from;
    BoundTableExpression boundFrom;
    SelectExpression select;
    WhenExpression when;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    std::shared_ptr<PipelineElement> impl;

    std::shared_ptr<BoundPipelineElement> bind() const;
};


/*****************************************************************************/
/* FILTER WHERE ELEMENT                                                      */
/*****************************************************************************/

/** Implements a filter element, which checks the output of a previous select
    expression and filters elements where its result was false.
*/

struct FilterWhereElement: public PipelineElement {
    FilterWhereElement(std::shared_ptr<PipelineElement> source,
                       std::shared_ptr<SqlExpression> where);

    std::shared_ptr<SqlExpression> where_;
    std::shared_ptr<PipelineElement> source_;

    struct Bound;

    struct Executor: public ElementExecutor {

        const Bound * parent_;
        std::shared_ptr<ElementExecutor> source_;
        PipelineExpressionScope * context_;

        virtual std::shared_ptr<PipelineResults> take();

        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {
        
        Bound(std::shared_ptr<BoundPipelineElement> source,
              const SqlExpression & where);

        std::shared_ptr<BoundPipelineElement> source_;

        // Input and output context are identical as this is just a filter
        std::shared_ptr<PipelineExpressionScope> scope_;
        BoundSqlExpression where_;

        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement>
    bind() const;
};


/*****************************************************************************/
/* SELECT ELEMENT                                                            */
/*****************************************************************************/

/** Implements a select element, adding a row to the context with the result
    of the select expression.
*/

struct SelectElement: public PipelineElement {
    SelectElement(std::shared_ptr<PipelineElement> source,
                  SelectExpression select);

    SelectElement(std::shared_ptr<PipelineElement> source,
                  std::shared_ptr<SqlExpression> expr);

    std::shared_ptr<SqlExpression> select;
    std::shared_ptr<PipelineElement> source;

    struct Bound;

    struct Executor: public ElementExecutor {
        const Bound * parent;
        std::shared_ptr<ElementExecutor> source;

        virtual std::shared_ptr<PipelineResults> take();

        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {

        Bound(std::shared_ptr<BoundPipelineElement> source,
              const SqlExpression & select);

        std::shared_ptr<BoundPipelineElement> source_;
        BoundSqlExpression select_;
        std::shared_ptr<PipelineExpressionScope> outputScope_;
        
        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement>
    bind() const;
};


/*****************************************************************************/
/* ORDER BY ELEMENT                                                          */
/*****************************************************************************/

/** Implements an order by clause, by taking all of the elements and sorting
    them in-memory.
*/

struct OrderByElement: public PipelineElement {
    OrderByElement(std::shared_ptr<PipelineElement> source,
                   OrderByExpression orderBy);

    std::shared_ptr<PipelineElement> source;
    OrderByExpression orderBy;

    struct Bound;

    struct Executor: public ElementExecutor {
        Executor(const Bound * parent,
                 std::shared_ptr<ElementExecutor> source);

        const Bound * parent;
        std::shared_ptr<ElementExecutor> source;
        
        std::vector<std::shared_ptr<PipelineResults> > sorted;
        ssize_t numDone;

        // When we take elements, we take a group at a time
        virtual std::shared_ptr<PipelineResults> take();

        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {

        Bound(std::shared_ptr<BoundPipelineElement> source,
              const OrderByExpression & orderBy);

        std::shared_ptr<BoundPipelineElement> source_;
        std::shared_ptr<PipelineExpressionScope> scope_;
        BoundOrderByExpression orderBy_;
        
        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        /** The select expression doesn't bring anything new into scope, so its
            output context is the same as its input context.
        */
        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement>
    bind() const;
};


/*****************************************************************************/
/* AGGREGATE LEXICAL SCOPE                                                   */
/*****************************************************************************/

/** When we enter an aggregate, we gain access to a new lexical scope that
    makes available aggregate functions and some default functions for
    the subexpressions, as well as the joined variables.
*/

struct AggregateLexicalScope: public LexicalScope {

    AggregateLexicalScope(std::shared_ptr<PipelineExpressionScope> inner, int numValues);

    std::shared_ptr<PipelineExpressionScope> inner;
    int numValues_;

    virtual ColumnGetter
    doGetColumn(const ColumnPath & columnName, int fieldOffset);

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep,
                    int fieldOffset);

    virtual BoundFunction
    doGetFunction(const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  int fieldOffset,
                  SqlBindingScope & argsScope);

    /** Aggregates have no lexical scope, so don't introduce any element. */
    virtual Utf8String as() const;

    /** Aggregates don't add anything to the output. */
    virtual std::vector<std::shared_ptr<ExpressionValueInfo> >
    outputAdded() const;
};


/*****************************************************************************/
/* PARTITION ELEMENT                                                         */
/*****************************************************************************/

struct PartitionElement: public PipelineElement {
    PartitionElement(std::shared_ptr<PipelineElement> source,
                     int numValues);

    std::shared_ptr<PipelineElement> source;
    int numValues;

    struct Bound;

    struct Executor: public ElementExecutor {
        Executor(const Bound * parent,
                 std::shared_ptr<ElementExecutor> source,
                 int firstIndex,
                 int lastIndex);

        const Bound * parent;
        std::shared_ptr<ElementExecutor> source;
        
        std::shared_ptr<PipelineResults> first;
        int firstIndex, lastIndex;
            
        /// Are two of them in the same group?
        bool sameGroup(const std::vector<ExpressionValue> & group1,
                       const std::vector<ExpressionValue> & group2) const;

        // Take a group at a time
        virtual std::shared_ptr<PipelineResults> take();

        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {

        Bound(std::shared_ptr<BoundPipelineElement> source,
              int numValues);

        std::shared_ptr<BoundPipelineElement> source_;
        std::shared_ptr<PipelineExpressionScope> outputScope_;
        int numValues_;
        
        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;
        
        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        /** The select expression doesn't bring anything new into scope, so its
            output context is the same as its input context.
        */
        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement>
    bind() const;
};


/*****************************************************************************/
/* PARAMS ELEMENT                                                            */
/*****************************************************************************/

struct ParamsElement: public PipelineElement {

    ParamsElement(std::shared_ptr<PipelineElement> source,
                  GetParamInfo getParamInfo);

    std::shared_ptr<PipelineElement> source_;
    GetParamInfo getParamInfo_;

    struct Bound;

    struct Executor: public ElementExecutor {
        Executor(std::shared_ptr<ElementExecutor> source,
                 BoundParameters getParam);

        std::shared_ptr<ElementExecutor> source_;
        BoundParameters getParam_;

        virtual std::shared_ptr<PipelineResults> take();

        virtual void restart();
    };

    struct Bound: public BoundPipelineElement {

        Bound(std::shared_ptr<BoundPipelineElement> source,
              GetParamInfo getParamInfo);
        
        std::shared_ptr<BoundPipelineElement> source_;
        std::shared_ptr<PipelineExpressionScope> outputScope_;
        
        std::shared_ptr<ElementExecutor>
        start(const BoundParameters & getParam) const;

        virtual std::shared_ptr<BoundPipelineElement>
        boundSource() const;

        virtual std::shared_ptr<PipelineExpressionScope> outputScope() const;
    };

    std::shared_ptr<BoundPipelineElement> bind() const;
};

} // namespace MLDB

