/** execution_pipeline.h                                           -*- C++ -*-
    Jeremy Barnes, 27 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "sql_expression.h"
#include "binding_contexts.h"
#include "table_expression_operations.h"


namespace MLDB {

struct BoundTableExpression;


/*****************************************************************************/
/* PIPELINE RESULTS                                                          */
/*****************************************************************************/

struct PipelineResults: public SqlRowScope {
    PipelineResults(SqlRowScope * inner = nullptr)
        : inner(inner)
    {
    }

    std::vector<ExpressionValue> values;
    SqlRowScope * inner;
    BoundParameters getParam;
    std::vector<std::shared_ptr<PipelineResults> > group;
};

DECLARE_STRUCTURE_DESCRIPTION(PipelineResults);


/*****************************************************************************/
/* LEXICAL SCOPE                                                             */
/*****************************************************************************/

/** This structure encapsulates a table that is in scope of an expression.
    The table can be searched for variables, wildcards and functions using
    this object.
*/

struct LexicalScope {
    virtual ~LexicalScope();

    /** Return a variable accessor for the table.  fieldOffset gives the
        offset within the row scope for this table's fields.
    */
    virtual ColumnGetter
    doGetColumn(const ColumnPath & variableName, int fieldOffset) = 0;

    /** Return a wildcard accessor for the table.  fieldOffset gives the
        offset within the row scope for this table's fields.
    */
    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep,
                    int fieldOffset) = 0;

    /** Return a function accessor for the table.  fieldOffset gives the
        offset within the row scope for this table's fields.
    */
    virtual BoundFunction
    doGetFunction(const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  int fieldOffset,
                  SqlBindingScope & argsScope) = 0;
    
    /** Return the name of the table.  If it isn't addressable by name,
        then the empty string should be returned.
    */
    virtual Utf8String as() const = 0;

    /** Return all of the table names accessible within this scope. */
    virtual std::set<Utf8String> tableNames() const;

    /** Return the output that is added to the scope by this table when
        a row is brought in to scope. */
    virtual std::vector<std::shared_ptr<ExpressionValueInfo> >
    outputAdded() const = 0;
};


/*****************************************************************************/
/* GET PARAM INFO                                                            */
/*****************************************************************************/

typedef std::function<std::shared_ptr<ExpressionValueInfo> (const Utf8String & name)> GetParamInfo;


/*****************************************************************************/
/* PIPELINE EXPRESSION SCOPE                                                 */
/*****************************************************************************/

struct PipelineExpressionScope:
        public SqlBindingScope,
        public std::enable_shared_from_this<PipelineExpressionScope> {
    
    PipelineExpressionScope(std::shared_ptr<SqlBindingScope> outerScope);

    ~PipelineExpressionScope();
    
    /** Return a new scope, with the given table added to the scope. */
    std::shared_ptr<PipelineExpressionScope>
    tableScope(std::shared_ptr<LexicalScope> table);

    /** Return a new scope, with the given bound parameter getter added to
        the scope.
    */
    std::shared_ptr<PipelineExpressionScope>
    parameterScope(GetParamInfo getParamInfo,
                   std::vector<std::shared_ptr<ExpressionValueInfo> > outputAdded) const;

    /** Return a new scope, with the extra variables selected. */
    std::shared_ptr<PipelineExpressionScope>
    selectScope(std::vector<std::shared_ptr<ExpressionValueInfo> > outputAdded) const;

    /** Return an extractor function that will retrieve the given variable
        from the function input or output.
    */
    virtual ColumnGetter
    doGetColumn(const Utf8String & tableName, const ColumnPath & columnName);

    virtual GetAllColumnsOutput 
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep);

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    virtual ColumnFunction
    doGetColumnFunction(const Utf8String & functionName);

    virtual ColumnGetter doGetBoundParameter(const Utf8String & paramName);

    virtual ColumnPath
    doResolveTableName(const ColumnPath & fullColumnName,
                       Utf8String &tableName) const;

    std::vector<Utf8String> getTableNames() const;

    bool inLexicalScope() const
    {
        return defaultTables.size() > 0;
    }

    size_t numOutputFields() const { return outputInfo_.size(); }

    std::shared_ptr<LexicalScope> defaultScope() const
    {
        return defaultTables.back().scope;
    }

    const std::vector<std::shared_ptr<ExpressionValueInfo> > &
    outputInfo() const
    {
        return outputInfo_;
    }

    virtual MldbServer * getMldbServer() const;

    virtual std::shared_ptr<Dataset> doGetDataset(const Utf8String & datasetName);
    virtual std::shared_ptr<Dataset> doGetDatasetFromConfig(const Any & datasetConfig);

private:
    /// Entries for a table.
    struct TableEntry {
        TableEntry(std::shared_ptr<LexicalScope> scope = nullptr,
                   int fieldOffset = -1);

        std::shared_ptr<LexicalScope> scope;  ///< Scope for the table
        int fieldOffset;                    ///< Offset for fields of table

        ColumnGetter
        doGetColumn(const ColumnPath & variableName) const;

        GetAllColumnsOutput
        doGetAllColumns(const Utf8String & tableName, const ColumnFilter& keep) const;

        virtual BoundFunction
        doGetFunction(const Utf8String & functionName,
                      const std::vector<BoundSqlExpression> & args,
                      SqlBindingScope & argsScope) const;

         virtual std::set<Utf8String> tableNames() const;
    };

    /** The outer scope, with the scope for the current element. */
    std::shared_ptr<SqlBindingScope> outerScope_;
    
    /** The parent pipeline scope, if it's a pipeline. */
    std::shared_ptr<const PipelineExpressionScope> parent_;

    /// Default table into which we will look, starting at last
    std::vector<TableEntry> defaultTables;

    /// Other tables with their names
    std::map<Utf8String, TableEntry> tables;

    /// Function to obtain information about parameters
    GetParamInfo getParamInfo_;

    /// Information on the type of each field in the row output
    std::vector<std::shared_ptr<ExpressionValueInfo> > outputInfo_;
};


/*****************************************************************************/
/* ELEMENT EXECUTOR                                                          */
/*****************************************************************************/

struct ElementExecutor {

    virtual ~ElementExecutor()
    {
    }

    /** Take one element from the pipeline. */
    virtual std::shared_ptr<PipelineResults> take() = 0;

    /** Take all elements from the pipeline.  inParallel describes whether
        the function can be called from multiple threads at once.
    */
    virtual bool takeAll(std::function<bool (std::shared_ptr<PipelineResults> &)> onResult);

    /** Restart the executor from the start. */
    virtual void restart() = 0;
};


/*****************************************************************************/
/* BOUND PIPELINE ELEMENT                                                    */
/*****************************************************************************/

struct BoundPipelineElement {

    virtual ~BoundPipelineElement()
    {
    }

    /** Start running the query */
    virtual std::shared_ptr<ElementExecutor>
    start(const BoundParameters & getParam) const = 0;

    /** Return the scope that describes the output of this element. */
    virtual std::shared_ptr<PipelineExpressionScope>
    outputScope() const = 0;

    /** Return its source element in the pipeline.  Null pointer if
        there is none.
    */
    virtual std::shared_ptr<BoundPipelineElement>
    boundSource() const = 0;

    virtual int numOutputFields() const
    {
        return outputScope()->numOutputFields();
    }
};


/*****************************************************************************/
/* PIPELINE ELEMENT                                                          */
/*****************************************************************************/

struct PipelineElement: public std::enable_shared_from_this<PipelineElement> {

    virtual ~PipelineElement()
    {
    }

    /** Note that normally we have a reference to the tail of a pipeline, 
        not the head, and each knows what the earlier element in the
        pipeline is.  This is the other order from which scopes work,
        where the scope at the tail end has resolved everything, whereas
        the scope at the head has nothing resolved.
        
        Since scopes need to be created in the reverse order, it is
        normally necessary to bind the source first, and then create the
        current scope from the scope in the returned value.

        Those elements at the head always contain a reference to a root
        binding scope, such as the database itself or an empty scope
        for an isolated expression.
    */
    virtual std::shared_ptr<BoundPipelineElement> bind() const = 0;

    /** Start from an existing scope.  Everything in-scope from that scope is
        also available within this scope.
    */
    static std::shared_ptr<PipelineElement>
    root(std::shared_ptr<SqlBindingScope> scope);

    /** Start from an existing scope.  Everything in-scope from that scope is
        also available within this scope.

        The caller must guarantee that the scope outlives the returned object.
    */
    static std::shared_ptr<PipelineElement>
    root(SqlBindingScope & scope);
    
    /** Start with absolutely no scope.  The expression will be executed in
        isolation.
    */
    static std::shared_ptr<PipelineElement>
    root();

    /** Add the given bound parameters in to the scope. */
    std::shared_ptr<PipelineElement>
    params(GetParamInfo getParamInfo);

    /** Add a generic from expression to the scope.  The rest of the
        parameters can be used to push down part of the query into
        the dataset itself.
    */
    std::shared_ptr<PipelineElement>
    from(std::shared_ptr<TableExpression> from,
         WhenExpression when,
         SelectExpression select = SelectExpression::STAR,
         std::shared_ptr<SqlExpression> where = SqlExpression::TRUE,
         OrderByExpression orderBy = OrderByExpression(),
         GetParamInfo getParamInfo = nullptr);

    /** Add a from expression which has already been bound into the
        immediate outer scope.
    */
    std::shared_ptr<PipelineElement>
    from(std::shared_ptr<TableExpression> from,
         BoundTableExpression boundFrom,
         WhenExpression when,
         SelectExpression select = SelectExpression::STAR,
         std::shared_ptr<SqlExpression> where = SqlExpression::TRUE,
         OrderByExpression orderBy = OrderByExpression());

    /** Add a join to the pipeline. */
    std::shared_ptr<PipelineElement>
    join(std::shared_ptr<TableExpression> left,
         std::shared_ptr<TableExpression> right,
         std::shared_ptr<SqlExpression> on,
         JoinQualification joinQualification,
         SelectExpression select = SelectExpression(),
         std::shared_ptr<SqlExpression> where = SqlExpression::TRUE,
         OrderByExpression orderBy = OrderByExpression());
    
    /** Add a join to the pipeline that is pre-bound into the immediate outer
        scope.
    */
    std::shared_ptr<PipelineElement>
    join(std::shared_ptr<TableExpression> left,
         BoundTableExpression boundLeft,
         std::shared_ptr<TableExpression> right,
         BoundTableExpression boundRight,
         std::shared_ptr<SqlExpression> on,
         JoinQualification joinQualification,
         SelectExpression select = SelectExpression(),
         std::shared_ptr<SqlExpression> where = SqlExpression::TRUE,
         OrderByExpression orderBy = OrderByExpression());
    
    std::shared_ptr<PipelineElement>
    where(std::shared_ptr<SqlExpression> where);

    std::shared_ptr<PipelineElement>
    select(const OrderByExpression & select);

    std::shared_ptr<PipelineElement>
    sort(OrderByExpression sortBy);

    std::shared_ptr<PipelineElement>
    select(const TupleExpression & tup);

    std::shared_ptr<PipelineElement>
    aggregate(TupleExpression orderBy);

    std::shared_ptr<PipelineElement>
    partition(int numElements);
    
    std::shared_ptr<PipelineElement>
    select(SelectExpression select);

    std::shared_ptr<PipelineElement>
    select(std::shared_ptr<SqlExpression> select);

    // return a pipeline that will execute the specified statement
    std::shared_ptr<PipelineElement>
    statement(const SelectStatement& statement, GetParamInfo getParamInfo);
};

} // namespace MLDB

