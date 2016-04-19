/** sql_expression.h                                               -*- C++ -*-
    Jeremy Barnes, 24 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Base SQL expression support.
*/

#pragma once

#include "dataset_types.h"
#include "expression_value.h"
#include "mldb/types/string.h"
#include "mldb/types/any.h"
#include "mldb/types/value_description_fwd.h"
#include <memory>
#include <set>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.  Only
// value_expression_fwd.h is OK.


namespace ML {
struct Parse_Context;
} // namespace ML

namespace Datacratic {
namespace MLDB {

/** SQL expressions
    
    This implements an SQL expression, including the ability to parse it,
    bind it into a given dataset, and execute it.

    The broad phases are:

    1.  The expression itself is parsed.  That produces a SqlExpression
        pointer, which is an abstract syntax tree of the expression.
    2.  The expression is bound into the context in which it operates.
        This will often be a table, but does not need to be (eg, for when
        expressions run over rows).  That is done by applying a
        SqlBindingScope, which tells the function how to
        do things like read and write variables or call functions.  It
        also allows the ranges and types of the arguments to be known, which
        aids in the generation of more specialized (faster) code.  For example,
        if a column is always a floating point number, then we don't need to
        make it operate on strings.
    3.  Finally, a std::function is produced that takes a row, and will
        return the result as a ExpressionValue.

    Future support has been considered for:

    a) Support for parameters that can allow the names of variables and
       constants to look up an execution-time parameter.  For example,

       SELECT * FROM table WHERE uid=%{uid};

    b) Support for more information about what kind of values a column can
       have.  For example, percentage of times it's true versus false.  Many
       datasets have access to this information, and it can be used to
       optimize and specialize the execution.
    c) Support for a compilation phase with LLVM.
    d) Support to automatically populate a caller-driven structure from the
       values.  For example, pass in a distribution<float> and have it filled
       out in-place with conversions made on the other end.
*/

struct SqlExpression;
struct KnownColumn;
struct SqlRowScope;
struct SqlRowExpression;
struct OrderByExpression;
struct TupleExpression;
struct GenerateRowsWhereFunction;
struct SelectExpression;
struct SqlBindingScope;
struct MldbServer;
struct BasicRowGenerator;
struct WhenExpression;
struct SqlExpressionDatasetScope;
struct TableOperations;
struct RowStream;

extern const OrderByExpression ORDER_BY_NOTHING;


enum OrderByDirection {
    DESC,
    ASC
};

DECLARE_ENUM_DESCRIPTION(OrderByDirection);

/*****************************************************************************/
/* BOUND PARAMETERS                                                          */
/*****************************************************************************/

/** This is how we access the name of a bound parameter, as we are
    executing a query but before we get to having actual rows.
*/

typedef std::function<ExpressionValue (const Utf8String & paramName)> BoundParameters;


/*****************************************************************************/
/* BOUND EXPRESSION METADATA                                                 */
/*****************************************************************************/

/** Metadata about a bound expression.  Mostly used for static analysis. */

struct BoundExpressionMetadata {

    BoundExpressionMetadata()
        : isConstant(false)
    {
    }

    BoundExpressionMetadata(bool isConstant)
        : isConstant(isConstant)
    {
    }

    /// Is this expression constant, in other words it always returns the
    /// same value that is independent of input?  If this is true, the
    /// exec function must not access or use its SqlRowScope
    /// argument at all.
    bool isConstant;
};


/*****************************************************************************/
/* BOUND ROW EXPRESSION                                                      */
/*****************************************************************************/

/** Represents the output of the binding process for row expressions.  This
    takes in a context and returns a cell value.
*/
struct BoundSqlExpression {

    /** Function type to execute the expression, returning a cell value.  All
        bound row expressions must provide an exec function.

        The storage parameter is provided to allow for a reference to be returned
        by the function.  It can return either
        - A reference to something that has a lifetime at least as long as the
          context, or
        - A reference to storage, which has been modified to contain the return
          value.

        This allows for values to be returned as references without copying.
    */
    typedef std::function<const ExpressionValue & (const SqlRowScope & context,
                                                   ExpressionValue & storage,
                                                   const VariableFilter & filter)> ExecFunction;

    BoundSqlExpression()
    {
    }

    BoundSqlExpression(ExecFunction exec,
                       const SqlExpression * expr,
                       std::shared_ptr<ExpressionValueInfo> info,
                       BoundExpressionMetadata metadata = BoundExpressionMetadata());
    
    operator bool () const { return !!exec; };

    ExecFunction exec;
    std::shared_ptr<const SqlExpression> expr;

    /// What kind of value does this return?
    std::shared_ptr<ExpressionValueInfo> info;

    /// Metadata for the expression
    BoundExpressionMetadata metadata;

    /** Attempt to extract the value of this expression as a constant.  Only
        really makes sense when metadata.isConstant is true.
    */
    ExpressionValue constantValue() const;

    const ExpressionValue &
    operator () (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter /*= GET_ALL*/) const
    {
        return exec(context, storage, filter);
    }

    ExpressionValue
    operator () (const SqlRowScope & context,
                 const VariableFilter & filter /*= GET_ALL*/) const
    {
        ExpressionValue storage;
        const ExpressionValue & res = exec(context, storage, filter);
        if (&res == &storage)
            return std::move(storage);
        return res;
    }

};

DECLARE_STRUCTURE_DESCRIPTION(BoundSqlExpression);

/*****************************************************************************/
/* TABLE OPERATIONS                                                          */
/*****************************************************************************/

/** Represents a generalized table, with enough information for MLDB to
    work with it.
*/

struct TableOperations {

    /// Get a description of a row of the table, including all known columns
    std::function<std::shared_ptr<RowValueInfo> ()> getRowInfo;

    /// Get a function bound to the given dataset
    std::function<BoundFunction (SqlBindingScope & scope,
                                 const Utf8String & tableName,
                                 const Utf8String & functionName,
                                 const std::vector<std::shared_ptr<ExpressionValueInfo> > & args)>
    getFunction;

    /// Run a basic query on the table
    std::function<BasicRowGenerator (const SqlBindingScope & context,
                                     const SelectExpression & select,
                                     const WhenExpression & when,
                                     const SqlExpression & where,
                                     const OrderByExpression & orderBy,
                                     ssize_t offset,
                                     ssize_t limit,
                                     bool allowParallel)>
    runQuery;

    bool operator ! () const {return !getRowInfo && !getFunction  && !runQuery; }
};

/*****************************************************************************/
/* BOUND TABLE EXPRESSION                                                    */
/*****************************************************************************/

/** A table expression bound to a context.  This indicates a concrete
    dataset on which an operation can be performed.
*/

struct BoundTableExpression {
    std::shared_ptr<Dataset> dataset;  // deprecated -- use table ops instead
    TableOperations table;
    Utf8String asName;

    bool operator ! () const {return !dataset && !table;}
};



/*****************************************************************************/
/* VARIABLE GETTER                                                           */
/*****************************************************************************/

/** Object returned when we bind a get variable expression. */

struct ColumnGetter {
    typedef std::function<const ExpressionValue & (const SqlRowScope & context,
                                                   ExpressionValue & storage,
                                                   const VariableFilter & filter) > Exec;
    
    ColumnGetter()
    {
    }
    
    ColumnGetter(Exec exec, std::shared_ptr<ExpressionValueInfo> info)
        : exec(exec), info(info)
    {
    }
    
    /// Function called to retrieve the value of the variable
    Exec exec;
    
    /// Function that describes the characteristics of the return type
    std::shared_ptr<ExpressionValueInfo> info;
    
    /// Make it feel like it's just a callable function
    const ExpressionValue & operator () (const SqlRowScope & context,
                                         ExpressionValue & storage,
                                         const VariableFilter & filter = GET_LATEST) const
    {
        return exec(context, storage, filter);
    }
};


/*****************************************************************************/
/* BOUND FUNCTION                                                            */
/*****************************************************************************/

/** Result of binding a function.  This provides an executor as well as
    information on the range of the function.
*/

struct BoundFunction {
    typedef std::function<ExpressionValue (const std::vector<ExpressionValue> &,
                          const SqlRowScope & context) > Exec;

    BoundFunction()
        : filter(GET_LATEST)
    {
    }

    BoundFunction(Exec exec,
                  std::shared_ptr<ExpressionValueInfo> resultInfo)
        : exec(std::move(exec)),
          resultInfo(std::move(resultInfo)),
          filter(GET_LATEST)
    {
    }

    BoundFunction(Exec exec,
                  std::shared_ptr<ExpressionValueInfo> resultInfo,
                  VariableFilter filter)
        : exec(std::move(exec)),
          resultInfo(std::move(resultInfo)),
          filter(filter)
    {
    }

    operator bool () const { return !!exec; }

    Exec exec;
    std::shared_ptr<ExpressionValueInfo> resultInfo;
    VariableFilter filter; // allows function to filter variable as they need

    ExpressionValue operator () (const std::vector<ExpressionValue> & args,
                                 const SqlRowScope & context) const
    {
        return exec(args, context);
    }
};


/*****************************************************************************/
/* EXTERNAL FUNCTION                                                         */
/*****************************************************************************/

/** Type of an external function factory.  This should return the bound
    version of the function.
*/
typedef std::function<BoundFunction(const Utf8String &,
                                    const std::vector<BoundSqlExpression> & args,
                                    SqlBindingScope & context)>
    ExternalFunction;

/** Register a new function into the SQL system under the given name.  The
    function will remain available until the returned value is destroyed,
    at which point it will be deregistered.
*/
std::shared_ptr<void> registerFunction(Utf8String name, ExternalFunction function);

/** Look up the given function.  Throws if not found. */
ExternalFunction lookupFunction(const Utf8String & name);

/** Look up the given function.  Returns a null pointer if not found. */
ExternalFunction tryLookupFunction(const Utf8String & name);

/** Structure that does the same for use in initialization. */
struct RegisterFunction {

    RegisterFunction(Utf8String name, ExternalFunction function)
    {
        handle = registerFunction(std::move(name), std::move(function));
    }

    std::shared_ptr<void> handle;
};

/*****************************************************************************/
/* BOUND AGGREGATOR                                                          */
/*****************************************************************************/

/** Structure used to allow evaluation of aggregators. */

struct BoundAggregator {
    /// Called to initialize the aggregator.  It returns an object that is
    /// used to hold its state.
    std::function<std::shared_ptr<void> ()> init;

    /// Called to add a new value to the aggregator.  It should update the
    /// state based upon the new value.  It will only be called from one
    /// thread; so thread safety is not required
    std::function<void (const ExpressionValue * args,
                        size_t nargs,
                        void * data)> process;

    /// Called once the aggregator is finished.  It should calculate the
    /// result of the expression.
    std::function<ExpressionValue (void *)> extract;

    //Merge the state data on the right into the data on the left
    //State data is as allocated by Init 
    std::function< void (void*, void*) > mergeInto;

    /// The type of the result of the function
    std::shared_ptr<ExpressionValueInfo> resultInfo;

    operator bool () const { return !!init && !!process && !!extract && !!mergeInto; }
};


/*****************************************************************************/
/* EXTERNAL AGGREGATOR                                                       */
/*****************************************************************************/

/** Type of an external aggregator factory.  This should return the bound
    version of the aggregator.
*/
typedef std::function<BoundAggregator(const Utf8String &,
                                      const std::vector<BoundSqlExpression> & args,
                                      SqlBindingScope & context)>
ExternalAggregator;

/** Register a new aggregator into the SQL system under the given name.  The
    aggregator will remain available until the returned value is destroyed,
    at which point it will be deregistered.
*/
std::shared_ptr<void> registerAggregator(Utf8String name, ExternalAggregator aggregator);

/** Look up the given aggregator.  Throws if not found. */
ExternalAggregator lookupAggregator(const Utf8String & name);

/** Look up the given aggregator.  Returns a null pointer if not found. */
ExternalAggregator tryLookupAggregator(const Utf8String & name);

/** Structure that does the same for use in initialization. */
struct RegisterAggregator {

    RegisterAggregator(Utf8String name, ExternalAggregator aggregator)
    {
        handle = registerAggregator(std::move(name), std::move(aggregator));
    }

    std::shared_ptr<void> handle;
};

/*****************************************************************************/
/* EXTERNAL FUNCTION                                                         */
/*****************************************************************************/

/** Type of an external dataset function factory.  This should return the bound
    version of the function.
*/
typedef std::function<BoundTableExpression(const Utf8String & str,
                                    const std::vector<BoundTableExpression> & args,
                                    const ExpressionValue & options,
                                    const SqlBindingScope & context,
                                    const Utf8String& alias)>
    ExternalDatasetFunction;

std::shared_ptr<void> registerDatasetFunction(Utf8String name, ExternalDatasetFunction function);

/*****************************************************************************/
/* GET ALL COLUMNS OUTPUT                                                    */
/*****************************************************************************/

struct GetAllColumnsOutput {

    /// Function that will return a row with the given columns
    std::function<ExpressionValue (const SqlRowScope &)> exec;

    /// Row information about the value returned from calling getColumns
    std::shared_ptr<RowValueInfo> info;
};


/*****************************************************************************/
/* COLUMN FUNCTION                                                           */
/*****************************************************************************/

/** Function which operates in a column context.  It takes a column name and
    a set of parameters, and returns the value of the function.
*/
typedef std::function<ExpressionValue (const ColumnName & columnName,
                                       const std::vector<ExpressionValue> & args)>
ColumnFunction;


/*****************************************************************************/
/* ROW EXPRESSION BINDING CONTEXT                                            */
/*****************************************************************************/

/** Context in which a row expression is bound.  At this point, the dataset
    on which the expression is being applied is known, which allows
    specialization based upon known characteristics of the data.
*/

struct SqlBindingScope {

    SqlBindingScope();

    virtual ~SqlBindingScope();

    /** Return a bound function.  This returns a BoundFunction object, which
        will apply the given function (optionally in the scope of the given
        table) to the passed arguments when called.

        The tableName parameter is optional (empty if unused), and gives the
        name of the table that the function was found in; for example
        t1.rowName() will have "t1" in tableName.

        The functionName parameter gives the name of the function to be
        found and bound.

        The args array gives a list of argument expressions which will be
        evaluated when passed to the bound function.  These can be used for
        static analysis of the input and output of the function.  They
        must be bound in the argScope, not the current scope, as otherwise
        references from inner scopes may not be resolveable.

        The argScope parameter is the scope in which the arguments are
        bound.  This is not necessarily the same as the scope in which
        the function is discovered: whenever the function is found in an
        outer scope but the arguments evaluated in an inner scope, then
        argScope will not be the same as *this.
    */
    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);
    

    virtual BoundTableExpression
    doGetDatasetFunction(const Utf8String & functionName,
                         const std::vector<BoundTableExpression> & args,
                         const ExpressionValue & options,
                         const Utf8String & alias);
    
    virtual BoundAggregator
    doGetAggregator(const Utf8String & functionName,
                    const std::vector<BoundSqlExpression> & args);
    
    // Used to get the value of a column
    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnName & columnName);

    // Used to list all columns in a dataset.  It returns a function that can
    // be used to return a row containing just those columns.
    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep);

    // Function used to create a generator for an expression
    virtual GenerateRowsWhereFunction
    doCreateRowsWhereGenerator(const SqlExpression & where,
                               ssize_t offset,
                               ssize_t limit);
    
    /** Used to obtain functions that operate on a given column, within an
        expression designed to select columns programatically.
    */
    virtual ColumnFunction
    doGetColumnFunction(const Utf8String & functionName);

    /** Used to obtain the value of a bound parameter. */
    virtual ColumnGetter
    doGetBoundParameter(const Utf8String & paramName);

    /** Used to obtain a dataset from a dataset name. */
    virtual std::shared_ptr<Dataset>
    doGetDataset(const Utf8String & datasetName);

    /** Used to obtain a dataset from a dataset config. */
    virtual std::shared_ptr<Dataset>
    doGetDatasetFromConfig(const Any & datasetConfig);

    /** Used to obtain a table from a table name. */
    virtual TableOperations
    doGetTable(const Utf8String & tableName);

    /** Used to resolve the table name from a full identifier.
        This will split a variable identifier, with multiple dots,
        into a table name and a variable name, in the context of
        the current scope.

        Returns the table name in tableName and the variable part in
        the return value.  The middle dot should be removed.

        This is primarily used for datasets that implement joins,
        where they may need to be able to resolve their variables to
        several underlying tables.
    */
    virtual ColumnName
    doResolveTableName(const ColumnName & fullVariableName,
                       Utf8String & tableName) const;

    /** Return the MLDB server behind this context.  Default returns a null
        pointer which means we're running outside of MLDB.
    */
    virtual MldbServer * getMldbServer() const;

    size_t functionStackDepth;
};


/*****************************************************************************/
/* TRANSFORM ARGS                                                            */
/*****************************************************************************/

/** Create a copy, applying the given transformation to each of the child
    expressions.
*/
typedef std::function<std::vector<std::shared_ptr<SqlExpression> > (const std::vector<std::shared_ptr<SqlExpression> > & args)> TransformArgs;


/*****************************************************************************/
/* SCOPED NAME                                                               */
/*****************************************************************************/

/** This represents a variable name with an optional scope, for example
    x.y.  The scope (x) is optional and normally represents the table
    name in which we're looking for the variable.
*/

struct ScopedName {
    ScopedName(Utf8String scope = Utf8String(),
               ColumnName name = ColumnName()) noexcept
        : scope(std::move(scope)),
          name (std::move(name))
    {
    }

    Utf8String scope;
    ColumnName name;

    bool operator == (const ScopedName & other) const;
    bool operator != (const ScopedName & other) const;
    bool operator < (const ScopedName & other) const;
};

DECLARE_STRUCTURE_DESCRIPTION(ScopedName);


/*****************************************************************************/
/* UNBOUND ENTITIES                                                          */
/*****************************************************************************/

struct UnboundVariable {
    std::shared_ptr<ExpressionValueInfo> info;
    void merge(UnboundVariable var);
};

DECLARE_STRUCTURE_DESCRIPTION(UnboundVariable);

struct UnboundWildcard {
    ColumnName prefix;
    void merge(UnboundWildcard wildcard);
};

DECLARE_STRUCTURE_DESCRIPTION(UnboundWildcard);

struct UnboundFunction {
    /// Arguments for each arity of the function
    std::map<int, std::vector<std::shared_ptr<ExpressionValueInfo> > > argsForArity;
    void merge(UnboundFunction fn);
};

DECLARE_STRUCTURE_DESCRIPTION(UnboundFunction);

struct UnboundTable {
    std::map<Coords, UnboundVariable> vars;
    std::map<Coords, UnboundWildcard> wildcards;
    std::map<Coords, UnboundFunction> funcs;
    void merge(UnboundTable table);
};

DECLARE_STRUCTURE_DESCRIPTION(UnboundTable);

/** A list of unbound entities from an expression.  This is used to determine
    whether or not an expression needs to be further bound.
*/

struct UnboundEntities {

    /// List of tables that are unbound.  Variables with a table name set go here.
    std::map<Utf8String, UnboundTable> tables;

    /// List of variables that are unbound.  Only those with no table name are
    /// included here.
    std::map<ColumnName, UnboundVariable> vars;

    /// List of wildcards which are unbound.  Only those with no table name are
    /// included here.
    std::map<ColumnName, UnboundWildcard> wildcards;

    /// List of functions that are unbound.  Only those with no table name are
    /// included here.
    std::map<Utf8String, UnboundFunction> funcs;

    /// List of query parameters that need to exist.
    std::map<Utf8String, UnboundVariable> params;

    /// Merge the unknown entities from other
    void merge(UnboundEntities other);

    /// Merge the unknown entities from other, except that the tables
    /// given will not be merged.
    void mergeFiltered(UnboundEntities other,
                       const std::set<Utf8String> & knownTables);

    /// Is there any variable unbound, that will require a row context for
    /// this expression to run inside of?  It looks inside vars and tables
    /// to work it out.
    bool hasUnboundVariables() const;
};

DECLARE_STRUCTURE_DESCRIPTION(UnboundEntities);


/*****************************************************************************/
/* SQL ROW SCOPE                                                             */
/*****************************************************************************/

/** Context in which a row expression is executed.  This is to allow access
    to the columns in the row, etc.

    This is an empty class; any implementation must derive from it.
*/

struct SqlRowScope {
    virtual ~SqlRowScope()
    {
    }

    /** In some circumstances, such as calling functions, we want to signal
        that there is no row available even though the functions require
        one to be passed.

        To do this, use an SqlRowScope object directly.  The code can detect
        whether it has a row or not by calling this hasRow() function.
    */
    bool hasRow() const
    {
        return typeid(*this) != typeid(SqlRowScope);
    }

    /** Throw an exception saying that the types requested were wrong. */
    static void throwBadNestingError(const std::type_info & typeRequested,
                                     const std::type_info & typeFound)
        __attribute__((noreturn));

    /** Assert that the type of this object is the one given, and return it
        as that type.
    */
    template<typename T>
    T & as()
    {
        if (typeid(*this) == typeid(T))
            return static_cast<T &>(*this);

        auto * cast = dynamic_cast<T *>(this);
        if (cast)
            return *cast;
        throwBadNestingError(typeid(T), typeid(*this));
    }


    /** Assert that the type of this object is the one given, and return it
        as that type.
    */
    template<typename T>
    const T & as() const
    {
        if (typeid(*this) == typeid(T))
            return static_cast<const T &>(*this);

        auto * cast = dynamic_cast<const T *>(this);
        if (cast)
            return *cast;
        throwBadNestingError(typeid(T), typeid(*this));
    }
};


/*****************************************************************************/
/* SQL EXPRESSION                                                            */
/*****************************************************************************/

/** This is the basic SQL expression.  It encapsulates running an expression
    and returning a result.

    The basic setup is
    - An unbound SQL expression is a function of tables, variables, query
      parameters and functions, and the current row.  In other words,
      unboundExpr: f(tables, functions, variables, params, row) -> ExpressionValue
    - It then gets bound, which resolves the references to tables and functions.
      The variables and query parameters remain unresolved, although their
      types are known as we know what table they come from.  In other words:
      boundExpr: f(variables, params, row) -> ExpressionValue
    - It can then be executed by passing it a row scope, which encapsulates
      the variables, parameters and current row.
*/

struct SqlExpression: public std::enable_shared_from_this<SqlExpression> {
    virtual ~SqlExpression();

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const = 0;

    virtual Utf8String print() const = 0;

    /// SQL expression that always returns TRUE; used for intialization
    static const std::shared_ptr<SqlExpression> TRUE;

    /// SQL expression that always returns 1.0; used for intialization
    static const std::shared_ptr<SqlExpression> ONE;

    /** Parse an expression.  It should eventually be equivalent to an SQL
        "WHERE" clause (ie, expression):

        http://www.sqlite.org/syntax/expr.html
    */
    static std::shared_ptr<SqlExpression>
    parse(ML::Parse_Context & context, int precendence, bool allowUtf8);

    static std::shared_ptr<SqlExpression>
    parse(const std::string & expression, const std::string & filename = "",
          int row = 1, int col = 1);

    static std::shared_ptr<SqlExpression>
    parse(const Utf8String & expression, const std::string & filename = "",
          int row = 1, int col = 1);

    static std::shared_ptr<SqlExpression>
    parse(const char * expression, const std::string & filename = "",
          int row = 1, int col = 1);
    

    /** Parse the expression, but if an empty string is passed, return a
        constant that evaluates to the given expression.
    */
    static std::shared_ptr<SqlExpression>
    parseDefault(ExpressionValue def,
                 const std::string & expression,
                 const std::string & filename = "UNKNOWN",
                 int row = 1, int col = 1);

    /** Parse the expression, but if an empty string is passed, return a
        constant that evaluates to the given expression.
    */
    static std::shared_ptr<SqlExpression>
    parseDefault(ExpressionValue def,
                 const Utf8String & expression,
                 const std::string & filename = "UNKNOWN",
                 int row = 1, int col = 1);
    
    /** Function to create a copy of a potentially transformed version of the
        given function.
    */
    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const = 0;


    typedef std::function<bool (const SqlExpression & expr,
                                const std::string & type,
                                const Utf8String & arg,
                                const std::vector<std::shared_ptr<SqlExpression> > & children)>
        TraverseFunction;

    /** Function to traverse the tree, applying the given function at each
        node.
    */
    virtual void traverse(const TraverseFunction & visitor) const;

    /** Return the type of this expression. */
    virtual std::string getType() const = 0;

    /** Return the argument or operation of this expression; meaning varies by type */
    virtual Utf8String getOperation() const = 0;

    /** Return all children of this expression. */
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const = 0;

    /** Return all variables that are referenced from within this expression. */
    virtual std::map<ScopedName, UnboundVariable>
    variableNames() const;
    
    /** Return all wildcards that are used by this expression to match multiple
        variables.
    */
    virtual std::map<ScopedName, UnboundWildcard>
    wildcards() const;

    /** Return all functions that are called from within this expression. */
    virtual std::map<ScopedName, UnboundFunction>
    functionNames() const;

    /** Return all functions that are referenced from within this expression. */
    virtual std::map<Utf8String, UnboundVariable>
    parameterNames() const;

    /** Get a list of everything that is unbound (needs to exist in the
        external context in order for bind() to succeed).

        This is used, amongst other things, to decide at which point
        tables can be instantiated and to detect correlated subqueries and
        dependencies on query parameters.

        Default implementation calls variableNames() and parameterNames()
        to do its work.
    */
    virtual UnboundEntities getUnbound() const;

    /** Helpful shallow copy function that calls transform() */
    std::shared_ptr<SqlExpression> shallowCopy() const;

    /** Helpful deep copy function that calls transform() */
    std::shared_ptr<SqlExpression> deepCopy() const;

    /** Return a substituted version of the expression where any references to
        variables created by the select statement are replaced by the expressions
        that they are calculated from.
    */
    std::shared_ptr<SqlExpression>
    substitute(const SelectExpression & toSubstitute) const;

    /** Is this expression constant?   Default is false. */
    virtual bool isConstant() const;

    /** For expressions that are constant, return the result of the expression.
        This will be done by evaluation within a context that only has
        builtin functions available.  If there is something that depends
        upon something outside the context, it will be false.

        Note that the isConstant() function returning true guarantees that
        this call will succeed, but if isConstant() returns false it is
        possible that the call succeeds anyway, due to SQL mandating lazy
        evaluation.  Similarly for getUnbound().

        Expression types that know how to rapidly evaluate a constant
        can override to make this more efficient, eg to do so without
        binding.
    */
    virtual ExpressionValue constantValue() const;

    /** Is this a constant expression that always returns true/false in a
        boolean context?
    */
    virtual bool isConstantTrue() const;
    virtual bool isConstantFalse() const;

    /** Evaluates to true if this expresion selects the entire row passed in,
        ie if it's a SELECT * or a {*}

        Default implementation returns false; the subclasses which could be
        a SELECT * should override.
    */
    virtual bool isIdentitySelect(SqlExpressionDatasetScope & context) const;

    /** Find any children that is an aggregator call 
        This function perform partial validation of the parse tree for 
        GROUP BY validatity.  
        Caller must pass true if there is a GROUP BY clause associated with
        this expression.
    */
    std::vector<std::shared_ptr<SqlExpression> > findAggregators(bool withGroupBy) const;

    //should be private:
    typedef std::shared_ptr<SqlExpression> (*OperatorHandler)
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs,
     const std::string & op);
    
    // Handle a bitwise operator
    static std::shared_ptr<SqlExpression> bwise
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs,
     const std::string & op);

    // Handle an arithmetic operator
    static std::shared_ptr<SqlExpression> arith
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs,
     const std::string & op);

    // Handle a comparison operator
    static std::shared_ptr<SqlExpression> compar
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs,
     const std::string & op);

    // Handle a boolean operator
    static std::shared_ptr<SqlExpression> booln
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs, const std::string & op);

    // Handle infix operator as function invocation
    static std::shared_ptr<SqlExpression> func
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs, const std::string & op);

    // Handle an unimplemented operator
    static std::shared_ptr<SqlExpression> unimp
    (std::shared_ptr<SqlExpression> lhs,
     std::shared_ptr<SqlExpression> rhs,
     const std::string & op);

    struct Operator {
        const char * token;
        bool unary;
        OperatorHandler handler;
        int precedence;
        const char * desc;
    };

public:
    /// This is the text that was originally parsed to create the
    /// expression.
    Utf8String surface;
};

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<SqlExpression>);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<const SqlExpression>);



/*****************************************************************************/
/* SQL ROW EXPRESSION                                                        */
/*****************************************************************************/

/** This defines the bit between a "SELECT" and the "FROM" clause of an SQL
    statement, ie the selection or calculation of the results.
*/

struct SqlRowExpression: public SqlExpression {

    virtual ~SqlRowExpression();

    /** Parses a single result variable expression. */
    static std::shared_ptr<SqlRowExpression>
    parse(ML::Parse_Context & context, bool allowUtf8);

    static std::shared_ptr<SqlRowExpression>
    parse(const std::string & expr,
          const std::string & filename = "", int row = 1, int col = 1);

    static std::shared_ptr<SqlRowExpression>
    parse(const Utf8String & expr,
          const std::string & filename = "", int row = 1, int col = 1);

    static std::shared_ptr<SqlRowExpression>
    parse(const char * expr,
          const std::string & filename = "", int row = 1, int col = 1);

    /** Parses a comma separated list of result variable expressions. */
    static std::vector<std::shared_ptr<SqlRowExpression> >
    parseList(ML::Parse_Context & context, bool allowUtf8);

    static std::vector<std::shared_ptr<SqlRowExpression> >
    parseList(const std::string & expr,
              const std::string & filename = "", int row = 1, int col = 1);

    static std::vector<std::shared_ptr<SqlRowExpression> >
    parseList(const char * expr,
              const std::string & filename = "", int row = 1, int col = 1);

    static std::vector<std::shared_ptr<SqlRowExpression> >
    parseList(const Utf8String & expr,
              const std::string & filename = "", int row = 1, int col = 1);

    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const = 0;

};

PREDECLARE_VALUE_DESCRIPTION(SqlRowExpression);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<SqlRowExpression>);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<const SqlRowExpression>);



/*****************************************************************************/
/* SELECT EXPRESSION                                                         */
/*****************************************************************************/

/** Select expression.  This returns a row, with named columns assigned
    values.
*/

struct SelectExpression: public SqlRowExpression {
    SelectExpression();

    /** Construct from a string to be parsed. */
    SelectExpression(const std::string & exprToParse,
                     const std::string & filename = "",
                     int row = 1, int col = 1);

    SelectExpression(const char * exprToParse,
                     const std::string & filename = "",
                     int row = 1, int col = 1);

    SelectExpression(const Utf8String & exprToParse,
                     const std::string & filename = "",
                     int row = 1, int col = 1);

    SelectExpression(std::vector<std::shared_ptr<SqlRowExpression> > clauses);

    /// Result of parsing "*", used for default values
    static const SelectExpression STAR;
    
    static SelectExpression
    parse(ML::Parse_Context & context, bool allowUtf8);

    static SelectExpression
    parse(const std::string & expr,
          const std::string & filename = "", int row = 1, int col = 1);

    static SelectExpression
    parse(const Utf8String & expr,
          const std::string & filename = "", int row = 1, int col = 1);

    static SelectExpression
    parse(const char * expr,
          const std::string & filename = "", int row = 1, int col = 1);

    virtual BoundSqlExpression
    bind(SqlBindingScope & context) const;

    virtual Utf8String print() const;

    virtual std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    virtual std::string getType() const;
    virtual Utf8String getOperation() const;
    virtual std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    virtual bool isIdentitySelect(SqlExpressionDatasetScope & context) const;

    std::vector<std::shared_ptr<SqlRowExpression> > clauses;

    bool operator == (const SelectExpression & other) const;
    bool operator != (const SelectExpression & other) const
    {
        return ! operator == (other);
    }

};

PREDECLARE_VALUE_DESCRIPTION(SelectExpression);


/*****************************************************************************/
/* BOUND ORDER BY EXPRESSION                                                 */
/*****************************************************************************/

struct BoundOrderByClause {
    BoundSqlExpression expr;
    OrderByDirection dir;
};

DECLARE_STRUCTURE_DESCRIPTION(BoundOrderByClause);


struct BoundOrderByExpression {
    std::vector<BoundOrderByClause> clauses;

    bool empty() const
    {
        return clauses.empty();
    }

    size_t size() const
    {
        return clauses.size();
    }

    /** Apply the order by expression to a row, returning the set of
        order by fields.
    */
    std::vector<ExpressionValue> apply(const SqlRowScope & context) const;

    /** Compare according to the clauses, returning -1, 0 or 1 depending
        upon whether vec1 is less, equal or greater than vec2. */
    int compare(const std::vector<ExpressionValue> & vec1,
                const std::vector<ExpressionValue> & vec2,
                int offset = 0) const;

    /** Return if the first is less than the second. */
    bool less(const std::vector<ExpressionValue> & vec1,
              const std::vector<ExpressionValue> & vec2,
              int offset = 0) const
    {
        return compare(vec1, vec2, offset) == -1;
    }

};

DECLARE_STRUCTURE_DESCRIPTION(BoundOrderByExpression);


/*****************************************************************************/
/* ORDER BY EXPRESSION                                                       */
/*****************************************************************************/

/** Represents an order by clause:
    
    ORDER BY expression1 ASC, expression2 DESC,...

*/
struct OrderByExpression {
    OrderByExpression();
    OrderByExpression(std::vector<std::pair<std::shared_ptr<SqlExpression>, OrderByDirection> > clauses);
    OrderByExpression(TupleExpression clauses);

    std::vector<std::pair<std::shared_ptr<SqlExpression>, OrderByDirection> > clauses;

    static OrderByExpression parse(ML::Parse_Context & context, bool allowUtf8);
    static OrderByExpression parse(const std::string & expression);
    static OrderByExpression parse(const char * expression);
    static OrderByExpression parse(const Utf8String & expression);

    /// Returns value of parsing "rowHash()"; used for initialization
    static const OrderByExpression ROWHASH;
    
    bool empty() const
    {
        return clauses.empty();
    }

    size_t size() const
    {
        return clauses.size();
    }

    Utf8String surface;
    Utf8String print() const;

    OrderByExpression transform(const TransformArgs & transformArgs) const;

    /** Bind all of the sub-expressions and return a bound version. */
    BoundOrderByExpression
    bindAll(SqlBindingScope & context) const;

    /** Substitute in the expressions behind the variables from the select
        expression so that the order by expression stands on its own.
    */
    OrderByExpression substitute(const SelectExpression & select) const;

    bool operator == (const OrderByExpression & other) const;
    bool operator != (const OrderByExpression & other) const
    {
        return ! operator == (other);
    }
};

PREDECLARE_VALUE_DESCRIPTION(OrderByExpression);


/*****************************************************************************/
/* TUPLE EXPRESSION                                                          */
/*****************************************************************************/

/** An expression that returns a tuple of values, like a group by clause. */

struct TupleExpression {  // TODO: should be a row expression
    std::vector<std::shared_ptr<SqlExpression> > clauses;
    Utf8String surface;

    bool empty() const
    {
        return clauses.empty();
    }

    size_t size() const
    {
        return clauses.size();
    }

    static TupleExpression parse(ML::Parse_Context & context, bool allowUtf8);
    static TupleExpression parse(const std::string & expression);
    static TupleExpression parse(const char * expression);
    static TupleExpression parse(const Utf8String & expression);

    Utf8String print() const;

    TupleExpression transform(const TransformArgs & transformArgs) const;
    
    /** Substitute in the expressions behind the variables from the select
        expression so that the order by expression stands on its own.
    */
    TupleExpression substitute(const SelectExpression & select) const;

    /** Are all clauses constant? */
    bool isConstant() const;
};

PREDECLARE_VALUE_DESCRIPTION(TupleExpression);


/*****************************************************************************/
/* GENERATE ROWS WHERE FUNCTION                                              */
/*****************************************************************************/

/** Function to generate the values over multiple rows.  Only really applies
    to contexts in which there *are* multiple rows, such as datasets.

    The function takes a number of values to generate and a token (which can
    be empty) telling it where to start generating from.  It returns a list
    of row hashes with the values in, and a new token to be used for the
    next call.
*/
struct GenerateRowsWhereFunction {

    typedef std::function<std::pair<std::vector<RowName>, Any>
                          (ssize_t numToGenerate, Any token,
                           const BoundParameters & params)> Exec;

    GenerateRowsWhereFunction(Exec exec = nullptr,
                              Utf8String explain = "",
                              OrderByExpression orderedBy = ORDER_BY_NOTHING)
        : exec(std::move(exec)),
          explain(std::move(explain)),
          orderedBy(std::move(orderedBy))
    {
    }

    std::pair<std::vector<RowName>, Any>
    operator () (ssize_t numToGenerate, Any token,
                 const BoundParameters & params = BoundParameters()) const
    {
        return exec(numToGenerate, token, params);
    }

    Exec exec;

    // BADSMELL the rowStream and upperBound are implementation details and
    // should be hidden inside the lambda
    std::shared_ptr<RowStream> rowStream;
    int      upperBound;

    operator bool () const { return !!exec; };

    /// Explain the type of algorithm used
    Utf8String explain;

    /// How the results are ordered.  Null means not ordered
    OrderByExpression orderedBy;
};

DECLARE_STRUCTURE_DESCRIPTION(GenerateRowsWhereFunction);


/*****************************************************************************/
/* BASIC ROW GENERATOR                                                       */
/*****************************************************************************/

/** Function to generate rows as lists of expression values.
*/
struct BasicRowGenerator {

    typedef std::function<std::vector<NamedRowValue>
                          (ssize_t numToGenerate,
                           SqlRowScope & rowScope,
                           const BoundParameters & params)> Exec;

    BasicRowGenerator(Exec exec = nullptr, const std::string & explain = "")
        : exec(std::move(exec)),
          explain(explain)
    {
    }

    std::vector<NamedRowValue>
    operator () (ssize_t numToGenerate,
                 SqlRowScope & rowScope,
                 const BoundParameters & params = BoundParameters()) const
    {
        return exec(numToGenerate, rowScope, params);
    }

    Exec exec;

    operator bool () const { return !!exec; };

    /// Explain the type of algorithm used
    std::string explain;
};

DECLARE_STRUCTURE_DESCRIPTION(BasicRowGenerator);


/*****************************************************************************/
/* TABLE EXPRESSION                                                          */
/*****************************************************************************/

/** This is an expression that operates as the target of a FROM clause,
    or anywhere else that a table is required.
*/

struct TableExpression: public std::enable_shared_from_this<TableExpression> {

    virtual ~TableExpression();

    virtual BoundTableExpression
    bind(SqlBindingScope & context) const = 0;
    
    virtual Utf8String print() const = 0;

    static std::shared_ptr<TableExpression>
    parse(ML::Parse_Context & context, int precendence, bool allowUtf8);

    static std::shared_ptr<TableExpression>
    parse(const Utf8String & expression, const std::string & filename = "",
          int row = 1, int col = 1);

    /** Return the type of this expression. */
    virtual std::string getType() const = 0;

    /** Return the argument or operation of this expression; meaning varies
        by type */
    virtual Utf8String getOperation() const = 0;

    /** Return the table names known in this table expression.  This allows
        compound expressions to know where to go to get their variables.
    */
    virtual std::set<Utf8String> getTableNames() const = 0;

    /** Return the default name of this table in the table expression.  It
        is permitted to return an empty string (the default) which means
        that the table does not have a name and can't be referred to
        directly.
    */
    virtual Utf8String getAs() const;

    /** Serialize to JSON.  Default just serializes the surface... derived
        classes need to override.
    */
    virtual void printJson(JsonPrintingContext & context);

    /** Get a list of everything that is unbound (needs to exist in the
        external context in order to bind).

        This is used, amongst other things, to decide at which point
        tables can be instantiated and to detect correlated subqueries and
        dependencies on query parameters.
    */
    virtual UnboundEntities getUnbound() const = 0;

    /// This is the text that was originally parsed to create the
    /// expression.
    Utf8String surface;
};

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<TableExpression>);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<const TableExpression>);

/** Return a value description for an input dataset. */
std::shared_ptr<ValueDescriptionT<std::shared_ptr<TableExpression> > >
makeInputDatasetDescription();


/*****************************************************************************/
/* BOUND WHEN EXPRESSION                                                     */
/*****************************************************************************/

struct BoundWhenExpression {
    
    typedef std::function<void (MatrixNamedRow & row,
                                const SqlRowScope & rowScope)> FilterFunction;

    FilterFunction filterInPlace;

    /// Expression that lef to this bound expression
    const WhenExpression * expr;
};


/*****************************************************************************/
/* WHEN EXPRESSION                                                           */
/*****************************************************************************/

/** Represents a WHEN clause, which filters rows by timestamp. */

struct WhenExpression {
    WhenExpression();

    WhenExpression(std::shared_ptr<SqlExpression> when);

    /// When expression that is always true; used for default values
    static const WhenExpression TRUE;

    std::shared_ptr<SqlExpression> when;

    static WhenExpression
    parse(const std::string & str);

    static WhenExpression
    parse(const char * str);
    
    static WhenExpression
    parse(const Utf8String & str);

    static WhenExpression
    parse(ML::Parse_Context & context, bool allowUtf8);

    BoundWhenExpression
    bind(SqlBindingScope & context) const;

    Utf8String print() const;

    std::shared_ptr<SqlExpression>
    transform(const TransformArgs & transformArgs) const;

    std::string getType() const
    {
        return "when";
    }

    Utf8String getOperation() const
    {
        return Utf8String();
    }

    std::vector<std::shared_ptr<SqlExpression> > getChildren() const;

    bool operator == (const WhenExpression & other) const;

    Utf8String surface;
};

PREDECLARE_VALUE_DESCRIPTION(WhenExpression);


/******************************************************************************/
/* SELECT STATEMENT                                                           */
/******************************************************************************/

/** Statement that groups all of the elements of a select query together and
    allows parsing of everything after FROM ... SELECT
*/

struct SelectStatement
{
    SelectStatement();

    SelectExpression select;
    std::shared_ptr<TableExpression> from;
    WhenExpression when;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    TupleExpression groupBy;
    std::shared_ptr<SqlExpression> having;
    std::shared_ptr<SqlExpression> rowName;

    ssize_t offset;
    ssize_t limit;

    // Surface form of select statement (original string that was parsed)
    Utf8String surface;

    static SelectStatement parse(const std::string& body);
    static SelectStatement parse(const char * body);
    static SelectStatement parse(const Utf8String& body);
    static SelectStatement parse(ML::Parse_Context& context, bool allowUtf8);

    Utf8String print() const;
};

DECLARE_STRUCTURE_DESCRIPTION(SelectStatement);

struct InputQuery
{
    std::shared_ptr<SelectStatement> stm;
};

PREDECLARE_VALUE_DESCRIPTION(InputQuery);

} // namespace MLDB
} // namespace Datacratic
