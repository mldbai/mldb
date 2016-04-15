/** function.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Interface for functions into MLDB.
*/

#include "mldb/sql/dataset_types.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/http/http_exception.h"
#include "mldb/sql/coord.h"


// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


#pragma once

namespace Datacratic {

#if 0
/*****************************************************************************/
/* VALUE MAP KEY                                                             */
/*****************************************************************************/

/** Key for value maps, that allows lookups without having to make a copy
    of the value being looked up.
*/

struct ValueMapKey {
    ValueMapKey();
    ValueMapKey(Utf8String str);
    ValueMapKey(std::string str);
    ValueMapKey(const MLDB::Coord & coord);
    ValueMapKey(const char * utf8Start);
    ValueMapKey(const char * utf8Start, size_t utf8Len);

    template<size_t N>
    ValueMapKey(const char (&name)[N])
        : utf8Start(name),
          utf8Len((N && name[N - 1])?N:N-1)  // remove null char from end
    {
    }

    /// Obtain a ValueMapKey that allocates nothing and has a reference to
    /// the data in name, which must be immutable and outlive the result
    static ValueMapKey ref(const Utf8String & name);

    /// Obtain a ValueMapKey that allocates nothing and has a reference to
    /// the data in name, which must be immutable and outlive the result
    static ValueMapKey ref(const std::string & name);

    /// Obtain a ValueMapKey that allocates nothing and has a reference to
    /// the data in coord, which must be immutable and outlive the result
    static ValueMapKey ref(const MLDB::Coord & coord);

    /// Obtain a ValueMapKey that allocates nothing and has a reference to
    /// the data in coord, which must be immutable and outlive the result
    static ValueMapKey ref(const char * coord);

    /// Obtain a ValueMapKey that allocates nothing and has a reference to
    /// the data in coord, which must be immutable and outlive the result
    template<size_t N>
    static ValueMapKey ref(const char (&name)[N])
    {
        return ValueMapKey(name);
    }

    bool empty() const { return utf8Len == 0; }

    MLDB::Coord toCoord() const;
    Utf8String toUtf8String() const;
    std::string rawString() const;
    
    bool startsWith(const Utf8String & str) const;

    bool operator < (const ValueMapKey & other) const;
    bool operator == (const ValueMapKey & other) const;

private:
    Utf8String str;
    const char * utf8Start;
    size_t utf8Len;
};

PREDECLARE_VALUE_DESCRIPTION(ValueMapKey);

ValueMapKey stringToKey(const std::string & str, ValueMapKey *);
std::string keyToString(const ValueMapKey & key);
std::string keyToString(ValueMapKey && key);
#endif

namespace MLDB {

struct Function;
struct MldbServer;
struct SqlExpression;
struct SqlRowExpression;
struct ExpressionValueInfo;
struct RowValueInfo;
struct KnownColumn;

typedef EntityType<Function> FunctionType;


#if 0
/*****************************************************************************/
/* FUNCTION CONTEXT                                                          */
/*****************************************************************************/

/** Context for functions, containing the values that have been
    set so far.
*/

struct FunctionContext {

    /** Initialize by taking the named values set in the
        other context and putting them into this context.
    */
    void initializeFrom(const FunctionContext & other);

    ExpressionValue get(const Utf8String & name) const;
    ExpressionValue get(const Coord & name) const;

    ExpressionValue getValueOrNull(const Utf8String & name) const;
    ExpressionValue getValueOrNull(const Coord & name) const;
    ExpressionValue getValueOrNull(const ValueMapKey & name) const;
    ExpressionValue getValueOrNull(const char * name) const;
    template<size_t N>
    ExpressionValue getValueOrNull(const char (&name)[N]) const
    {
        auto key = ValueMapKey::ref(name, N - 1);
        return getValueOrNull(key);
    }
    

    /// Used to provide a reference to a missing value
    static const ExpressionValue NONE;

    const ExpressionValue * findValue(const Utf8String & name) const;
    const ExpressionValue * findValue(const std::string & name) const;
    const ExpressionValue * findValue(const char * name) const;
    const ExpressionValue * findValue(const Coord & name) const;

    // This one is is special; it will look recursively
    const ExpressionValue * findValue(const ColumnName & name) const;

    template<typename Key>
    const ExpressionValue & get(const Key & name, ExpressionValue & storage) const
    {
        auto p = findValue(name);
        if (!p) return NONE;
        return *p;
    }

    template<typename Key>
    const ExpressionValue & mustGet(const Key & name, ExpressionValue & storage) const
    {
        auto p = findValue(name);
        if (!p) return NONE;
        return *p;
    }
    
    ExpressionValue get(const std::string & name) const;

    template<typename T>
    T get(const Utf8String & name) const
    {
        return getTyped(name, (T *)0);
    }

    template<typename T>
    T get(const std::string & name) const
    {
        return get<T>(Utf8String(name));
    }

    template<typename T>
    T get(const char * name) const
    {
        return get<T>(Utf8String(name));
    }

    template<typename T>
    T getTyped(const std::string & name, T * = 0) const
    {
        return getTyped(Utf8String(name), (T *)0);
    }

    CellValue getTyped(const Utf8String & name, CellValue * = 0) const;
    ExpressionValue getTyped(const Utf8String & name, ExpressionValue * = 0) const;
    RowValue getTyped(const Utf8String & name, RowValue * = 0) const;

    CellValue getTyped(const std::string & name, CellValue * = 0) const
    {
        return getTyped(Utf8String(name), (CellValue *)0);
    }
    ExpressionValue getTyped(const std::string & name, ExpressionValue * = 0) const
    {
        return getTyped(Utf8String(name), (ExpressionValue *)0);
    }
    RowValue getTyped(const std::string & name, RowValue * = 0) const
    {
        return getTyped(Utf8String(name), (RowValue *)0);
    }

    void set(const Utf8String & name, const ExpressionValue & value)
    {
        values[name] = value;
    }

    void set(const std::string & name, const ExpressionValue & value)
    {
        set(Utf8String(name), value);
    }

    void set(const char * name, const ExpressionValue & value)
    {
        set(Utf8String(name), value);
    }

    template<typename Arg>
    void setT(const std::string & name, const Arg & arg, Date ts)
    {
        set(Utf8String(name), ExpressionValue(arg, ts));
    }

    void update(const FunctionOutput & output);
    void update(FunctionOutput && output);

private:
    std::map<ValueMapKey, ExpressionValue> values;
    friend class FunctionContextDescription;
};

DECLARE_STRUCTURE_DESCRIPTION(FunctionContext);


/*****************************************************************************/
/* FUNCTION OUTPUT                                                           */
/*****************************************************************************/

struct FunctionOutput {
    FunctionOutput()
    {
    }

    // Only allow moving into the FunctionOutput structure
    FunctionOutput(ExpressionValue val);
    FunctionOutput & operator = (ExpressionValue val);

    std::map<ValueMapKey, ExpressionValue> values;

    /** Set a named value. */
    void set(ValueMapKey key, ExpressionValue value);

    void set(const Utf8String & name, ExpressionValue value)
    {
        set(ValueMapKey(name), std::move(value));
    }

    void set(const std::string & name, ExpressionValue value)
    {
        set(ValueMapKey(name), std::move(value));
    }

    void set(const char * name, ExpressionValue value)
    {
        set(ValueMapKey(name), std::move(value));
    }

    void set(const Coord & name, ExpressionValue value)
    {
        set(ValueMapKey(name), std::move(value));
    }

    template<typename Arg>
    void setT(const std::string & name, const Arg & arg, Date ts)
    {
        set(Utf8String(name), ExpressionValue(arg, ts));
    }

    void update(const FunctionOutput & output);
    void update(FunctionOutput && output);
};

DECLARE_STRUCTURE_DESCRIPTION(FunctionOutput);


/*****************************************************************************/
/* FUNCTION VALUE INFO                                                       */
/*****************************************************************************/

struct FunctionValueInfo {
    FunctionValueInfo()
    {
    }

    FunctionValueInfo(std::shared_ptr<ExpressionValueInfo> info)
        : valueInfo(std::move(info))
    {
    }

    /// Return the expression value info for a named value.  That describes how
    /// we convert the Any into an ExpressionValue and what that object
    /// looks like.
    std::shared_ptr<ExpressionValueInfo> getExpressionValueInfo() const
    {
        return valueInfo;
    }

    /** These two named values are the same.  Merge them together and make
        sure that they are compatible.
    */
    void merge(const FunctionValueInfo & other);

    std::shared_ptr<ExpressionValueInfo> valueInfo;
};

DECLARE_STRUCTURE_DESCRIPTION(FunctionValueInfo);


/*****************************************************************************/
/* FUNCTION VALUES                                                           */
/*****************************************************************************/

/** This structure describes the types and range of named values in a function, 
    both for input and for output.
*/

struct FunctionValues {

    std::map<ValueMapKey, FunctionValueInfo> values;

    FunctionValues(){}

    /** Construct from the value info of something that returns a row. */
    FunctionValues(const ExpressionValueInfo & rowInfo);

    void addValue(const Utf8String & name,
                  std::shared_ptr<ExpressionValueInfo> info);
    
    /** Add a named value that has an embedding value (fixed length list of
        real valued coordinates).
    */
    void addEmbeddingValue(const std::string & name,
                           ssize_t numDimensions);

    /** Add a named value that has a row value (key/value/timestamp tuples).
        This one is temporary and simply says that there is an open schema and
        so all columns are known.
    */
    void addRowValue(const std::string & name);

    /** Add a named value that has a row value (key/value/timestamp tuples) */
    void addRowValue(const std::string & name,
                     const std::vector<KnownColumn> & knownColumns,
                     SchemaCompleteness completeness = SCHEMA_CLOSED);
    
    /** Add a named value that is an atom (null, number, string). */
    void addAtomValue(const std::string & name);

    /** Add a named value that is a string (ASCII or UTF-8). */
    void addStringValue(const std::string & name);

    /** Add a named value that is a timestamp. */
    void addTimestampValue(const std::string & name);

    /** Add a named value that is a blob. */
    void addBlobValue(const std::string & name);

    /** Add a named value that is a floating point number. */
    void addNumericValue(const std::string & name);

    /** Return the value info for the given name. */
    const FunctionValueInfo & getValueInfo(const Utf8String & name) const;

    /** Check that this value is compatible as input to the other value.  Will
        throw an exception if not.
    */
    void checkValueCompatibleAsInputTo
        (const Utf8String & otherName,
         const FunctionValueInfo & otherValueInfo) const;

    /** Check that the entire set of values is compatible as an input to the
        function with the given input values requirements.
    */
    void checkCompatibleAsInputTo(const FunctionValues & expectedInput) const;

    /** Merge the two together. */
    void merge(const FunctionValues & other);

    /** Convert to a row information object. */
    std::shared_ptr<RowValueInfo> toRowInfo() const;

};

DECLARE_STRUCTURE_DESCRIPTION(FunctionValues);
#endif

typedef std::shared_ptr<RowValueInfo> FunctionValues;
typedef ExpressionValue FunctionOutput;
typedef ExpressionValue FunctionContext;


/*****************************************************************************/
/* FUNCTION INFO                                                             */
/*****************************************************************************/

/** A function is basically described by its input and its output values. */

struct FunctionInfo {

    /// Values that this function takes as an input
    std::shared_ptr<RowValueInfo> input;

    /// Values that this function produces as an output
    std::shared_ptr<RowValueInfo> output;
};

DECLARE_STRUCTURE_DESCRIPTION(FunctionInfo);


/*****************************************************************************/
/* FUNCTION APPLIER                                                          */
/*****************************************************************************/

/** This is a structure that can apply a given function over a given set of
    input data.

    Functions may override to include extra information.

    Note that this is essentially a BoundFunction now, apart from not taking
    an argument to its outside row scope.
*/

struct FunctionApplier {
    FunctionApplier(const Function * function = nullptr)
        : function(function)
    {
    }

    /** Virtual destructor is required so that derived classes will be properly
        destroyed by a unique_ptr.
    */
    virtual ~FunctionApplier()
    {
    }
    
    const Function * function;  ///< Function to which this applies
    FunctionInfo info;       ///< Information about the input and output of the applier

    /// Apply the function to the given context
    FunctionOutput apply(const FunctionContext & input) const;
};


/*****************************************************************************/
/* FUNCTION                                                                  */
/*****************************************************************************/

/** This represents a function: something that is called in real-time as part
    of a query.  It models an SQL function or a sequence of prediction steps
    that make up a deployed model.

    To use a function, you need to:

    1.  Initialize it from its constructor.  This should load up all of its
        resources, etc.
    2.  Call bind() with the input that's going into the function to get an
        applier, which is bound and optimized to work on the given structure
        of input.
    3.  Call the applier on each input context to get an output.
*/

struct Function: public MldbEntity {
    Function(MldbServer * server);

    virtual ~Function();

    MldbServer * server;
    
    virtual Any getStatus() const = 0;

    virtual std::string getKind() const
    {
        return "function";
    }

    /** Return details about the internal representation of the function.  This
        can be verbose.  Default implementation returns nothing.
    */
    virtual Any getDetails() const;

    /** Initialize a version of the function to operate based upon the given
        input values.  This gives an opportunity to specialize the function
        if the range or type of input values is more restricted than the
        function usually works with.

        Default will use getFunctionInfo(), for functions that don't specialize
        at all based upon their input.
    */
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const;

    /** Return the input and the output expected by the function.  Every
        function needs to be able to say what it expects; there is no
        such thing as a function that will take "whatever comes in".
    */
    virtual FunctionInfo getFunctionInfo() const = 0;

    /** Call an unbound function directly with the given input.  Note
        that this will re-bind the function on every call and is therefore
        horrendously inefficient; it should be used only for when the
        input is truly dynamic and there is no possible way to bind it.

        It's not a virtual method as it's complex in how it interprets
        its input.

        This method is actually defined in function_collection.cc, since it
        requires functionality from the server to create the scope for the
        bind.
    */
    FunctionOutput call(const ExpressionValue & input) const;

    /** Method to overwrite to handle a request.  By default, the function
        will return that it can't handle any requests.  Used to expose
        function-specific functionality.
    */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

protected:
    /** Used by the FunctionApplier to actually apply the function.  It allows
        access to the information put in the applier by the bind()
        method.
    */

    virtual FunctionOutput apply(const FunctionApplier & applier,
                                 const FunctionContext & context) const = 0;

    friend class FunctionApplier;
};


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/


std::shared_ptr<Function>
createFunction(MldbServer * server,
               const PolyConfig & config,
               const std::function<bool (const Json::Value & progress)> & onProgress,
               bool overwrite);

std::shared_ptr<Function>
obtainFunction(MldbServer * server,
               const PolyConfig & config,
               const std::function<bool (const Json::Value & progress)> & onProgress
                   = nullptr);

DECLARE_STRUCTURE_DESCRIPTION_NAMED(FunctionPolyConfigDescription, PolyConfigT<Function>);

std::shared_ptr<FunctionType>
registerFunctionType(const Package & package,
                     const Utf8String & name,
                     const Utf8String & description,
                     std::function<Function * (RestDirectory *,
                                               PolyConfig,
                                               const std::function<bool (const Json::Value)> &)>
                     createEntity,
                     TypeCustomRouteHandler docRoute,
                     TypeCustomRouteHandler customRoute,
                     std::shared_ptr<const ValueDescription> config,
                     std::set<std::string> registryFlags);

/** Register a new function kind.  This takes care of registering everything behind
    the scenes.
*/
template<typename FunctionT, typename Config>
std::shared_ptr<FunctionType>
registerFunctionType(const Package & package,
                     const Utf8String & name,
                     const Utf8String & description,
                     const Utf8String & docRoute,
                     TypeCustomRouteHandler customRoute = nullptr,
                     std::set<std::string> registryFlags = {})
{
    return registerFunctionType(package, name, description,
                                [] (RestDirectory * server,
                                    PolyConfig config,
                                    const std::function<bool (const Json::Value)> & onProgress)
                                {
                                    auto function = new FunctionT(FunctionT::getOwner(server), config, onProgress);
                                    function->logger = MLDB::getMldbLog<FunctionT>();
                                    return function;
                                },
                                makeInternalDocRedirect(package, docRoute),
                                customRoute,
                                getDefaultDescriptionSharedT<Config>(),
                                registryFlags);
}

template<typename FunctionT, typename Config>
struct RegisterFunctionType {
    RegisterFunctionType(const Package & package,
                         const Utf8String & name,
                         const Utf8String & description,
                         const Utf8String & docRoute,
                         TypeCustomRouteHandler customRoute = nullptr,
                         std::set<std::string> registryFlags = {})
    {
        handle = registerFunctionType<FunctionT, Config>
            (package, name, description, docRoute, customRoute,
             registryFlags);
    }

    std::shared_ptr<FunctionType> handle;
};

} // namespace MLDB
} // namespace Datacratic
