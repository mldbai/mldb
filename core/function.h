/** function.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for functions into MLDB.
*/

#include "mldb/sql/dataset_types.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/http/http_exception.h"
#include "mldb/sql/path.h"


// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


#pragma once




namespace MLDB {

struct Function;
struct MldbServer;
struct SqlExpression;
struct SqlRowExpression;
struct ExpressionValueInfo;
struct RowValueInfo;
struct KnownColumn;

typedef EntityType<Function> FunctionType;


/*****************************************************************************/
/* FUNCTION INFO                                                             */
/*****************************************************************************/

/** A function is basically described by its input and its output values. */

struct FunctionInfo {

    /// Values that this function takes as an input.  Historically, functions
    /// could only take a single argument (which was a row), but now they
    /// can take any arity and can have non-row parameters.  The length of
    /// this array is the arity of the function.
    std::vector<std::shared_ptr<ExpressionValueInfo> > input;
    
    /// Values that this function produces as an output
    std::shared_ptr<ExpressionValueInfo> output;
    
    /** Statically check that the fields in input are compatible with our
        input specification, and throw an exception if incompatibility can
        be statically proven at bind time.  Note that this function succeeding
        is not a guarantee that input will always be compatible; if the
        input is dynamic, then it may fail at runtime.

        This version asserts also that there is only one input value.
    */

    void checkInputCompatibility(const ExpressionValueInfo & input) const;

    /** Statically check that the fields in input are compatible with our
        input specification, and throw an exception if incompatibility can
        be statically proven at bind time.  Note that this function succeeding
        is not a guarantee that input will always be compatible; if the
        input is dynamic, then it may fail at runtime.

        This version checks each of the values in input.
    */
    void checkInputCompatibility
    (const std::vector<std::shared_ptr<ExpressionValueInfo> > & inputs) const;

    /**  Will the function always return the same output given the same input
    */
    bool deterministic;
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
    ExpressionValue apply(const ExpressionValue & input) const;
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

    Function(MldbServer * server, const PolyConfig& config);

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
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const;
    
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
    ExpressionValue call(const ExpressionValue & input) const;

    /** Method to overwrite to handle a request.  By default, the function
        will return that it can't handle any requests.  Used to expose
        function-specific functionality.
    */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    /** Used by the FunctionApplier to actually apply the function.  It allows
        access to the information put in the applier by the bind()
        method.
    */

    virtual ExpressionValue apply(const FunctionApplier & applier,
                                  const ExpressionValue & context) const = 0;

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
                                    std::shared_ptr<spdlog::logger> logger = MLDB::getMldbLog<FunctionT>();
                                    auto function = new FunctionT(FunctionT::getOwner(server), config, onProgress);
                                    function->logger = std::move(logger); // noexcept
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

