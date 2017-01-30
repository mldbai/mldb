/** value_function.h                                               -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for functions into MLDB.
*/

#pragma once

#include "function.h"


namespace MLDB {

template<typename Input, typename Output>
struct FunctionApplierT;

template<typename Input, typename Output,
         typename Applier = FunctionApplierT<Input, Output> >
struct ValueFunctionT;


/*****************************************************************************/
/* VALUE FUNCTION                                                            */
/*****************************************************************************/

/** Represents a function taking an arbitrary value described by a
    ValueDescription and returning an arbritrary value described by a
    ValueDescription.  Normally both of these will be structures, although
    that is not strictly required.  This class provides the scaffolding
    to convert inputs and outputs and use the ValueDescription system to
    document them.
*/

struct ValueFunction: public Function {

    ValueFunction(MldbServer * server,
                  const PolyConfig& config,
                  std::shared_ptr<const ValueDescription> inputDescription,
                  std::shared_ptr<const ValueDescription> outputDescription);
    
    /// Default status is empty; don't make everyone copy and paste it.
    virtual Any getStatus() const override;
    
    /// Description of the binary type used as a parameter for the applyT
    /// function.
    std::shared_ptr<const ValueDescription> inputDescription;

    /// Description of the binary type returned by the applyT function.
    std::shared_ptr<const ValueDescription> outputDescription;

    /// Description of the ExpressionValue for the input
    std::shared_ptr<ExpressionValueInfo> inputInfo;

    /// Description of the ExpressionValue for the output
    std::shared_ptr<ExpressionValueInfo> outputInfo;

    /// Type of function that converts from an ExpressionValue described by
    /// inputInfo into a binary representation described by inputDescription.
    typedef std::function<void (void * obj, const ExpressionValue & inputVal) >
    FromInput;

    /// Type of function that converts from a binary representation (described
    /// by outputDescription) into an ExpressionValue described by outputInfo.
    typedef std::function<ExpressionValue (const void * obj)> ToOutput;

    /// Function that does the conversion from ExpressionValue -> binary for
    /// the function's input parameters
    FromInput fromInput;

    /// Function that does the conversion from binary -> ExpressionValue for
    /// the function's return type.
    ToOutput toOutput;
    
    /// Since we know the input and output types, we can provide a default
    /// implementation of this function.
    virtual FunctionInfo getFunctionInfo() const override;
};


/*****************************************************************************/
/* FUNCTION APPLIER T                                                        */
/*****************************************************************************/

/** This is a subclass of a FunctionApplier that expects specific types for
    its arguments.
*/

template<typename Input, typename Output>
struct FunctionApplierT: public FunctionApplier {
    FunctionApplierT(const Function * function = nullptr)
        : FunctionApplier(function)
    {
    }
    
    virtual ~FunctionApplierT()
    {
    }
};


/*****************************************************************************/
/* VALUE FUNCTION T                                                          */
/*****************************************************************************/

/** This is a convenience class for a function that takes simple input and
    output types describable with the ValueDescription class.
*/

template<typename Input, typename Output, typename Applier>
struct ValueFunctionT: public ValueFunction {

    typedef Applier ApplierT;
    typedef ValueFunctionT<Input, Output> BaseT;

    ValueFunctionT(MldbServer * server,
                   const PolyConfig& config,
                   std::shared_ptr<const ValueDescription> inputDesc
                       = getDefaultDescriptionSharedT<Input>(),
                   std::shared_ptr<const ValueDescription> outputDesc
                       = getDefaultDescriptionSharedT<Output>())
        : ValueFunction(server, config, std::move(inputDesc), std::move(outputDesc))
    {
    }

    /** Simple interface for when no special applier type is required.

        Should apply the function to the input and return the result
        of the function.

        Either this or applyT needs to be overridden, or an infinite
        loop will ensue.
    */
    virtual Output call(Input input) const
    {
        throw HttpReturnException(500, "ValueFunctionT type "
                                  + MLDB::type_name(*this)
                                  + " needs to override call()");
    }

    /** Complex interface for when a special applier type is required.

        Should apply the function to the input and return the result
        of the function, with the applier available for any bind-time
        information that needs to be used.
        
        Either this or applyT needs to be overridden, or an infinite
        loop will ensue.
    */
    virtual Output applyT(const ApplierT & applier,
                          Input input) const
    {
        return call(std::move(input));
    }
    
    virtual std::unique_ptr<Applier>
    bindT(SqlBindingScope & outerContext,
          const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
    {
        ExcAssert(config_);
        std::unique_ptr<Applier> result(new Applier(this));
        result->info = getFunctionInfo();
        result->info.checkInputCompatibility(input);
        result->info.deterministic = config_->deterministic;
        return result;
    }

private:
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        const override
    {
        return bindT(outerContext, input);
    }
    
    virtual ExpressionValue
    apply(const FunctionApplier & applier,
          const ExpressionValue & context) const override
    {
        const auto * downcast
            = dynamic_cast<const FunctionApplierT<Input, Output> *>(&applier);
        if (!downcast) {
            throw HttpReturnException(500, "Couldn't downcast applier");
        }

        // Convert the input from an ExpressionValue to its real type
        Input in;
        fromInput(&in, context);

        // Apply the function with the proper types
        Output out = applyT(*downcast, std::move(in));

        // Return the output
        return toOutput(&out);
    }

    template<typename InputT, typename OutputT>
    friend class FunctionApplierT;
};

} // namespace MLDB

