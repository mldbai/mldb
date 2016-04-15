/** value_function.h                                               -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Interface for functions into MLDB.
*/

#include "function.h"

namespace Datacratic {
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
                  std::shared_ptr<const ValueDescription> inputDescription,
                  std::shared_ptr<const ValueDescription> outputDescription);
    
    virtual Any getStatus() const;
    
    std::shared_ptr<const ValueDescription> inputDescription;
    std::shared_ptr<const ValueDescription> outputDescription;

    std::shared_ptr<ExpressionValueInfo> inputInfo;
    std::shared_ptr<ExpressionValueInfo> outputInfo;

    void fromInput(void * obj, const ExpressionValue & inputVal) const;
    ExpressionValue toOutput(const void * obj) const;

    virtual FunctionInfo getFunctionInfo() const;
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
                   std::shared_ptr<const ValueDescription> inputDesc
                       = getDefaultDescriptionSharedT<Input>(),
                   std::shared_ptr<const ValueDescription> outputDesc
                       = getDefaultDescriptionSharedT<Output>())
        : ValueFunction(server, std::move(inputDesc), std::move(outputDesc))
    {
    }

    /** Simple interface for when no special applier type is required.

        Should apply the function to the input and return the result
        of the function.

        Either this or applyT needs to be overridden, or an infinite
        loop will ensue.
    */
    virtual Output call(const Input & input) const
    {
        throw HttpReturnException(500, "ValueFunctionT type "
                                  + ML::type_name(*this)
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
                          const Input & input) const
    {
        return call(input);
    }
    
    virtual std::unique_ptr<Applier>
    bindT(SqlBindingScope & outerContext,
          const FunctionValues & input) const
    {
        std::unique_ptr<Applier> result(new Applier(this));
        result->info = getFunctionInfo();

        // Check that all values on the passed input are compatible with the required
        // inputs.
        // TO RESOLVE BEFORE MERGE
        throw HttpReturnException(600, "ValueFunctionT::bindT");
#if 0
        for (auto & p: result->info.input.values) {
            input.checkValueCompatibleAsInputTo(p.first.toUtf8String(), p.second);
        }
#endif

        return result;
    }

private:
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const
    {
        return bindT(outerContext, input);
    }
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                                 const FunctionContext & context) const
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
        Output out = applyT(*downcast, in);

        // Return the output
        return toOutput(&out);
    }

    template<typename InputT, typename OutputT>
    friend class FunctionApplierT;
};

} // namespace MLDB
} // namespace Datacratic
