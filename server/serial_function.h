/** serial_function.h                                              -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Interface for functions into MLDB.
*/

#pragma once

#include "mldb/core/function.h"

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* SERIAL FUNCTION STEP CONFIG                                               */
/*****************************************************************************/

struct SerialFunctionStepConfig: public PolyConfigT<Function> {
    SerialFunctionStepConfig();

    SelectExpression with;      ///< Expression to set values of values for input
    SelectExpression extract;   ///< Expression to set values of values for output
};

DECLARE_STRUCTURE_DESCRIPTION(SerialFunctionStepConfig);


/*****************************************************************************/
/* SERIAL FUNCTION CONFIG                                                    */
/*****************************************************************************/

struct SerialFunctionConfig {
    std::vector<SerialFunctionStepConfig> steps;
};

DECLARE_STRUCTURE_DESCRIPTION(SerialFunctionConfig);


/** Used to apply a single function step (including a with and extract clause).
 */
struct FunctionStepApplier {
    FunctionStepApplier(SqlBindingScope & outerContext,
                        const FunctionValues & input,
                        const Function & function,
                        const SelectExpression & with,
                        const SelectExpression & extract);

    ~FunctionStepApplier();

    FunctionStepApplier(FunctionStepApplier && other) = default;
    FunctionStepApplier & operator= (FunctionStepApplier && other) = default;

    /** Apply the function step to the given input, returning the output
        values which will conform to getOutput().
    */
    FunctionOutput apply(const FunctionContext & input) const;

    /** Return the description of the values that this will output when
        applied.
    */
    const FunctionValues & getOutput() const;

    const Function * function;
    struct Itl;
    std::unique_ptr<Itl> itl;
};


/*****************************************************************************/
/* SERIAL FUNCTION                                                           */
/*****************************************************************************/

struct SerialFunction: public Function {
    SerialFunction(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);
    
    struct Step {
        Step(std::shared_ptr<Function> function,
             SelectExpression with, SelectExpression extract)
            : function(std::move(function)),
              with(std::move(with)), extract(std::move(extract))
        {
        }
        
        std::shared_ptr<Function> function;
        SelectExpression with;      ///< Expression to set values for input
        SelectExpression extract;   ///< Expression to set values for output

        /** Return the information about the input and output of this step,
            including the with and extract clauses.
        */
        FunctionInfo getStepInfo() const;
    };

    /// Initialize programatically from a series of steps
    SerialFunction(MldbServer * owner, std::vector<Step> steps);

    /// Initialize programatically from empty.  Init must be called later.
    SerialFunction(MldbServer * owner);

    void init(std::vector<Step> steps);

    SerialFunctionConfig functionConfig;
    std::vector<Step> steps;

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const;

    /** Used by the FunctionApplier to actually apply the function. */
    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    virtual Any getStatus() const;
};

} // namespace MLDB
} // namespace Datacratic
