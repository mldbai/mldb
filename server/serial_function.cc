/* serial_function.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Function support.
*/

#include "serial_function.h"
#include "mldb/server/function_collection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/server/function_contexts.h"
#include "mldb/types/map_description.h"
#include "mldb/types/any_impl.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* SERIAL FUNCTION STEP CONFIG                                               */
/*****************************************************************************/

SerialFunctionStepConfig::
SerialFunctionStepConfig()
    : with(SelectExpression::parse("*")),
      extract(SelectExpression::parse("*"))
{
}

DEFINE_STRUCTURE_DESCRIPTION(SerialFunctionStepConfig);

SerialFunctionStepConfigDescription::
SerialFunctionStepConfigDescription()
{
    addParent<PolyConfigT<Function> >();

    addField("with", &SerialFunctionStepConfig::with,
             "SQL select expression that provides the input values"
             "to the function.  This can be used to "
             "connect values up to output values of previous functions that don't "
             "necessarily have the same names, or to calculate values in "
             "place.  The default will extract all values from all previous "
             "functions and input values and make them available to values with the "
             "same name.",
             SelectExpression::parse("*"));
    addField("extract", &SerialFunctionStepConfig::extract,
             "SQL select expression that extracts values from the output values "
             "of the function and adds them to the final output of the function.  "
             "This allows for the output of values to be renamed, modified or "
             "ignored.  The default will extract all output values of the function "
             "and add them with the same name to the Serial function's result.",
             SelectExpression::parse("*"));
}


/*****************************************************************************/
/* SERIAL FUNCTION CONFIG                                                       */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SerialFunctionConfig);

SerialFunctionConfigDescription::
SerialFunctionConfigDescription()
{
    addField("steps", &SerialFunctionConfig::steps,
             "A list of functions that will be executed in the given order in "
             "order to produce the output of the function.");
}


/*****************************************************************************/
/* FUNCTION STEP APPLIER                                                        */
/*****************************************************************************/

struct FunctionStepApplier::Itl {
    Itl(SqlBindingScope & outerContext,
        const FunctionValues & input,
        const Function & function,
        const SelectExpression & with,
        const SelectExpression & extract)
        : withBindingContext(outerContext, input),
          boundWith(with.bind(withBindingContext)),
          applier(function.bind(withBindingContext, *boundWith.info)),
          extractBindingContext(outerContext, applier->info.output),
          boundExtract(extract.bind(extractBindingContext)),
          outputValues(*boundExtract.info)
    {
    }

    FunctionExpressionContext withBindingContext;
    BoundSqlExpression boundWith;
    std::unique_ptr<FunctionApplier> applier;
    FunctionExpressionContext extractBindingContext;
    BoundSqlExpression boundExtract;
    FunctionValues outputValues;
};

FunctionStepApplier::
FunctionStepApplier(SqlBindingScope & outerContext,
                    const FunctionValues & input,
                    const Function & function,
                    const SelectExpression & with,
                    const SelectExpression & extract)
    : function(&function),
      itl(new Itl(outerContext, input, function, with, extract))
{
}

FunctionStepApplier::
~FunctionStepApplier()
{
}

FunctionOutput
FunctionStepApplier::
apply(const FunctionContext & input) const
{
    //cerr << "input is " << jsonEncode(input) << endl;
            
    // Run the WITH expression to get our values from the input
    SqlRowScope outerScope;
    auto withRow = itl->withBindingContext.getRowContext(outerScope, input);
    FunctionOutput withOutput;
    withOutput = itl->boundWith(withRow);

    FunctionContext selectContext;
    selectContext.update(std::move(withOutput));

    FunctionOutput stepOutput = itl->applier->apply(outerScope, selectContext);

    //cerr << "stepOutput = " << jsonEncode(stepOutput) << endl;

    FunctionContext extractContext;
    extractContext.update(std::move(stepOutput));

    // Now the extract
    auto extractRow = itl->extractBindingContext.getRowContext(outerScope, extractContext);
    return itl->boundExtract(extractRow);
}

const FunctionValues &
FunctionStepApplier::
getOutput() const
{
    return itl->outputValues;
}


/*****************************************************************************/
/* SERIAL FUNCTION                                                           */
/*****************************************************************************/

SerialFunction::
SerialFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<SerialFunctionConfig>();

    for (auto & s: functionConfig.steps) {
        steps.emplace_back(obtainFunction(server, s, onProgress), s.with, s.extract);
    }
}

SerialFunction::
SerialFunction(MldbServer * owner, std::vector<Step> steps)
    : Function(owner), steps(std::move(steps))
{
}

SerialFunction::
SerialFunction(MldbServer * owner)
    : Function(owner)
{
}

void
SerialFunction::
init(std::vector<Step> steps)
{
    ExcAssert(this->steps.empty());
    this->steps = std::move(steps);
}

/** This infers what the input to a WITH clause needs to look like in order
    to generate the given input for a function.  Each variable that is mentioned
    in the requiredWithOutput is traced back to the input requirements to
    generate that variable, and these are extracted.
*/
static FunctionValues
inferInputFromOutput(const FunctionValues & requiredWithOutput,
                     const SelectExpression & with)
{
    FunctionValues inputInfo;

    for (auto & p: requiredWithOutput.values) {
        Utf8String name = p.first.toUtf8String();

        // Check the with expression to find how to generate it
        bool foundName = false;

        // Now scan clauses
        for (auto & clause: with.clauses) {
            //cerr << "trying clause " << clause->surface << endl;
            
            const WildcardExpression * wildcard
                = dynamic_cast<const WildcardExpression*>(clause.get());

            if (wildcard) {
                // TODO: excluding...

                // See if this variable matches
                if (name.startsWith(wildcard->asPrefix)) {
                    //cerr << "Matches wildcard" << endl;

                    Utf8String inputName = name;
                    inputName.replace(0, wildcard->asPrefix.length(),
                                         wildcard->prefix);
                            
                    if (foundName)
                        throw HttpReturnException
                            (400, "Multiple ways to produce value '" + name + "' for function");
                    foundName = true;

                    auto it = inputInfo.values.find(inputName);
                    if (it != inputInfo.values.end()) {
                        continue;
                    }

                    auto & valueInfo = inputInfo.values[inputName];
                    valueInfo = p.second;
                    continue;
                }
            }

            const ComputedVariable * computed
                = dynamic_cast<const ComputedVariable *>(clause.get());

            if (computed) {
                if (computed->alias == name) {

                    foundName = true;

                    // Record all of the inputs to the expression
                    auto vars = computed->expression->variableNames();

                    for (auto & v: vars) {
                        
                        // If we ask for a variable with a scope, we
                        // are asking for something that's bound
                        // externally, and so we skip it.

                        if (!v.first.scope.empty())
                            continue;

                        auto & valueInfo = inputInfo.values[v.first.name];

                        // TODO: infer types
                        
                        if (!valueInfo.valueInfo)
                            valueInfo.valueInfo.reset(new AnyValueInfo());
                    }
                }
            }
        }
        
        if (!foundName)
            throw HttpReturnException(400, "Couldn't find how to obtain input value '"
                                      + name + "' from extract clause");
    }

    return inputInfo;
}

FunctionInfo
SerialFunction::Step::
getStepInfo() const
{
    FunctionInfo result;

    // 1.  Ask the function what it needs for input and output

    auto functionInfo = function->getFunctionInfo();

    cerr << "functionInfo = " << jsonEncode(functionInfo);

    // 2.  The output of the function is what the extract clause
    //     will output.

    ExtractContext extractContext(function->server, functionInfo.output);
    
    // Bind the expression so that we know everything that is read
    // from.
    BoundSqlExpression boundExtract = extract.bind(extractContext);

    cerr << "boundExtract.info = " << jsonEncode(boundExtract.info) << endl;

    result.output = *boundExtract.info;

    // 3.  The input to the function is what the with context requires
    result.input = inferInputFromOutput(functionInfo.input, with);

    cerr << "returning result " << jsonEncode(result) << endl;
    
    return result;
}

struct SerialFunctionApplier: public FunctionApplier {

    SerialFunctionApplier(SqlBindingScope & outerContext,
                          const SerialFunction * function,
                          const FunctionValues & inputInfo)
    {
        this->function = function;
        this->info.input = inputInfo;

        FunctionValues currentInput = inputInfo;

        // Create all of the steps
        for (const SerialFunction::Step & step: function->steps) {
            this->steps.emplace_back(outerContext, currentInput, *step.function, step.with, step.extract);
            
            // The output of the sub-function is copied to the output of the function
            this->info.output.merge(this->steps.back().getOutput());

            // The output of the sub-function is available as input to the next functions
            currentInput.merge(this->steps.back().getOutput());
        }
    }

    std::vector<FunctionStepApplier> steps;
};

std::unique_ptr<FunctionApplier>
SerialFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<SerialFunctionApplier> result
        (new SerialFunctionApplier(outerContext, this, input));
    return std::move(result);
}

FunctionOutput
SerialFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    auto & serialApplier = static_cast<const SerialFunctionApplier &>(applier);

    ExcAssertEqual(serialApplier.steps.size(), this->steps.size());
    
    FunctionContext input = context;
    FunctionOutput output;

    for (unsigned i = 0;  i < serialApplier.steps.size();  ++i) {
        auto & step = serialApplier.steps[i];
        try {
            FunctionOutput stepOutput = step.apply(input);
            if (i != serialApplier.steps.size() - 1)
                input.update(stepOutput);
            output.update(std::move(stepOutput));
        } catch (const std::exception & exc) {
            rethrowHttpException(-1, "Error applying step " + to_string(i) + " of type "
                                 + ML::type_name(*step.function) + " to function: "
                                 + exc.what(),
                                 "inputContext", context);
        }
    }

    return output;
}

FunctionInfo
SerialFunction::
getFunctionInfo() const
{
    // Go through one by one

    // The input is the union of the input of each of the function steps
    // The output is the union of the output of each of the function steps

    // Values that aren't satisfied internally and so need to be passed from
    // the output in.
    FunctionValues externalInput;

    // Values that are accumulated from the output of previous functions
    FunctionValues internalInput;

    // Values that are written as part of the output
    FunctionValues output;

    for (auto & s: steps) {
        FunctionInfo stepInfo = s.getStepInfo();
        
        cerr << "step has info " << jsonEncode(stepInfo);

        // Each input value is satisfied from the accumulated output, or if
        // not available there, from the required output.

        for (auto & p: stepInfo.input.values) {
            if (internalInput.values.count(p.first)) {
                // TODO: check compatibility
            }
            else {
                // Satisfy externally
                if (externalInput.values.count(p.first)) {
                    // TODO: check compatibility
                }
                else {
                    externalInput.addValue(p.first.toUtf8String(), p.second.valueInfo);
                }
            }
        }
        
        internalInput.merge(stepInfo.output);
        output.merge(stepInfo.output);
    }
    
    return { externalInput, output };
}

Any
SerialFunction::
getStatus() const
{
    vector<Any> result;
    for (auto & s: steps) {
        result.push_back(s.function->getStatus());
    }
    return result;
}


/*****************************************************************************/
/* FUNCTIONS                                                                 */
/*****************************************************************************/

namespace {

RegisterFunctionType<SerialFunction, SerialFunctionConfig>
regSerialFunction(builtinPackage(),
                  "serial",
                  "Function type that runs several functions in order",
                  "functions/Serial.md.html");

} // file scope


} // namespace MLDB
} // namespace Datacratic
