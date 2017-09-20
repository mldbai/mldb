/* llvm.h                                                          -*- C++ -*-
   Jeremy Barnes, July 25, 2017
   Copyright (c) 2017 mldb.ai inc.  All rights reserved.
   
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Allows a function to be JIT compiled with LLVM.
*/

#include "llvm.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/dataset_context.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {


/*****************************************************************************/
/* LLVM FUNCTION                                                             */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(LlvmFunctionConfig);

LlvmFunctionConfigDescription::
LlvmFunctionConfigDescription()
{
    addField("query", &LlvmFunctionConfig::query,
             "SQL query to compile.  The values in the dataset, as "
             "well as the input values, will be available for the expression "
             "calculation");
}

LlvmFunction::
LlvmFunction(MldbServer * owner,
             PolyConfig config,
             const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
{
    functionConfig = config.params.convert<LlvmFunctionConfig>();
}

Any
LlvmFunction::
getStatus() const
{
    Json::Value result;
    return result;
}

/** Structure that does all the work of the SQL expression function. */
struct LlvmFunctionApplier: public FunctionApplier {
    LlvmFunctionApplier(const LlvmFunction * function,
                        const LlvmFunctionConfig & config)
        : FunctionApplier(function), function(function)
    {
        std::set<Utf8String> inputParams;
        
        // Called when we bind a parameter, to get its information
        auto getParamInfo = [&] (const Utf8String & paramName)
            {
                inputParams.insert(paramName);
                return std::make_shared<AnyValueInfo>();
            };

        pipeline
            = PipelineElement::root
            (std::make_shared<SqlExpressionMldbScope>(function->server))
            ->statement(*config.query.stm, getParamInfo);

        // Bind the pipeline; this populates the input parameters
        boundPipeline = pipeline->bind();

        std::vector<KnownColumn> inputColumns;
        inputColumns.reserve(inputParams.size());
        for (auto & p: inputParams) {
            inputColumns.emplace_back(PathElement(p),
                                      std::make_shared<AnyValueInfo>(),
                                      COLUMN_IS_SPARSE);
        }

        if (!inputColumns.empty())
            this->info.input.emplace_back(new RowValueInfo(std::move(inputColumns),
                                                           SCHEMA_CLOSED));
        

        // What type does the pipeline return?
        this->info.output = ExpressionValueInfo::toRow
            (boundPipeline->outputScope()->outputInfo().back());
    }

    virtual ~LlvmFunctionApplier()
    {
    }

    ExpressionValue apply(const ExpressionValue & context) const
    {
        // 1.  Run our generator, finding all rows
        BoundParameters params
            = [&] (const Utf8String & name) -> ExpressionValue
            {
                return context.getColumn(name);
            };

        StructValue result;
        
        return std::move(result);
    }

    const LlvmFunction * function;
    std::shared_ptr<PipelineElement> pipeline;
    std::shared_ptr<BoundPipelineElement> boundPipeline;
};

std::unique_ptr<FunctionApplier>
LlvmFunction::
bind(SqlBindingScope & outerContext,
     const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<LlvmFunctionApplier> result
        (new LlvmFunctionApplier(this, functionConfig));

    result->info.checkInputCompatibility(input);

    return std::move(result);
}

ExpressionValue
LlvmFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    return static_cast<const LlvmFunctionApplier &>(applier)
        .apply(context);
}

FunctionInfo
LlvmFunction::
getFunctionInfo() const
{
    LlvmFunctionApplier applier(this, functionConfig);
    return applier.info;
}

static RegisterFunctionType<LlvmFunction, LlvmFunctionConfig>
regLlvmFunction(builtinPackage(),
                    "llvm.query",
                    "Run a single row SQL query against a dataset",
                    "functions/LlvmFunction.md.html");


/*****************************************************************************/
/* LLVM EXPRESSION FUNCTION                                                  */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(LlvmExpressionFunctionConfig);

LlvmExpressionFunctionConfigDescription::
LlvmExpressionFunctionConfigDescription()
{
    addField("expression", &LlvmExpressionFunctionConfig::expression,
             "SQL expression function to run.  Takes the same syntax as a SELECT "
             "clause (but without the SELECT keyword); for example "
             "'x, y + 1 AS z'");
    addField("prepared", &LlvmExpressionFunctionConfig::prepared,
             "Do we pre-prepare the expression to be run many times quickly?  "
             "If this is true, it will only be bound once, for generic "
             "inputs, and so will allow for quick individual queries, "
             "possibly at the expense of batch queries being slower.  In "
             "this case, the expression also cannot refer to variables "
             "outside of the arguments to the expression.  "
             "If this is false, the default, then for every query the "
             "expression will be specialized (rebound) for that query's "
             "data type.  "
             "This can lead to faster batch queries, at the expense of a "
             "possibly high per-query overhead for individual queries.",
             false);
    addField("raw", &LlvmExpressionFunctionConfig::raw,
             "If true, then the output will be raw (just the result "
             "of the expression will be returned; it will not be turned "
             "into a row and the name of the output will be ignored).  If "
             "false (default), then the output will be structured into "
             "a row.  For example, the expression `1 AS z` will return "
             "`1` if raw is true, but `{z: 1}` if raw is "
             "false.", false);
    addField("autoInput", &LlvmExpressionFunctionConfig::autoInput,
             "If true, then a function that takes a single parameter "
             "will automatically pass that parameter without needing "
             "to put it within an object.  For example, if `expression` "
             "is `x + 1`, then with `autoInput` as `false` the function "
             "must be called with `{x: 2}` but with `autoInput` as `true` "
             "the function can be called with `2` and the `x` will be "
             "added automatically.", false);
}

LlvmExpressionFunction::
LlvmExpressionFunction(MldbServer * owner,
                      PolyConfig config,
                      const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config),
      outerScope(new SqlExpressionMldbScope(owner)),
      innerScope(new SqlExpressionExtractScope(*outerScope))
{
    functionConfig = config.params.convert<LlvmExpressionFunctionConfig>();

    if (functionConfig.prepared) {
        // 1.  Bind the expression in.  That will tell us what it is expecting
        //     as an input.
        this->bound = doBind(*innerScope);

        // 2.  Our output is known by the bound expression
        this->info.output = this->bound.info;
        
        if (functionConfig.autoInput) {
            std::tie(this->preparedAutoInputName, info.input)
                = getAutoInputName(*innerScope);
        }
        else {
            // 3.  Infer the input, now the binding is all done
            innerScope->inferInput();
            
            // 4.  Our required input is known by the binding context, as it
            // records what was read.
            info.input.emplace_back(innerScope->inputInfo);
        }

    }
}

LlvmExpressionFunction::
~LlvmExpressionFunction()
{
}

std::tuple<PathElement, std::vector<std::shared_ptr<ExpressionValueInfo> > >
LlvmExpressionFunction::
getAutoInputName(SqlExpressionExtractScope & innerScope) const
{
    if (innerScope.inferredInputs.size() != 1) {
        Utf8String knownVars;
        for (auto & v: innerScope.inferredInputs) {
            if (!knownVars.empty())
                knownVars += ", ";
            knownVars += v.toUtf8String();
        }
        throw HttpReturnException
            (400, "An sql.expression function with autoInput=true "
             "must have a single variable in the expression; "
             "the passed expression " + functionConfig.expression.surface
             + " had " + to_string(innerScope.inferredInputs.size())
             + " variables (" + knownVars + ").");
    }
    // Can take any input
    std::vector<std::shared_ptr<ExpressionValueInfo> > inputs
        = {std::make_shared<AnyValueInfo>()};
    return make_tuple((*innerScope.inferredInputs.begin())[0],
                      std::move(inputs));
}

BoundSqlExpression
LlvmExpressionFunction::
doBind(SqlExpressionExtractScope & innerScope) const
{
    if (functionConfig.raw) {
        // 1.  Grab the single SqlExpression that we need from the select
        //     expression
        if (functionConfig.expression.clauses.size() != 1) {
            throw HttpReturnException
                (400, "An sql.expression function with raw=true "
                 "must have a single clause in the select; there were "
                 + to_string(functionConfig.expression.clauses.size())
                 + " in the passed expression "
                 + functionConfig.expression.surface);
        }
        auto singleClause = functionConfig.expression.clauses[0];
        auto named
            = std::dynamic_pointer_cast<NamedColumnExpression>(singleClause);
        if (!named) {
            throw HttpReturnException
                (400, "An sql.expression function with raw=true "
                 "must have a single statement in the select; passed "
                 "expression was " + functionConfig.expression.surface
                 + " which parsed as " + functionConfig.expression.print()
                 + ".");
        }
        return named->expression->bind(innerScope);
    }
    else {
        // 1.  Bind the expression in.  That will tell us what it is expecting
        //     as an input.
        return functionConfig.expression.bind(innerScope);
    }
}

Any
LlvmExpressionFunction::
getStatus() const
{
    Json::Value result;
    result["expression"]["surface"] = functionConfig.expression.surface;
    result["expression"]["ast"] = functionConfig.expression.print();
    result["info"] = jsonEncode(getFunctionInfo());
    return result;
}

/** Structure that does all the work of the SQL expression function. */
struct LlvmExpressionFunctionApplier: public FunctionApplier {
    LlvmExpressionFunctionApplier
        (SqlBindingScope & outerScope,
         const LlvmExpressionFunction * function,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        : FunctionApplier(function),
          function(function),
          autoInputName(nullptr),
          innerScope(outerScope, input.at(0))
    {
        if (!function->functionConfig.prepared) {
            // Specialize to this input
            this->bound = function->doBind(innerScope);

            if (function->functionConfig.autoInput) {
                std::tie(this->autoInputNameStorage,
                         this->info.input)
                    = function->getAutoInputName(innerScope);
                this->autoInputName = &this->autoInputNameStorage;
            }
            else {
                innerScope.inferInput();
                this->info.input = { std::move(innerScope.inputInfo) };
            }
            // That leads to a specialized output
            this->info.output = std::move(bound.info);
        }
        else {
            this->info = function->info;
            this->autoInputName = &function->preparedAutoInputName;
        }
    }

    virtual ~LlvmExpressionFunctionApplier()
    {
    }

    ExpressionValue apply(const ExpressionValue & input) const
    {
        ExpressionValue autoInputNameStorage;
        if (function->functionConfig.autoInput) {
            ExcAssert(this->autoInputName);
            StructValue val;
            val.emplace_back(*autoInputName, input);
            autoInputNameStorage = std::move(val);
        }

        const ExpressionValue & realInput 
            = function->functionConfig.autoInput
            ? autoInputNameStorage
            : input;

        // We know that we won't go outside of the current row, so we can
        // pass in a dummy object here.
        SqlRowScope outerRow;

        if (function->functionConfig.prepared) {
            // Use the pre-bound version.   
            return function->bound(function->innerScope->getRowScope(realInput),
                                   GET_LATEST);
        }
        else {
            // Use the specialized version. 
            return bound(this->innerScope.getRowScope(realInput),
                         GET_LATEST);
        }
    }

    const LlvmExpressionFunction * function;
    const PathElement * autoInputName;
    PathElement autoInputNameStorage;
    SqlExpressionExtractScope innerScope;
    BoundSqlExpression bound;
};

std::unique_ptr<FunctionApplier>
LlvmExpressionFunction::
bind(SqlBindingScope & outerContext,
     const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<LlvmExpressionFunctionApplier> result
        (new LlvmExpressionFunctionApplier(outerContext, this, input));

    result->info.checkInputCompatibility(input);

    return std::move(result);
}

ExpressionValue
LlvmExpressionFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    return static_cast<const LlvmExpressionFunctionApplier &>(applier)
           .apply(context);
}

FunctionInfo
LlvmExpressionFunction::
getFunctionInfo() const
{
    if (functionConfig.prepared) {
        return this->info;
    }

    FunctionInfo result;

    // 1.  Create a binding context to see what this function takes
    //     We want the pure function information, so we assume there is
    //     no context for it apart from MLDB itself.
    SqlExpressionMldbScope ultimateScope(MldbEntity::getOwner(this->server));
    SqlExpressionExtractScope outerScope(ultimateScope);

    // 2.  Bind the expression in.  That will tell us what it is expecting
    //     as an input.
    BoundSqlExpression bound = functionConfig.expression.bind(outerScope);

    // 3.  Our output is known by the bound expression
    result.output = ExpressionValueInfo::toRow(bound.info);
    
    // 4.  Infer our input
    outerScope.inferInput();

    // 4.  Our required input is known by the binding context, as it records
    //     what was read.
    if (functionConfig.autoInput) {
        result.input = std::get<1>(getAutoInputName(outerScope));
    }
    else {
        result.input.emplace_back(outerScope.inputInfo);
    }
    
    return result;
}

static RegisterFunctionType<LlvmExpressionFunction,
                            LlvmExpressionFunctionConfig>
regLlvmExpressionFunction(builtinPackage(),
                         "llvm.expression",
                         "Run an SQL expression as a function",
                         "functions/LlvmExpressionFunction.md.html");


} // namespace MLDB
