// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* sql_expression_function_operations.cc

   Jeremy Barnes, 15 March 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/sql/sql_expression.h"
#include "mldb/server/function_contexts.h"


using namespace std;


namespace Datacratic {
namespace MLDB {

/** Bind the apply function expression with the given parameters into a bound
    SqlExpression.

    This is implemented here to allow libsql_expression to not depend on the
    main MLDB library.
*/
BoundSqlExpression
bindApplyFunctionExpression(const Utf8String & functionName,
                            const SelectExpression & with,
                            const SqlExpression & extract,
                            const SqlExpression * expr,
                            SqlBindingScope & context)
{
    // Ask what our function needs
    std::shared_ptr<Function> function = context.doGetFunctionEntity(functionName);

    auto boundWith = with.bind(context);

    // Initialize the function, now we know what it's input will look like
    std::shared_ptr<FunctionApplier> applier(function->bind(context, *boundWith.info).release());

    // And figure out what the output looks like afterwards
    FunctionValues extractInfo = applier->info.output;
    
    ExtractContext extractContext(context.getMldbServer(), extractInfo);

    // Extract gets bound on the output values, since it happens after the function
    auto boundExtract = extract.bind(extractContext);

    return {[=] (const SqlRowScope & row, 
                 ExpressionValue & storage, 
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                // Run the with expressions
                ExpressionValue withVal = boundWith(row);
                FunctionOutput withOutput(std::move(withVal));

                //cerr << "withOutput " << jsonEncode(withOutput);

                FunctionContext context;
                context.update(std::move(withOutput));

                if (function) {
                    FunctionOutput functionOutput = applier->apply(context);
                    //cerr << "functionoutput = " << jsonEncode(functionOutput) << endl;
                    context.update(std::move(functionOutput));
                }

                // Apply the extract statement to calculate any derived features and
                //   to send values to the output.
                
                auto rowFunctionContext
                    = extractContext.getRowContext(context);
                
                auto val = boundExtract(rowFunctionContext, storage);               

                return storage = val;
            },
            expr,
            boundExtract.info};
}

extern BoundSqlExpression
(*bindApplyFunctionExpressionFn) (const Utf8String & functionName,
                                  const SelectExpression & with,
                                  const SqlExpression & extract,
                                  const SqlExpression * expr,
                                  SqlBindingScope & context);

/** Bind the apply function expression with the given parameters into a bound
    SqlExpression.

    This is implemented here to allow libsql_expression to not depend on the
    main MLDB library.
*/
BoundSqlExpression
bindSelectApplyFunctionExpression(const Utf8String & functionName,
                                  const SelectExpression & with,
                                  const SqlRowExpression * expr,
                                  SqlBindingScope & context)
{
    std::shared_ptr<Function> function = context.doGetFunctionEntity(functionName);

    // Function input is...
    auto boundWith = with.bind(context);

    // Initialize the function, now we know what it's input will look like
    std::shared_ptr<FunctionApplier> applier(function->bind(context, *boundWith.info).release());

    //cerr << "function info is " << jsonEncode(applier->info) << endl;

    // And figure out what the output looks like afterwards
    FunctionValues extractInfo = applier->info.output;

    ExtractContext extractContext(context.getMldbServer(), extractInfo);  

    auto captureColumnName = [&] (const Utf8String & inputColumnName) -> Utf8String
        {
           return inputColumnName;
        };

    auto allColumns = extractContext.doGetAllColumns("", captureColumnName);

    std::vector<KnownColumn> knownColumns = allColumns.info->getKnownColumns();

    auto exec = [=] (const SqlRowScope & row, ExpressionValue & storage, const VariableFilter & filter)
        -> const ExpressionValue &
            {
                // Run the with expressions
                ExpressionValue withVal = boundWith(row);

                FunctionOutput withOutput(std::move(withVal));

                FunctionContext context;
                context.update(std::move(withOutput));

                if (function) {
                    FunctionOutput functionOutput = applier->apply(context);
                    //cerr << "functionoutput = " << jsonEncode(functionOutput) << endl;
                    context.update(std::move(functionOutput));
                }

                /* Apply the extract statement to calculate any derived features and
                   to send values to the output.
                */
                auto rowFunctionContext = extractContext.getRowContext(context);

                //this version returns all columns
                return storage = std::move(allColumns.exec(rowFunctionContext));
            };

    BoundSqlExpression result(exec, expr, allColumns.info);
    return std::move(result);
}

extern BoundSqlExpression
(*bindSelectApplyFunctionExpressionFn) (const Utf8String & functionName,
                                        const SelectExpression & with,
                                        const SqlRowExpression * expr,
                                        SqlBindingScope & context);

namespace {
struct AtInit {
    AtInit()
    {
        bindApplyFunctionExpressionFn = bindApplyFunctionExpression;
        bindSelectApplyFunctionExpressionFn = bindSelectApplyFunctionExpression;
    }
} atInit;
} // file scope

} // namespace MLDB
} // namespace Datacratic
