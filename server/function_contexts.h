// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** function_contexts.h                                               -*- C++ -*-
    Jeremy Barnes, 14 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Contexts in which to execute the WITH and EXTRACT clauses of applying
    functions.
*/

#pragma once

#include "mldb/sql/binding_contexts.h"
#include "mldb/server/function.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* EXTRACT CONTEXT                                                           */
/*****************************************************************************/

/** Used to extract named values from the output of a function. */

struct ExtractContext: public SqlBindingScope {

    struct RowContext: public SqlRowScope {
        RowContext(const FunctionContext & input)
            : input(input)
        {
        }

        const FunctionContext & input;
    };

    ExtractContext(MldbServer * server,
                   FunctionValues values);

    MldbServer * server;
    FunctionValues values;

    /** Return an extractor function that will retrieve the given variable
        from the function input or output.
    */
    VariableGetter doGetVariable(const Utf8String & tableName,
                                 const Utf8String & variableName);

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<Utf8String (const Utf8String &)> keep);

    RowContext getRowContext(const FunctionContext & input) const
    {
        return RowContext(input);
    }

    virtual MldbServer * getMldbServer() const;
};

/*****************************************************************************/
/* FUNCTION EXPRESSION CONTEXT                                                  */
/*****************************************************************************/

/** Used to run an expression in a purely function context. */

struct FunctionExpressionContext: public ReadThroughBindingContext {

    struct RowContext: public SqlRowScope {
        RowContext(const FunctionContext & input)
            : input(input)
        {
        }

        const FunctionContext & input;
    };

    /// Initialize.  The info will be inferred from the function itself.
    FunctionExpressionContext(SqlBindingScope & context);
    
    /// Initialize with known input input.
    FunctionExpressionContext(SqlBindingScope & context,
                              FunctionValues input);
    
    /** Information for input values goes here. */
    FunctionValues input;

    /** Do we know our input?  If not we are inferring it. */
    bool knownInput;

    /** Return an extractor function that will retrieve the given variable
        from the function input.
    */
    VariableGetter doGetVariable(const Utf8String & tableName,
                                 const Utf8String & variableName);
    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<Utf8String (const Utf8String &)> keep);

    RowContext getRowContext(const FunctionContext & input) const
    {
        return RowContext(input);
    }

private:

    bool findVariableRecursive(const Utf8String& variableName, std::shared_ptr<ExpressionValueInfo>& valueInfo, SchemaCompleteness& schemaCompleteness) const;
};

} // namespace MLDB
} // namespace Datacratic
