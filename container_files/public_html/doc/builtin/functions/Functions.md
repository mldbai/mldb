# Intro to Functions

User-defined Functions are named, reusable programs used to implement streaming computations which can accept input values and return output values. Functions are used to:

* Encapsulate [SQL expressions](../sql/Sql.md)
* Apply machine learning models parameterized by the output of a training Procedure

Functions can be created directly via a [REST API call](../functions/FunctionConfig.md) or by passing a `functionName` argument to most training [Procedures](../procedures/Procedures.md).

## Input and Output Values

Functions have a fixed set of input values and output values. Input and output values can be either scalar or variable-length row-like values. The input and output values are named so that it is easy to identify them.  Also, this permits function's output values to be passed as another function's input values.  This composition of functions can be done using the ![](%%doclink serial function).

## Applying Function via REST Endpoints

All MLDB Functions are automatically accessible as [REST Endpoints](Application.md), which can be used to apply machine learning models in a streaming/real-time process. Functions can be applied via a REST API call like `GET /v1/functions/<id>/application?input={<values>}`.

Functions support input values passed in as query string parameters or as a JSON payload. In both cases, the REST call must be a GET.

## Applying Function via SQL

In MLDB SQL, Functions can be  [applied much like built-in functions](../sql/ValueExpression.md): they accept a row as input and return a row as output.

## Available Function Types

Functions are created via a [REST API call](../functions/FunctionConfig.md) with one of the following types:

![](%%availabletypes function table)

## See also

* ![](%%nblink _tutorials/Procedures and Functions Tutorial) 
