# Serial Function

Functions of this type apply a list of functions in sequence.  It can be used to compose
functions into a more complex operation.

The resulting function has input values that are available for each sub-function.
Each sub-function in the sequence has access to these input values and all the
output values of the preceding sub-functions.

## Configuration

![](%%config function serial)

![](%%type Datacratic::MLDB::SerialFunctionStepConfig)

The `type` field can take any of the following function types:

![](%%availabletypes function table)

## Connecting input to output

The matching of the output values of a function to the
input values of another function appearing later in the sequence
is done through their names.  This requires a mechanism to
rename values or input values.  This is done via SQL expressions.

Each sub-function is executed in its own context.  The input
values of that context are defined via a `with` expression and the
output values are defined via a `extract` expression.

The following diagram shows this in detail:

![Serial Function Diagram](/doc/builtin//img/SerialFunction.svg)

The following points are important:

- Sub-functions are always run in the order in which they are given.  (Future
  versions of MLDB will run sub-functions in parallel as an optimization,
  but the effect will always be the same as if they had run in sequence).
- Each sub-function runs in its own context, with its own input values and
output values.
- `with` and `extract` expressions are used to copy values between
  the serial function and its sub-functions.
- `with` and `extract` expressions are normally of one of the forms
  - `*`: (only for extract; doesn't work for with): copy all input values
    and output values without changing their names.  This may work
    for simple functions but can lead to name conflicts on more complex
    functions.
  - `arg1[,...]` or `val1[,...]`: copy only the given input values or output
    values without changing their names
  - `arg1 AS name1[,...]` or `val1 AS name1[,...]`:
    copy the input values or output values with a different  name
  - `expr AS name[,...]`: copy input values or output values transforming them
     on their output.
- The `with` expressions are used to copy input values or output values from the outer
  context to a sub-function's context.  For example, if the outer function outputs an integer
  value named `userid`, and the sub-function requires a string input value named `usernum`,
  then the `with` expression might be `CAST (userid AS STRING) AS usernum`.
- The `extract` expressions are used to copy output values from the
  sub-function to the output.  For example, if the output value `prob`
  is needed to be available as the `user_conversion_percentage` output value on
  the output of the serial function, then the `extract` expression
  might be `prob * 100 AS user_conversion_percentage`.


The following example shows how two SQL expression functions can be
connected together.  Consider a serial function with the following
configuration:

```
{
    type: "serial",
    id: "test",
    params: {
        steps: [
            {
                type: "sql.expression",
                params: {
                    expression: "x + y AS z"
                },
                'with': "x, y",
                extract: "z"
            },
            {
                type: "sql.expression",
                params: {
                    expression: "p + q AS z"
                },
                'with': "z AS p, z AS q",
                extract: "z AS r"
            }
        ]
    }
};
```

In this function, the value named `z` is used inside the second sub-function,
but it is distinct from the value `z` returned by the first function, and is
renamed to `r` when extracted from the second function.

This serial function outputs all the values returned by the two functions, which
are `z` (from the first function) and `r` (from the second function).

The first sub-function calculates the value of \\(z = x + y\\), and the
second the value of \\(z = p + q\\).

Let's assume we run it with \\(x = 1\\) and \\(y = 2\\).  Then the
value of the first expression is \\(z = x + y = 1 + 2 = 3\\)/

The second expression calculates a different `z`, which we will call
\\(z'\\), as \\(z' = p + q\\).  Both \\(p\\) and \\(q\\) are set from
\\(z\\) from the output of the first function.  Thus, the value of the
second sub-function is \\(r = z' = p + q = z + z = 3 + 3 = 6\\), and
the output of applying the function with \\(x = 1\\) and \\(y = 2\\)
is:

    GET /v1/functions/<functionName>/applications?input={'x':1,'y':2}

```
{
   "output" : {
      "r" : 6,
      "z" : 3
   }
}
```

## Examples

* The ![](%%nblink _demos/Predicting Titanic Survival) demo notebook

## See Also

- [Functions] (Functions.md) for a description of functions
- [Function Configuration] (FunctionConfig.md) for a description of the kinds of functions and how to configure them.
