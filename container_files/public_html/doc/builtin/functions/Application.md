# Function Application via REST

All MLDB user-defined Functions (including machine learning models created by [Procedures](../procedures/Procedures.md)) are not only available within [SQL queries](../sql/ValueExpression.md.html#CallingFunctions) also automatically accessible as REST Endpoints, which can be used to apply machine learning models in a streaming/real-time process. Functions support input values passed in as query string parameters or as a JSON payload. In both cases, the REST call must be a GET. For example, functions can be applied with query string parameters via a REST API call like `GET /v1/functions/<id>/application?input={<values>}`.

### Example

Let's look at a hypothetical Function with id `example` whose type defined the following input and output values:

* Input
  * `x`: integer
  * `y`: row of integers
* Output:
  * `scaled_y`: row of integers, each of which is the corresponding value of `y` times `x`
  * `sum_scaled_y`: integer, the sum of the values of `scaled_y`

We could apply this Function via REST like so:

```python
mldb.get("/v1/functions/example/application", input={"x":2,"y":{"a":3,"b":4}})
```

And we would receive the following output:

```python
{
    "output": {
        "sum_scaled_y": 14,
        "scaled_y": [
            [ "a", [ 6, "-Inf" ] ], 
            [ "b", [ 8, "-Inf" ] ]
        ]
    }
}
```

The same output can be obtained by providing the arguments using a JSON payload:

```python
mldb.get("/v1/functions/example/application", data={"input": {"x":2,"y":{"a":3,"b":4}}})
```

## See also

* ![](%%nblink _tutorials/Procedures and Functions Tutorial) 
