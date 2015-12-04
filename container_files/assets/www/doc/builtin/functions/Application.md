# Function Application via REST

All MLDB Functions are also automatically accessible as REST Endpoints, which can be used to apply machine learning models in a streaming/real-time process. Functions can be applied via a REST API call like `GET /v1/functions/<id>/application?input={<values>}`.

Let's look at a hypothetical Function with id `example` whose type defined the following input and output values:

* Input
  * `x`: integer
  * `y`: row of integers
* Output:
  * `scaled_y`: row of integers, each of which is the corresponding value of `y` times `x`
  * `sum_scaled_y`: integer, the sum of the values of `scaled_y`
  
We could apply this Function via REST like so:

```
GET /v1/functions/example/application?input={"x":2,"y":{"a":3,"b":4}}
```

And we would receive the following output:

```
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

## See also

* ![](%%nblink _tutorials/Procedures and Functions Tutorial) 
