# SQL Expression Function

The SQL expression function type is used to create functions over an [SQL Expression](../sql/Sql.md).
A function of this type allows simple calculations on the function's input values and it is preferred
over creating custom function types when possible.

## Configuration

![](%%config function sql.expression)

## Example

The following function configuration will output a value `z` on a
that is the sum of the `x` and `y` input values:

```
{
    id: "expr",
    type: "sql.expression",
    params: {
        expression: "x + y AS z"
    }
}
```

## See also

* [MLDB's SQL Implementation](../sql/Sql.md)
* the ![](%%doclink sql.query function) runs an SQL query against a
  dataset when it is called.
