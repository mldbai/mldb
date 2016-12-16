# Image Wrapper Function

The Read Pixels Function is used to provide indexed access to an embedding over 2 dimensions.

## Configuration

![](%%config function image.readpixels)

## Example

The following function configuration will provide indexed access over the 2x2 embedding specified.

```python
{
    "id": "expr",
    "type": "image.wrapper",
    "params": {
        "expression": "[[[1],[2]],[[3],[4]]]"
    }
}
```

The expression `expr(0,1)` will return the value `2` from the embedding.

## See also

* [MLDB's SQL Implementation](../sql/Sql.md)
* the ![](%%doclink sql.query function) runs an SQL query against a
  dataset when it is called.
