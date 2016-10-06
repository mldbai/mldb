# Proximate Voxels Function

The Proximate Voxels Function is used to return all values in a cubic sub-volume of a particular embedding.

## Configuration

![](%%config function image.proximatevoxels)

## Example

The following function configuration will provide access to all values inside a 3x3x3 subvolume inside
the specified 4x4x4 3-dimension embedding.

```python
{
    "id": "expr",
    "type": "image.proximatevoxels",
    "params": {
        "expression": "[[[1,2,3,4],[5,6,7,8],[9,10,11,12],[13,14,15,16]],
                        [[17,18,19,20],[21,22,23,24],[25,26,27,28],[29,30,31,32]],
                        [[33,34,35,36],[37,38,39,40],[41,42,43,44],[45,46,47,48]],
                        [[49,50,51,52],[53,54,55,56],[57,58,59,60],[61,62,63,64]]                      
                        ]",
        "range": 1
    }
}
```

The expression `expr(1,1,1)` will return the subvolume 
`[[[1,2,3,4],[5,6,7,8],[9,10,11,12]],
  [[17,18,19,20],[21,22,23,24],[25,26,27,28]],
  [[33,34,35,36],[37,38,39,40],[41,42,43,44]]]` 

from the embedding.

## See also

* [MLDB's SQL Implementation](../sql/Sql.md)
* the ![](%%doclink sql.query function) runs an SQL query against a
  dataset when it is called.
