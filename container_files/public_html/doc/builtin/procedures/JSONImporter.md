# JSON Import Procedure

The JSON Import Procedure type is used to import a text file containing
one JSON per line in a dataset.

This procedure will process lines using the [parse_json](../sql/ValueExpression.md.html) builtin function with `arrays` set to `'encode'`.


## Configuration

![](%%config procedure import.json)


## See also

* The [parse_json](../sql/ValueExpression.md.html#parse_json) builtin function can apply the above
JSON unpacking algorithm to a text cell in an SQL query
* The ![](%%doclink import.text procedure) is used to import text files
* The ![](%%doclink melt procedure) is used to melt columns into many rows. This is useful
when dealing with a JSON array of objects

