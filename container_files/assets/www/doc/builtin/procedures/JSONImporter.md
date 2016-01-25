# JSON Import Procedure

The JSON Import Procedure type is used to import a text file containing
one JSON per line in a dataset.

This procedure will use the same algorithm as the 
[unpack_json](../sql/ValueExpression.md.html#unpack_json) builtin function
to store the data.


## Configuration

![](%%config procedure import.json)


## See also

* The [unpack_json](../sql/ValueExpression.md.html#unpack_json) builtin function can apply the above
JSON unpacking algorithm to a text cell in an SQL query
* The ![](%%doclink text.csv.tabular dataset) is used to import a CSV file
* The ![](%%doclink melt procedure) is used to melt columns into many rows. This is useful
when dealing with a JSON array of objects

