# Stats Table Get Counts Function

Functions of this type do a lookup in a stats table trained with the ![](%%doclink statsTable.train procedure) and returns 
the counts for each of the keys provided as input.

## Configuration

![](%%config function statsTable.getCounts)

## Input and Output Values

Functions of this type have a single input value named `keys` which is a row.

Functions of this type have a single output value named `counts` which is also a row. 
The row will contain two columns for each key, one containing the number of elements 
for the key and one containing the number of labels for that key. For example, for 
 keys in the `host` and `region` stats tables, a row of this form would be returned:

|  *rowName*   |  *imp-host*  |  *label-host* | *imp-region*  | *label-region* |
|----------|---|---|---|---|
| row_1     | 25  | 1 | 50 | 0 |


## See also
* The ![](%%doclink statsTable.train procedure) trains stats tables.
* The ![](%%doclink experimental.statsTable.derivedColumnsGenerator procedure) can be used to generate derived columns from stats table counts.

