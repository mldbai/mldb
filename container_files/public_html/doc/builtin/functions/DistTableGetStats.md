# Distribution Table Get Statistics Function

Functions of this type do a lookup in a statistical table trained using the
![](%%doclink experimental.distTable.train procedure) and return the statistics (count, avg, std, min, max)
for each of the features provided as input.

## Configuration

![](%%config function experimental.distTable.getStats)

## Input and Output Values

Functions of this type have a single input value named `features` which is a row.

Functions of this type have a single output value named `stats` which is also a row. 
The row will contain a column for each outcome/feature/statistic combination.

For example, if we take the same example as in the ![](%%doclink experimental.distTable.train procedure),
the returned row, using for instance `host='patate.com'` and `region='on'` as features, would look like this:

| rowName | stats.price.host.count | stats.price.host.avg | stats.price.host.std | ... | stats.price.region.std | stats.price.region.min | stats.price.region.max |
|---------|---|---|---|---|---|---|---|
| result  | 2 | 2 | 1.4142 | ... | 1 | 1 | 3 |


## See also
* The ![](%%doclink experimental.distTable.train procedure) to train statistical tables.

