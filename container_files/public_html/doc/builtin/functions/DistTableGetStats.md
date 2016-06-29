# Dist Table Get Stats Function

Functions of this type do a lookup in a statistical table trained with the ![](%%doclink distTable.train procedure) and returns 
the stats (coui for each of the keys provided as input.

## Configuration

![](%%config function distTable.getStats)

## Input and Output Values

Functions of this type have a single input value named `features` which is a row.

Functions of this type have a single output value named `stats` which is also a row. 
The row will contain a column for each outcome/feature/statistic combination.

For exemple, if we take the same exemple as in the ![](%%doclink distTable.train procedure),
the returned row using `host='patate.com'` and `region='on'` would look like this:

| rowName | stats.price.host.count | stats.price.host.avg | stats.price.host.std | ... | stats.price.region.std | stats.price.region.min | stats.price.region.max |
|--|--|--|--|--|--|--|--|
| result | 2 | 2 | 1.4142 | ... | 1 | 1 | 3 |


## See also
* The ![](%%doclink distTable.train procedure) to train statistical tables.

