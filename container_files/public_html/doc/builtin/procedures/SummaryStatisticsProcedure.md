# Summary Statistics Procedure

This procedure generates statistics as rows for every column of the input
dataset. For numeric columns, the statistics are
* min
* max
* mean
* number of uniques
* number of nulls
* 1st quartile
* median
* 3rd quartile
* most frequent item

Mixed or non numeric columns are treated as categorical and the statistics are
* number of uniques
* number of nulls
* most frequent item


## Configuration

![](%%config procedure summary.statistics)
