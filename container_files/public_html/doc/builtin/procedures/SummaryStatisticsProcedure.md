# Summary Statistics Procedure

This procedure generates [summary statistics](https://en.wikipedia.org/wiki/Summary_statistics) 
for every column of the input dataset. Each column from the `inputData` 
will be represented as a row in the `outputDataset`.

### Statistics for numeric columns

* minimum and maximum values
* mean
* 1st quartile, median, and 3rd quartile
* number of unique values
* number of null values
* most frequent items

### Statistics for categorical columns

Mixed or non numeric columns are treated as categorical and the statistics are:

* number of unique values
* number of null values
* most frequent items

## Configuration

![](%%config procedure summary.statistics)

## Examples

* The ![](%%nblink _demos/Predicting Titanic Survival) demo notebook

