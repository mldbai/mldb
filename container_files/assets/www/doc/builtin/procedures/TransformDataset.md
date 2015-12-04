# Transform Dataset Procedure

This procedure runs an [SQL query](../sql/Sql.md) on a dataset, and records the
output in another dataset.  It is frequently used to reduce, reshape and
reindex datasets.

It is particularly useful in order to generate a training dataset for
machine learning algorithms, which require a pre-indexed dataset with
all of the features in place.

## Configuration

![](%%config procedure transform)

## Examples

* The ![](%%nblink _demos/Predicting Titanic Survival) demo notebook

## See also

* [MLDB's SQL Implementation](../sql/Sql.md)
* the ![](%%doclink merged dataset) and ![](%%doclink transposed dataset) can be used in combination with a transform dataset procedure to implement joins on datasets.
