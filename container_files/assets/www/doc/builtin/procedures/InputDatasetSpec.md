# Input Dataset Specifications

When a procedure accepts a dataset as input, the dataset can be specified in one of two ways:

* by an [SQL From Expression](../sql/FromExpression.md), for example the string `"ds"` will refer to the pre-existing dataset with id "ds", and the string `"ds1 join ds2"` will refer to the full outer join of the pre-existing datasets with ids "ds1" and "ds2".
* by a [Dataset Configuration](../datasets/DatasetConfig.md), for example the object `{"id":"ds"}` will refer to the pre-existing dataset with id "ds", and the `{"id":"ds1","type":"t"}` will cause a dataset with id "ds1" to be created with type "t".

