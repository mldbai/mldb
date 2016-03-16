# Output Dataset Specifications

When a procedure writes its output to a dataset, the dataset that is to be created can be specified in one of two ways:

* by id, for example the string `"outputDs"` will cause the procedure to create a new dataset with the id "outputDs" (overwriting any pre-existing dataset with that id) and the specified default type.
* by a [Dataset Configuration](../datasets/DatasetConfig.md), for example the object `{"id":"outputDs"}` will cause a dataset with id "outputDs" to be created (overwriting any pre-existing dataset with that id), and the `{"id":"outputDs2","type":"t"}` will cause a dataset with id "outputDs2" to be created with type "t". Note: if specifying a type, the type must be mutable, otherwise the procedure will not be able to write its output to the dataset.

## Optional Output Datasets

If an output dataset is marked "(optional)" then the output dataset can remain unspecified, and the procedure will run without generating that particular output dataset. This can cause certain procedures to run much faster, as they can skip potentially-expensive processing.
