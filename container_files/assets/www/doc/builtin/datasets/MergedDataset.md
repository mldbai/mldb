# Merged Dataset

The merged dataset allows for rows from multiple datasets to be merged into a
single dataset.  Rows and columns that match up between the datasets will be
combined together.

The merge is done on the fly (only an index is created of rows and columns
in the merged dataset), which means it is relatively rapid to merge even
large datasets together.

In SQL, it's similar to an outer join:

```
SELECT ds1.*, ds2.* FROM ds1 OUTER JOIN ds2 ON ds1.rowName = ds2.rowName
```

## Configuration

![](%%config dataset merged)

## Examples

* The ![](%%nblink _demos/Mapping Reddit) demo notebook

## See Also

* The ![](%%doclink transposed dataset) is another dataset transformation
* The ![](%%doclink transform procedure) can be used to modify a dataset ready for merging
