# Bucketize Procedure

This procedure is used to assign rows from an input dataset to one of many percentile-based "buckets".
Based on an input dataset and order-by condition, it creates an output dataset with the same row 
names as the input and a single column called `bucket,` whose value is the name of the bucket to which
the corresponding input row has been assigned. The buckets cannot overlap, and are not required to cover
the entire 0-100 percentile range. Input rows which do not fit into any bucket will not have
corresponding rows in the output dataset.

## Configuration

![](%%config procedure bucketize)
