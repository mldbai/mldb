# Stats Table Derived Columns Function Generator Procedure (Experimental)

This procedure is used with stats tables and makes the process of 
specifying the SQL expression
representing the derived columns over the stats tables' counts easier.
This is acheived by allowing the specification of an expression template
that will be expanded for all stats tables.

## Configuration

![](%%config procedure experimental.statsTable.derivedColumnsGenerator)

## Template rules

In the following, `TBL` represents the current stat table's name.

- `trial` will be replaced by `trial-TBL`
- `outcome` will be replaced by `outcome-TLB`, where outcome represents one of the outcomes the stats table was trained with
- `$tbl`, meaning *table*, will be replace by `TBL`. This is useful for specifying column aliases

## Example

We will reuse the same example as in the ![](%%doclink statsTable.train procedure), which 
is using an online ad campaign. Below are the stats table counts:

|  *rowName*   |  *trial-host*  |  *click-host* | *purchase-host* | *trial-region*  | *click-region* | *purchase-region* |
|----------|---|---|---|---|---|---|
| br_1     | 0  | 0 | 0 | 0 | 0 | 0 |
| br_2     | 0  | 0 | 0 | 0 | 0 | 0 |
| br_3     | 1  | 0 | 1 | 1 | 1 | 0 |
| br_4     | 1  | 0 | 1 | 2 | 1 | 1 |

One derived column that is useful is the ratio of clicks to impressions, or click-through ratio (CTR).
The SQL expression to represent this for the `host` stats table is `"click-host"/"trial-host"`. In real-life
situations, we have many stats table to consider so we will provide a template for the procedure to expand.

The following expression:

```
"click"/"trial" as "ctr_$tbl", ln("purchase"+1) as "logPurchase_$tbl"
```

will produce this expanded expression for the dataset above:

```
"click-host"/"trial-host" as "ctr_host", 
log("purchase-host"+1) as "logPurchase_host", 
"click-region"/"trial-region" as "ctr_region",
log("purchase-host"+1) as "logPurchase_host"
```

As a best practice, it is recommended to always put quotes around columns when specifying expressions.

Applying the expanded expression to the `br_4` row will produce the following output:

| *rowName* | *ctr_host* | *logPurchase_host* | *ctr_region* | *logPurchase_region* |
|-----------|------------|--------------------|--------------|--------------------|
| br_4 | 0 | 0.3 | 0.5 | 0.3 |


## See also
* The ![](%%doclink statsTable.train procedure) trains stats tables.
* The ![](%%doclink statsTable.getCounts function) does a lookup in stats tables for a row of keys.

