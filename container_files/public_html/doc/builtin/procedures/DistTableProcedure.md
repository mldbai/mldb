# Dist Table Procedure

For a combination of feature columns (the `training_data` parameter) and outcomes, this procedure
will iteratively (over the rows) calculate different statistics (count, avg, std, min, max) on
the outcomes, for each values of the feature columns. 

The feature columns are assumed to be string-valued, and the outcome numeric-valued (type `INTEGER` or `NUMBER`). 

This procedure can be viewed as a ![](%%doclink statsTable.train procedure) where the
labels are numbers instead of booleans.

You can optionally specity an output dataset that will contain the computed rolling statistics for each feature column / outcome
combination. Since we are doing it iteratively, it means that the statistics for row `n` will only use the `n-1` preceding rows.

The resulting statistical tables can be persisted using the `distTableFileUrl` parameter
and used later on to lookup counts using the ![](%%doclink distTable.getstats function).

## Configuration

![](%%config procedure distTable.train)

## Example

Let's assume a dataset made up of data from a real-time bidding online campaign. Each row
represents a bid request and each column represents a field in the bid request. We're interested
in tracking statistics on each outcome, given some feature columns. The outcome we will be
interesed in here is the `purchase_value`, and the features will be the `host` and the `region`.


|  rowName   |  host  |  region  | click\_on\_ad | purchase_value |
|----------|---|---|---|---|---|
| br_1     | patate.com  | qc | 1 | 0 |
| br_2     | carotte.net | on | 1 | 25 |
| br_3     | patate.com | on | 0 | 12 |
| br_4     | carotte.net | on | 0 | 0 |


Assume this partial configuration:

```javascript
{
    "trainingData": "* EXCLUDING(purchase_value)",
    "outcomes": [
        ["price", "purchase_value"]
    ]
}
```

Here is an excerpt of the statistics table (as saved in `distTableFileUrl`)

| outcome | column | value | stat | stat\_value |
|---------|---|---|---|---|
| price  | host | patate.com | count | 2 |
| price  | host | patate.com | avg | 2 |
| price  | host | patate.com | std | 1.4142 |
| price  | ... | ... | ... | ... |
| price  | region | on | std | 1 |
| price  | region | on | min | 1 |
| price  | region | on | max | 3 |


The iteratively calculated dataset would look like this:

| rowName | price.host.count | price.host.avg | price.host.std | ... | price.region.std | price.region.min | price.max |
|---------|---|---|---|---|---|---|---|
| br\_1   | 0 | NaN | NaN | ... | NaN | NaN | NaN |
| br\_2   | 0 | NaN | NaN | ... | NaN | 1 | 1 |
| br\_3   | 1 | 1 | NaN | ... | 1.4142 | 1 | 2 |
| br\_4   | 1 | 2 | NaN | ... | NaN | NaN | NaN |

Now why is this dataset useful? Imagine we used this procedure on a series of
bid requests sorted in time. In the resulting dataset, each row will correspond
to one of the bid requests, and the columns will be the different statistics on the
outcomes, using only the previous bid requests with corresponding attributes. 
From a machine learning perspective, this generates training examples that
are unbiased with regards to the new information provided by this bid request
after it has been bid on.

## See also
* The ![](%%doclink distTable.getStats function) does a lookup in stats tables for an input row.
