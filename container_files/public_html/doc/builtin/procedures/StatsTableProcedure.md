# Stats Table Procedure

This procedure does a rolling sum of keys and one or more outcomes over multiple columns. It is 
the result of creating statistical tables, one per column, to track co-occurrence of each
key with each outcome, over all rows in a table.

This procedure is related to the ![](%%doclink statsTable.bagOfWords.train procedure) but 
is different in the sense that the `statsTable.bagOfWords.train` procedure is meant to 
operate on sparse bags of words. This procedure expects a dense input dataset with a fixed
number of columns. Each column will have its own stats table. The keys used in the stats table
will be the value of each cell.

When using this procedure, a new output dataset will be created. It will have the same number
of rows as the input dataset. The counts in each row of the output dataset will represent 
the counts in the stats tables at that point in the input dataset while going
sequentially through it.

The resulting statistical tables can be persisted using the `statsTableFileUrl` parameter
and used later on to lookup counts using the ![](%%doclink statsTable.getCounts function).

## Configuration

![](%%config procedure statsTable.train)

## Example

Let's assume a dataset named *rt_bidding* made up of data from a real-time bidding online campaign. Each row
represents a bid request and each column represents a field in the bid request. We're interested
in tracking statistics for each possible values of each field to determine which ones are 
strongly correlated with the outcomes. The outcomes we want to track are whether there was a click 
on the ad and/or a purchase was then made on the advertiser's website.


|  rowName   |  host  |  region  | click\_on\_ad | purchase_value |
|----------|---|---|---|---|---|
| br_1     | patate.com  | qc | 1 | 0 |
| br_2     | carotte.net | on | 1 | 25 |
| br_3     | patate.com | on | 0 | 12 |
| br_4     | carotte.net | on | 0 | 0 |

Assume this partial configuration:

```javascript
{
    "trainingData": "SELECT * EXCLUDING(click_on_ad, purchase_value) FROM rt_bidding ORDER BY rowName()",
    "outcomes": [
        ["click", "click_on_ad = 1"],
        ["purchase", "click_on_ad = 1 AND purchase_value > 0"]
    ]
}
```

The output dataset will contain:

|  rowName   |  trial-host  |  click-host | purchase-host | trial-region  | click-region | purchase-region |
|----------|---|---|---|---|---|---|
| br_1     | 0  | 0 | 0 | 0 | 0 | 0 |
| br_2     | 0  | 0 | 0 | 0 | 0 | 0 |
| br_3     | 1  | 1 | 0 | 1 | 1 | 1 |
| br_4     | 1  | 1 | 1 | 2 | 1 | 1 |

Note that the effect of a row on the counts is taken into account after the counts
for that specific row are generated. This is done to prevent introducing bias as the
values for the current row would take into account the row's outcomes.

## See also
* The ![](%%doclink statsTable.getCounts function) does a lookup in stats tables for a row of keys.
* The ![](%%doclink experimental.statsTable.derivedColumnsGenerator procedure) can be used to generate derived columns from stats table counts.
* The ![](%%doclink statsTable.bagOfWords.train procedure) trains a stats table on bags of words.

