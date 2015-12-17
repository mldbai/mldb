# Bag Of Words Stats Table Procedure

This procedure type is meant to work with bags of words, like the output of the
`tokenize` function. It creates a statistical table to track the co-occurrence of each
word with the specified outcome, over all rows in a table.

It is related to the ![](%%doclink statsTable.train procedure) but is different in the
sense that the `statsTable.train` procedure is meant to operate on a dense dataset composed of a
fixed number of columns. This procedure treats columns as words in a document, 
where a missing *word* for a document does not impact the counts.

The resulting statistical tables can be persisted using the `statsTableFileUrl` parameter
and used later on to lookup counts using the ![](%%doclink bagOfWordStatsTable.posneg function).

## Configuration

![](%%config procedure bagOfWordStatsTable.train)

## See also
* The ![](%%doclink statsTable.train procedure) is used to train stats tables on dense datasets

