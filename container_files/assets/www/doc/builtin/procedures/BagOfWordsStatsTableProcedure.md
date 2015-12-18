# Bag Of Words Stats Table Procedure

This procedure type is meant to work with bags of words, as returned by the
`tokenize` function. It creates a statistical table to track the co-occurrence of each
word with the specified outcome, over all rows in a table.

It is related to the ![](%%doclink statsTable.train procedure) but is different in the
sense that the `statsTable.train` procedure is meant to operate on a dense dataset composed of a
fixed number of columns where each column will have its own stats table. This procedure 
treats columns as words in a document and trains a single stats table for all words.

The resulting statistical table can be persisted using the `statsTableFileUrl` parameter
and used later on to lookup counts using the ![](%%doclink statsTable.bagOfWords.posneg function).

## Configuration

![](%%config procedure statsTable.bagOfWords.train)

## Example

Suppose we have the following dataset called `text_dataset`:

|  *rowName*   |  *text*  |  *label*  |
|-------------|---|---|
| a     | i like apples  | 1 |
| b     | i like juice |
| c     | what about bananas? | 
| d     | apples are red | 1 |
| e     | bananas are yellow |
| f     | oranges are ... orange | 

If we run the following procedure:

    PUT /v1/procedures/my_st
    {
        "type": "statsTable.bagOfWords.train",
        "params": {
            "trainingDataset": "text_dataset",
            "select": "tokenize(text, {splitchars: ' '}) as *",
            "outcomes": [["label", "label IS NOT NULL"]],
            "statsTableFileUrl": "file://my_st.st",
        }
    }

Below are some examples of the values in the resulting stats table:

| *word* | *P(label)* |
|--------|-------------------|
| apples | 1 |
| are | 0.333 |
| i | 0.5 |
| oranges | 0 |


## See also
* The ![](%%doclink statsTable.bagOfWords.posneg function) is used to do lookups in a stats table trained by this procedure and returning the matching top negative or positive words.
* The ![](%%doclink statsTable.train procedure) is used to train stats tables on dense datasets

