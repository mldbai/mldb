# Bag Of Words Stats Table Pos/Neg

Functions of this type do a lookup in a stats table trained with the 
![](%%doclink statsTable.bagOfWords.train procedure) and returns 
the probability of outcome for each of the words provided as input.

## Configuration

![](%%config function statsTable.bagOfWords.posneg)

## Input and Output Values

Functions of this type have a single input value named `words` which is a row.

Functions of this type have a single output value named `probs` which is also a row. 
The row will contain the probability of the P(outcome | word) for each valid word 
given the function configuration.

## See also
* The ![](%%doclink statsTable.bagOfWords.train procedure) trains a bag of words stats table.

