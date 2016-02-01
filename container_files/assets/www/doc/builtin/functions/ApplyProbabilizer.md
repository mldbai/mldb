# Apply Probabilizer function

This function applies a probabilizer model previously trained by a ![](%%doclink probabilizer.train procedure), converting a score to a probability.

## Input and Output Values

It has one real-valued input value named `score`, usually the output of a ![](%%doclink classifier function).
It outputs one value named `prob` which is a real value between 0 and 1 indicating
the probability associated with the input score.

## Configuration

![](%%config function probabilizer)

## See also

* The ![](%%doclink classifier.train procedure) trains a classifier.
* The ![](%%doclink classifier.test procedure) allows the accuracy of a predictor to be tested against
held-out data.
* The ![](%%doclink probabilizer.train procedure) trains a probabilizer.
* The ![](%%doclink classifier function) applies a classifier to a feature vector, producing a classification score.
* The ![](%%doclink classifier.explain function) explains how a classifier produced its output.
* The ![](%%doclink probabilizer function) works with classifier.apply to convert scores to probabilities.
