# Classifier Apply Function

This function applies a classifier model previously trained by a ![](%%doclink classifier.train procedure) to a feature vector, producing a classification
score.

## Configuration

![](%%config function classifier)


## Input and Output Values

Functions of this type have a single input value called `features` which is a row. The columns that
are expected in this row depend on the features that were trained into the classifier.
For example, if in the training the input was `"select": "x,y"`, then the function will 
expect two columns called `x` and `y`.

These functions output a single value where the name depends on the classifier mode:

* For a classifier in `regression` mode, the name is `score`
  and it gives the output of the regression.
* For a classifier in `boolean` mode, the name is `score`
  and it gives the classifier's score.  Depending on the classifier type,
  this could have any range, but normally the higher the score, the more
  likely the "true" response.  A ![](%%doclink probabilizer.train procedure) can be used to transform the output into a probability.
* For a classifier in `categorical` mode, the name is `scores`
  and it outputs a row of values with one column for each category, the value of which
  is the score the classifier has assigned to that category.  Again, the
  higher the score, the more likely that the category is true, and a
  ![](%%doclink probabilizer.train procedure) can be used.

## Status

The status of a Classifier Apply function will return a JSON representation of the
model parameters of the trained classifier, to allow introspection.

## Examples

* The ![](%%nblink _demos/Predicting Titanic Survival) demo notebook

## See also

* The ![](%%doclink classifier.train procedure) trains a classifier.
* The ![](%%doclink classifier.test procedure) allows the accuracy of a predictor to be tested against
held-out data.
* The ![](%%doclink probabilizer.train procedure) trains a probabilizer.
* The ![](%%doclink classifier function) applies a classifier to a feature vector, producing a classification score.
* The ![](%%doclink classifier.explain function) explains how a classifier produced its output.
* The ![](%%doclink probabilizer function) works with classifier.apply to convert scores to probabilities.
