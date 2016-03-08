# Classifier Explain Function

This function explains how a classifier  model previously trained by a ![](%%doclink classifier.train procedure) produced its output, allowing for
feedback on which features were causing the classifier result.

## Configuration

![](%%config function classifier.explain)

## Input and Output Values

Functions of this type have two input values: `label` which is an integer and `features` which is a row. The value of `label` should be compatible with the labels used to train the classifier. The columns that
are expected in the `feature` row depend on the features that were trained into the classifier.
For example, if in the training the input was `"select": "x,y"`, then the function will 
expect two columns called `x` and `y`.

These functions output two values, `explanation` which returns a row and `bias` which returns a float.
The `explanation` row contains a column for each feature which influenced the classifier's output for this
input, the value of which is a numerical score for the influence of that feature in this case.
The sum of the value of the `bias` column and the influence of each feature is the score that
the classifier assigns to the input feature vector. This means that the higher the influence
assigned to a given value of a feature (in the context of the others), the more that feature
contributes to the score, and the heavier that feature weighs in favour of the `label`.
Negative values indicate that the feature value in question pushes the classifier away from assigning
the `label`. The `bias` value is the score the classifier would assign in the absence of any 
features.


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
