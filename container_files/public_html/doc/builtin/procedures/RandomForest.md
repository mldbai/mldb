# Classifier Training Procedure

This procedure trains a binary random forest classifier model and stores the model file.

This procedure is a variant of the generic bagged decision tree classifier (see ![](%%doclink classifier.train procedure)) that has been
optimized for binary classification on dense, tabular data and forest of trees.

## Configuration

![](%%config procedure randomforest.binary.train)

## Input data

This classification procedure will work most efficiently on datasets that have their data well-organized by column, such
as the Tabular dataset.

This optimized version only support dense values, with all training samples containing no null values.

It only supports binary classification. The generic classifier.train procedure supports regression and multi-class classification.

Feature values can be numeric or strings. Strictly numeric features will be considered as ordinal, while feature that contains only 
strings or a mix of strings and numeric values will be considered as nominal. Other value types (blobs, timestamps, intervals, etc)
are not yet supported.

## Output model

The resulting model is a .cls classifier model that is compatible with the classifier function and the classifier.test procedure.

* The ![](%%doclink classifier.train procedure) trains a classifier.
* The ![](%%doclink classifier.test procedure) allows the accuracy of a predictor to be tested against
held-out data.
* The ![](%%doclink classifier function) applies a classifier to a feature vector, producing a classification score.