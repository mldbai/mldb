# Classifier Training Procedure

This procedure trains a classification model and stores the model file to disk.

## Configuration

![](%%config procedure classifier.train)

### Algorithm configuration

This procedures supports many training algorithm. The configuration is explained
on the [classifier configuration](../ClassifierConf.md) page.


## Status Output

The status of a Classifier procedure training will return a JSON representation of the
model parameters of the trained classifier, to allow introspection.

## Operation Modes

The `mode` field controls which mode the classifier will operate in:

- `boolean` mode will use a boolean label, and will predict the probability of
  the label being true as a single floating point number.
- `regression` mode will use a numeric label, and will predict the value of
  the label itself.
- `categorical` mode will use a categorical (multi-class) label, and will
  predict the probability of each of the categories independently.  This
  style therefore produces multiple outputs.
- `multilabel` mode will do [multi-label classification](https://en.wikipedia.org/wiki/Multi-label_classification) 
  by using a set of categorical (multi-class) labels, and will
  predict the probability of each of the categories independently.  This
  style therefore produces multiple outputs. The `multilabelStrategy` field
  controls how multilabel classification is handled.

## Multilabel classification

In all operation modes but `multilabel`, the label is a single scalar value. The `multilabel` handles
categorial classification problems where each example has a set of labels instead of a single one.
To this end the `label` input must be a row. In this row each column with a non-null value will be a
label value in the example's set. The column name is used to identify the label, while the value itself is disregarded.
This makes multi-label classification easy to use with bag of words, for example.

## Examples

* The ![](%%nblink _demos/Predicting Titanic Survival) demo notebook

## See also

* The ![](%%doclink classifier.test procedure) allows the accuracy of a predictor to be tested against
held-out data.
* The ![](%%doclink probabilizer.train procedure) trains a probabilizer.
* The ![](%%doclink classifier function) applies a classifier to a feature vector, producing a classification score.
* The ![](%%doclink classifier.explain function) explains how a classifier produced its output.
* The ![](%%doclink probabilizer function) works with classifier.apply to convert scores to probabilities.
