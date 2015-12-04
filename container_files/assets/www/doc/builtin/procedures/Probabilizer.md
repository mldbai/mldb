# Probabilizer Training Procedure

This procedure
trains a probabilizer model and stores the model file to disk.

The probabilizer plays the role of transforming the output of a classifier,
which may have any range, into a calibrated pseudo-probability that can
be used for value calculations.  It is used for five main purposes:

1.  When there are multiple classifiers, or multiple types of classifiers,
    who need their output to be comparable.  For example, if you want to
    compare the output of a decision tree with probabilistic buckets with
    the output of a neural network with a softmax activation function, you
    need to transform them into comparable quantities (ie, probabilities).
    The probabilizer allows you to do this.
2.  When you have trained a classifier that outputs something other than a
    probability, and you need to turn this into a probability.
3.  When you need to set thresholds that are independent of the type of
    classifier used.  For example, if you are implementing business logic
    that says that only applications with a 70% probability of being
    fraudulent may be skipped, you need a way of turning the "0.1033" from
    the classifier into "71.2% probably of being fraudulent".
4.  When you have trained a classifier on biased data, for example by sampling or
    weighting the positive and negative examples differently, and you need
    to correct for this bias.  As an example, imagine that you have 1 million
    examples of browsing sessions that didn't result in the purchase of a product,
    and 10,000 that did.  You may choose to train a classifier on the 10,000
    positive examples but sample 20,000 of the negative examples.  In this
    dataset, the purchase prior is 33% but in the real data, it's around 1%.  The
    probabilizer can help correct for the bias.
5.  When a classifier is frequently retrained, but the output from one
    training to the next needs to be consistent (for example, it feeds
    into the same business logic or a subsequent processing step that
    is retrained less frequently).

The probabilizer.train procedure allows for a probabilizer to be trained.

## Algorithm

The probabilizer training uses a generalized linear model to learn a monotonic
transformation of the output of the classifier onto a probability space.

## Configuration

![](%%config procedure probabilizer.train)

A probabilizer is trained on the output of a classifier applied over a
dataset.  The dataset should *not* have been used to train the classifier,
or a biased probabilizer will result.

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