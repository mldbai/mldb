# Classifier Training Procedure

This procedure trains a classification model and stores the model file to disk.

## Configuration

![](%%config procedure classifier.train)

### Methods of configuring a classifier training

There are three ways of configuring which classifier will be trained:

1. Leave the `configuration` and `configurationFile` empty, and choose a standard algorithm configuration by name. See below for the contents of the default `configurationFile`).
2. Put the configuration inline in the `configuration` parameter (JSON) and set
  `algorithm` to either empty (if the configuration is at the top level) or to
  the dot separated path if it's not at the top level. See below for details on specifying your own `configuration`.
3. Put the configuration in an external resource identified by the `configurationFile`
  parameter, and set the algorithm as in number 2. See below for details on specifying your own `configurationFile`.


## Operation Modes

The `mode` field controls which mode the classifier will operate in:

- `boolean` mode will use a boolean label, and will predict the probability of
  the label being true as a single floating point number.
- `regression` mode will use a numeric label, and will predict the value of
  the label itself.
- `categorical` model will use a categorical (multi-class) label, and will
  predict the probability of each of the categories independently.  This
  style therefore produces multiple outputs.

## Training Weighting

This section describes how you can set different weights for each example in
your training set, either based upon the label or based upon a calculation over
the row, to enable finer control over which examples the classifier makes the
most effort to classify.

### Equalizing class weights

The `equalizationFactor` parameter can be used to adjust an unbalanced training
set to be more balanced for training, which frequently has the effect of
requiring the classifiers to focus more on separating the positive and negative
classes rather then getting really high scores for the dominant class.

* Setting this parameter to 0.0 (the default) weights the parameters according to
  the `weight` configuration expression.
* Setting this parameter to 1.0 will adjust the weights such that each class has
  exactly identical weight.

* Setting it to something else (0.5 is a good value for most unbalanced training
  set use cases) will multiply the weights of each class according to
  $$
  w_{class} \rightarrow w_{\textrm{class}} \times \left( \sum {w_{\textrm{class}}} \right) ^{-\textrm{equalizationFactor}}
  $$

### Setting example weight explicitly

The `weight` parameter of the configuration is used to set an SQL expression for
the weight of the example.  This is a positive number that implies how many
examples this counts for.  For example, a single row with a weight of 2, or the
same single row duplicated twice with a weight of 1 will have the same effect.

Note that only the relative weights matter.  Before the classifier is trained,
the weights will be normalized so that they sum to 1 to avoid numerical issues
in the classifier training process.

### Combining the two

If the two weighting methods are combined, then the `weight` expression will be
used to set the relative weight per example within its label class, and the
`equalizationFactor` will adjust the relative weight of each class.

## `configuration`/`configurationFile` Contents

A `configuration` JSON object or the contents of a `configurationFile` looks like this (see below for the contents of the default, overrideable `configurationFile`:

```json
{
    "algorithm_name": {
        "type": "classifier_type",
        "parameter": "value",
        ...
    },
    ...
}
```

The classifier training procedure includes support for the following types
of classifiers.

These classifiers tend to be high performance implementations of well known
classifiers which train and predict fast and are often a good default choice
when a generic classification step is required.

### Decision Trees (type=decision_tree)

![](%%jmlclassifier decision_tree)

### Generalized Linear Models (type=glz)

![](%%jmlclassifier glz)

### Bagging (type=bagging)

![](%%jmlclassifier bagging)

### Boosting (type=boosting)

![](%%jmlclassifier boosting)

### Neural Networks (type=perceptron)

![](%%jmlclassifier perceptron)

### Naive Bayes (type=naive_bayes)

![](%%jmlclassifier naive_bayes)



## Default `configurationFile`

The default, overrideable `configurationFile` contains the following:

```json
{

    "nn": { 
        "_note": "Neural Network",
        
        "type": "perceptron",
        "arch": 50,
        "verbosity": 3,
        "max_iter": 100,
        "learning_rate": 0.01,
        "batch_size": 10
    },


    "bbdt": {
        "_note": "Bagged boosted decision trees",
        
        "type": "bagging",
        "verbosity": 3,
        "weak_learner": {
            "type": "boosting",
            "verbosity": 3,
            "weak_learner": {
                "type": "decision_tree",
                "max_depth": 3,
                "verbosity": 0,
                "update_alg": "gentle",
                "random_feature_propn": 0.5
            },
            "min_iter": 5,
            "max_iter": 30
        },
        "num_bags": 5
    },

    "bbdt2": {
        "_note": "Bagged boosted decision trees",
        
        "type": "bagging",
        "verbosity": 1,
        "weak_learner": {
            "type": "boosting",
            "verbosity": 3,
            "weak_learner": {
                "type": "decision_tree",
                "max_depth": 5,
                "verbosity": 0,
                "update_alg": "gentle",
                "random_feature_propn": 0.8
            },
            "min_iter": 5,
            "max_iter": 10,
            "verbosity": 0
        },
        "num_bags": 32
    },

    "bbdt_d2": {
        "_note": "Bagged boosted decision trees",
        
        "type": "bagging",
        "verbosity": 3,
        "weak_learner": {
            "type": "boosting",
            "verbosity": 3,
            "weak_learner": {
                "type": "decision_tree",
                "max_depth": 2,
                "verbosity": 0,
                "update_alg": "gentle",
                "random_feature_propn": 1
            },
            "min_iter": 5,
            "max_iter": 30
        },
        "num_bags": 5
    },

    "bbdt_d5": {
        "_note": "Bagged boosted decision trees",
        
        "type": "bagging",
        "verbosity": 3,
        "weak_learner": {
            "type": "boosting",
            "verbosity": 3,
            "weak_learner": {
                "type": "decision_tree",
                "max_depth": 5,
                "verbosity": 0,
                "update_alg": "gentle",
                "random_feature_propn": 1
            },
            "min_iter": 5,
            "max_iter": 30
        },
        "num_bags": 5
    },

    "bdt": {
        "_note": "Bagged decision trees",
        
        "type": "bagging",
        "verbosity": 3,
        "weak_learner": {
            "type": "decision_tree",
            "verbosity": 0,
            "max_depth": 5
        },
        "num_bags": 20
    },

    "dt": {
        "_note": "Plain decision tree",
        
        "type": "decision_tree",
        "max_depth": 8,
        "verbosity": 3,
        "update_alg": "prob"
    },

    "glz": {
        "_note": "Generalized Linear Model.  Very smooth but needs very good features",

        "type": "glz",
        "verbosity": 3,
        "normalize ": " true",
        "ridge_regression ": " true"
    },

    "glz2": {
        "_note": "Generalized Linear Model.  Very smooth but needs very good features",

        "type": "glz",
        "verbosity": 3
    },

    "bglz": {
        "_note": "Bagged random GLZ",

        "type": "bagging",
        "verbosity": 1,
        "validation_split": 0.1,
        "weak_learner": {
            "type": "glz",
            "feature_proportion": 1.0,
            "verbosity": 0    
        },
        "num_bags": 32
    },


    "bs": {
        "_note": "Boosted stumps",

        "type": "boosted_stumps",
        "min_iter": 10,
        "max_iter": 200,
        "update_alg": "gentle",
        "verbosity": 3
    },

    "bs2": {
        "_note": "Boosted stumps",

        "type": "boosting",
        "verbosity": 3,
        "weak_learner": {
            "type": "decision_tree",
            "max_depth": 1,
            "verbosity": 0,
            "update_alg": "gentle"
        },
        "min_iter": 5,
        "max_iter": 300,
        "trace_training_acc": "true"
    },

    "bbs2": {
        "_note": "Bagged boosted stumps",

        "type": "bagging",
        "num_bags": 5,
        "weak_learner": {
            "type": "boosting",
            "verbosity": 3,
            "weak_learner": {
                "type": "decision_tree",
                "max_depth": 1,
                "verbosity": 0,
                "update_alg": "gentle"
            },
            "min_iter": 5,
            "max_iter": 300,
            "trace_training_acc": "true"
        }
    }

}
```
## Status

The status of a Classifier procedure training will return a JSON representation of the
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