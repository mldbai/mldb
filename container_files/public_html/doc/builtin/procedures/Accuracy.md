# Classifier Testing Procedure

The classifier testing procedure allows the accuracy of a binary classifier, multi-class classifier or regressor  to be tested against held-out data. The output of this procedure is a dataset which contains the scores and statistics resulting from the application of a classifier to some input data.

## Configuration

![](%%config procedure classifier.test)

## Output

After this procedure has been run, a summary of the accuracy can be obtained via 

```python
mldb.get("/v1/procedures/<id>/runs/<runid>")
```

The `status` field will contain statistics relevant to the model's mode.

- <a href="#boolean">Boolean mode</a>
- <a href="#categorical">Categorical mode</a>
- <a href="#regression">Regression mode</a>
- <a href="#multilabel">Multi-label mode</a>

### <a name="boolean"></a>Boolean mode

The `status` field will contain the [Area
Under the Curve](http://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve) under the key `auc`, along with the performance statistics (e.g. [precision, recall](http://en.wikipedia.org/wiki/Precision_and_recall)) for the classifier using
the thresholds which give the best [MCC](http://en.wikipedia.org/wiki/Matthews_correlation_coefficient) and the best [F-score](http://en.wikipedia.org/wiki/F1_score), under the keys `bestMcc` and
`bestF1Score`, respectively.

Here is a sample output:

```python
{
  "status": {
    "bestMcc": {
      "pr": {
        "recall": 0.6712328767123288, 
        "precision": 0.8448275862068966, 
        "f1Score": 0.7480916030534351,
        "accuracy": 0.8196721311
      }, 
      "mcc": 0.6203113512927362, 
      "gain": 2.117855455833727, 
      "threshold": 0.6341791749000549, 
      "counts": {
        "falseNegatives": 24.0, 
        "truePositives": 49.0, 
        "trueNegatives": 101.0, 
        "falsePositives": 9.0
      }, 
      "population": {
        "included": 58.0, 
        "excluded": 125.0
      }
    }, 
    "auc": 0.8176836861768365, 
    "bestF1Score": {
      "pr": {
        "recall": 0.6712328767123288, 
        "precision": 0.8448275862068966, 
        "f1Score": 0.7480916030534351,
        "accuracy": 0.8196721311
      }, 
      "mcc": 0.6203113512927362, 
      "gain": 2.117855455833727, 
      "threshold": 0.6341791749000549, 
      "counts": {
        "falseNegatives": 24.0, 
        "truePositives": 49.0, 
        "trueNegatives": 101.0, 
        "falsePositives": 9.0
      }, 
      "population": {
        "included": 58.0, 
        "excluded": 125.0
      }
    }
  }, 
  "state": "finished"
}

```

The `output` dataset created by this procedure in `boolean` mode 
will contain one row per score by grouping together test set rows
with the same score. The dataset will have the following columns:

* `score`: the score the classifier assigned to this row
* `label`: the row's actual label
* `weight`: the row's assigned weight
* classifier attributes if this row's score was used as a binary threshold:
  * `falseNegatives`, `trueNegatives`, `falsePositives`, `truePositives`
  * `falsePositiveRate`, `truePositiveRate`, `precision`, `recall`, `accuracy`

Note that rows with the same score get grouped together.

### <a name="categorical"></a>Categorical mode

The `status` field will contain a sparse confusion matrix along with performance 
statistics (e.g. [precision, recall](http://en.wikipedia.org/wiki/Precision_and_recall)) 
for the classifier, where the label with the maximum score will be chosen as the
prediction for each example. 

The value of `support` is the number of occurrences 
for that label. The `weighted_statistics` represents the average of the 
per-label statistics, weighted by each label's support value. This excludes
the support value itself, for which we do the sum.

Here is a sample output:

```python
{
    "status": {
        "labelStatistics": {
            "0": {
                "f1Score": 0.8000000143051146,
                "recall": 1.0,
                "support": 2,
                "precision": 0.6666666865348816,
                "accuracy": 1.0
            },
            "1": {
                "f1Score": 0.0,
                "recall": 0.0,
                "support": 1,
                "precision": 0.0,
                "accuracy": 0.0
            },
            "2": {
                "f1Score": 1.0,
                "recall": 1.0,
                "support": 2,
                "precision": 1.0,
                "accuracy": 1.0
            }
        },
        "weightedStatistics": {
            "f1Score": 0.7200000057220459,
            "recall": 0.8,
            "support": 5,
            "precision": 0.6666666746139527,
            "accuracy": 0.8
        },
        "confusionMatrix": [
            {"predicted": "0", "actual": "1", "count": 1},
            {"predicted": "0", "actual": "0", "count": 2},
            {"predicted": "2", "actual": "2", "count": 2}
        ]
    },
    "state": "finished"
}
```

The `output` dataset created by this procedure in `categorical` mode 
will contain one row per input row, 
with the same row name as in the input, and the following columns:

* `label`: the row's actual label
* `weight`: the row's assigned weight
* `score.x`: the score the classifier assigned to this row for label `x`
* `maxLabel`: the label with the maximum score

### <a name="regression"></a>Regression mode

The `status` field will contain the following performance statistics:

- [mean squared error](https://en.wikipedia.org/wiki/Mean_squared_error) (MSE)
- [R squared score](https://en.wikipedia.org/wiki/Coefficient_of_determination)
- Quantiles of errors: More robust to outliers than MSE. Given \\( y_i \\) the true 
value and \\( \hat{y}_i \\) the predicted value, we return the 25th, 50th, 75th, and 90th
percentile of \\( | y_i - \hat{y}_i | / y_i \forall i \\). The 50th percentile is
the median and represents the *median absolute percentage* (MAPE).

Here is a sample output:

```python
{
    "status": {
        "quantileErrors": {
            "0.25": 0.0,
            "0.5": 0.1428571428571428,
            "0.75": 0.1666666666666667,
            "0.9": 0.1666666666666667
        },
        "mse": 0.375,
        "r2": 0.9699681653424412
    },
    "state": "finished"
}
```

The `output` dataset created by this procedure in `regression` mode 
will contain one row per input row, 
with the same row name as in the input, and the following columns:

* `label`: the row's actual value
* `score`: predicted value
* `weight`: the row's assigned weight

### <a name="multilabel"></a>Multi-label mode

The `status` field will contain the recall among top-N performance 
statistic (e.g. [recall](http://en.wikipedia.org/wiki/Precision_and_recall)) 
for the classifier, where the labels with the highest score will be chosen as the
predictions for each example. I.e, each example will register a positive for 
each of its label(s) that is found among the set of highest scoring labels returned by the classifier.
The size of this set is determined by the `recallOverN` parameter. It follows that a `1.0` recall rate cannot
be obtained if any example contains more unique labels than the value of the `recallOverN`
parameter.

The `weighted_statistics` represents the average of the 
per-label statistics.

Here is a sample output, with `recallOverN` set to `3`:

```python
{
    "status": {
                "weightedStatistics": {
                    "recall over top 3": 0.6666666666666666
                },
                "labelStatistics": {
                    "label0": {
                        "recall over top 3": 0.3333333333333333
                    },
                    "label1": {
                        "recall over top 3": 0.6666666666666666
                    },
                    "label2": {
                        "recall over top 3": 1.0
                    }
                }
            },
    "state": "finished"
}
```

The `output` dataset created by this procedure in `multilabel` mode 
will contain one row per input row, 
with the same row name as in the input, and the following columns:

* `label`: the row's actual label
* `weight`: the row's assigned weight
* `score.x`: the score the classifier assigned to this row for label `x`
* `maxLabel`: the label with the maximum score

## Examples

* Boolean mode: the ![](%%nblink _demos/Predicting Titanic Survival) demo notebook
* Categorical mode: the ![](%%nblink _tutorials/Procedures and Functions Tutorial) notebook

## See also

* The ![](%%doclink classifier.train procedure) trains a classifier.
* The ![](%%doclink classifier.test procedure) allows the accuracy of a predictor to be tested against
held-out data.
* The ![](%%doclink probabilizer.train procedure) trains a probabilizer.
* The ![](%%doclink classifier function) applies a classifier to a feature vector, producing a classification score.
* The ![](%%doclink classifier.explain function) explains how a classifier produced its output.
* The ![](%%doclink probabilizer function) works with classifier.apply to convert scores to probabilities.
