# Classifier Testing Procedure

The classifier testing procedure allows the accuracy of a binary classifier, multi-class classifier or
regressor  to be tested against
held-out data. The output of this procedure is a dataset which contains the scores and statistics
resulting from the application of a classifier to some input data.

## Configuration

![](%%config procedure classifier.test)

## Output

After this procedure has been run, a summary of the accuracy can be obtained via 
`GET /v1/procedures/<id>/runs/<runid>`. The the `status` field will contain statistics relevant
to the model's mode.

- <a href="#boolean">Boolean mode</a>
- <a href="#categorical">Categorical mode</a>
- <a href="#regression">Regression mode</a>

### <a name="boolean"></a>Boolean mode

The `status` field will contain the [Area
Under the Curve](http://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve) under the key `auc`, along with the performance statistics (e.g. [precision, recall](http://en.wikipedia.org/wiki/Precision_and_recall)) for the classifier using
the thresholds which give the best [MCC](http://en.wikipedia.org/wiki/Matthews_correlation_coefficient) and the best [F-score](http://en.wikipedia.org/wiki/F1_score), under the keys `bestMcc` and
`bestF`, respectively.

Here is a sample output:

```
GET http://localhost/v1/procedures/ttnc_test_scorer/runs/1

{
  "status": {
    "bestMcc": {
      "pr": {
        "recall": 0.6712328767123288, 
        "precision": 0.8448275862068966, 
        "f": 0.7480916030534351
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
    "bestF": {
      "pr": {
        "recall": 0.6712328767123288, 
        "precision": 0.8448275862068966, 
        "f": 0.7480916030534351
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
  "state": "finished", 
  "runFinished": "2015-06-05T18:06:19.165Z", 
  "runStarted": "2015-06-05T18:06:19.119Z", 
  "id": "2015-06-05T18:06:19.118784Z-5bc7042b732cb41f"
}

```

The `output` dataset created by this procedure in `boolean` mode 
will contain one row per input row, 
with the same row name as in the input, and the following columns:

* `score`: the score the classifier assigned to this row
* `label`: the row's actual label
* `weight`: the row's assigned weight
* classifier attributes if this row's score was used as a binary threshold:
  * `falseNegatives`, `trueNegatives`, `falsePositives`, `truePositives`
  * `falsePositiveRate`, `truePositiveRate`, `precision`, `recall`

Note that rows with the same score get grouped together.

### <a name="categorical"></a>Categorical mode

The `status` field will contain a sparse confusion matrix along with performance 
statistics (e.g. [precision, recall](http://en.wikipedia.org/wiki/Precision_and_recall)) 
for the classifier, where the label with the maximum score will be chosen as the
prediction for each example. 

The value of `support` is the number of true positives
for that label. The `weighted_statistics` represents the average of the 
per-label statistics, weighted by each label's support value.

The `confusion_matrix` is indexed by true label and then by predicted label.

Here is a sample output:

```
GET http://localhost/v1/procedures/ttnc_test_scorer/runs/1

{
    "status": {
        "label_statistics": {
            "1": {
                "f1_score": 0.0,
                "recall": 0.0,
                "support": 1,
                "precision": 0.0
            },
            "0": {
                "f1_score": 0.8000000143051146,
                "recall": 1.0,
                "support": 2,
                "precision": 0.6666666865348816
            },
            "2": {
                "f1_score": 1.0,
                "recall": 1.0,
                "support": 2,
                "precision": 1.0
            }
        },
        "weighted_statistics": {
            "f1_score": 0.7200000057220459,
            "recall": 0.8,
            "support": 5,
            "precision": 0.6666666746139527
        },
        "confusion_matrix": {
            "1": {
                "0": 1
            },
            "0": {
                "0": 2
            },
            "2": {
                "2": 2
            }
        }
    },
    "state": "finished"
    "runStarted": "2016-02-15T19:14:38.3371956Z",
    "runFinished": "2016-02-15T19:14:38.3453991Z",
    "id": "2016-02-15T19:14:38.337123Z-5bc7042b732cb41f",
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
percentile of \\( (y_i - \hat{y}_i) / y_i \forall i \\). The 50th percentile is
the median and represents the *median absolute percentage* (MAPE).

Here is a sample output:

```
GET http://localhost/v1/procedures/ttnc_test_scorer/runs/1

{
    "status": {
        "quantile_errors": {
            "0.9": 0.1666666666666667,
            "0.25": 0.0,
            "0.5": 0.1428571428571428,
            "0.75": 0.1666666666666667
        },
        "mse": 0.375,
        "r2_score": 0.9699681653424412
    },
    "state": "finished"
    "runFinished": "2016-02-15T19:14:38.3490527Z",
    "runStarted": "2016-02-15T19:14:38.3476734Z",
    "id": "2016-02-15T19:14:38.347613Z-5bc7042b732cb41f",
}
```

The `output` dataset created by this procedure in `regression` mode 
will contain one row per input row, 
with the same row name as in the input, and the following columns:

* `label`: the row's actual value
* `score`: predicted value
* `weight`: the row's assigned weight

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
