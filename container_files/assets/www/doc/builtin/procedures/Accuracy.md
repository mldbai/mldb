# Classifier Testing Procedure

The classifier testing procedure allows the accuracy of a binary classifier to be tested against
held-out data. The output of this procedure is a dataset which contains the scores and statistics
resulting from the application of a classifier to some input data.

## Configuration

![](%%config procedure classifier.test)

## Output

After this procedure has been run, a summary of the accuracy can be obtained via 
`GET /v1/procedures/<id>/runs/<runid>`, where the `status` field will contain the [Area
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
  "trainingFinished": "2015-06-05T18:06:19.165Z", 
  "state": "finished", 
  "trainingStarted": "2015-06-05T18:06:19.119Z", 
  "id": "2015-06-05T18:06:19.118784Z-5bc7042b732cb41f"
}

```

The `output` dataset created by this procedure will contain one row per input row, 
with the same row name as in the input, and the following columns:

* `score`: the score the classifier assigned to this row
* `label`: the row's actual label
* `weight`: the row's assigned weight
* classifier attributes if this row's score was used as a binary threshold:
  * `falseNegatives`, `trueNegatives`, `falsePositives`, `truePositives`
  * `falsePositiveRate`, `truePositiveRate`, `precision`, `recall`


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