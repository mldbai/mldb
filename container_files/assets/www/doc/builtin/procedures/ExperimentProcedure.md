# Classifier Experiment Procedure

The classifier experiment procedure is used to train and evaluate a classifier in a single run.
It wraps the `classifier.train` and the `classifier.test` procedures into a single and easier to 
use procedure. It can also perform k-fold cross-validation by specifying multiple
folds over the data to use for training and testing.

## Configuration

![](%%config procedure classifier.experiment)

<div id="DatasetFoldConfig">
## Cross-validation

The experiment procedure supports k-fold 
[cross-validation](https://en.wikipedia.org/wiki/Cross-validation_(statistics)) in a
flexible way. A fold can be specified using the following configuration, which should allow
for virtually any desired configuration.

![](%%type Datacratic::MLDB::DatasetFoldConfig)

The helper parameter `kfold` can also be used to generate a `datasetFolds` configuration
implementing a standard k-fold cross-validation. The dataset will be split in $$k$$ subsamples and
each fold will use one subsample as its test set and train on the other $$k-1$$ folds.

When using the `kfold` parameter, the `testing_dataset` parameter should not be specified.

### Default behaviour

If no fold configuration is specified and a testing dataset...

- **is specified**, a `dataset_folds` configuration that uses
all the `training_dataset` for training and all the `testing_dataset` for testing
will be generated.
- **is NOT specified**, a `dataset_folds` configuration that splits
the dataset in half for training and testing will be generated.


### Example: 3-fold cross-validation

To perform a 3-fold cross-validation, set the parameter `kfolds = 3` and the following
configuration will be generated for the `dataset_folds` parameter:

    [
        {
            "training_where": "rowHash() % 3 != 0",
            "testing_where": "rowHash() % 3 = 0",
        },
        {
            "training_where": "rowHash() % 3 != 1",
            "testing_where": "rowHash() % 3 = 1",
        },
        {
            "training_where": "rowHash() % 3 != 2",
            "testing_where": "rowHash() % 3 = 2",
        }
    ]



## Output

The output will contain performance metrics over each fold that was tested. See the 
![](%%doclink classifier.test procedure) page for a sample output.

An aggregated version of the metrics over all folds is also provided. The aggregated
results blob will have the same structure as each fold but every numeric metric will
be replaced by an object containing standard statistics (min, max, mean, std).
In the example below, the `auc` metric is used to illustrate the aggregation.

The following example would be for a 2-fold run:

    {
        "id" : "<id>",
        "runFinished" : "...",
        "runStarted" : "...",
        "state" : "finished",
        "status" : {
            "aggregated": {
                "auc": {
                    "max": x,
                    "mean": y,
                    "min": z,
                    "std": k
                },
                ...
            },
            "folds": [
                {
                    "results" : { <classifier.test output for fold 1> },
                    "fold": { <dataset_fold used for fold 1> }
                },
                {
                    "results" : { <classifier.test output for fold 2> },
                    "fold": { <dataset_fold used for fold 2> }
                }
            ]
        }
    }

## See also

* ![](%%doclink classifier.train procedure)
* ![](%%doclink classifier.test procedure)
* ![](%%doclink classifier function)
* [Cross-validation](https://en.wikipedia.org/wiki/Cross-validation_(statistics)) on Wikipedia


