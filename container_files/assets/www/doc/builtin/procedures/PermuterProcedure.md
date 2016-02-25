# Permuter procedure

The permuter procedure is used to run a child procedure multiple times while 
modifying some of its parameters at each run. It works by doing the cartesian
product over all the elements provided in its configuration.

This procedure can be used to do a grid search to find the best hyper-parameter for
an algorithm on a dataset.


## Configuration

![](%%config procedure permuter.run)

The string `$permutation` can be used anywhere in the configuration blobs and will be replaced by
an identifier for each permutation. This can be used if the procedure generates artifacts
at each run and they need to be distinguishable.

## Example

The following example will use the ![](%%doclink classifier.experiment procedure), 
that is used to train and test a classifier. We will be trying two types
of classifiers as well as different values for the `equalizationFactor`.

Notice how the key structure of the `permutations` object matches the
`procedure_configuration.params` object.

```javascript
PUT /v1/procedures/<id>
{
    "type": "permuter.run"
    "params": {
        "procedure": {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test-exp_$permutation",
                "training_dataset": "my_dataset",
                "testing_dataset": "my_dataset",
                "select": "* EXCLUDING(label)",
                "label": "label",
                "modelFileUrlPattern": "file://bouya-$runid.cls",
                "algorithm": "glz",
                "equalizationFactor": 0.5,
                "mode": "boolean"
            }
        },
        "permutations": {
            "equalizationFactor": [0.5, 0.9],
            "algorithm": ["glz", "bglz"]
        }
    }
}
```

## Output

The procedure will return a list of objects, one per permutation.
Each one will have the following structure:

```javascript
{
    "configuration": { <permutation> },
    "results": { <child procedure output> }
}
```

## See also

* [Hyperparameter optimization](https://en.wikipedia.org/wiki/Hyperparameter_optimization) on Wikipedia

