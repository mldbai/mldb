#
# MLDB-991_permuter_procedure.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import random, datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

## Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

for i in xrange(5000):
    label = random.random() < 0.2
    dataset.record_row("u%d" % i, [["feat1", random.gauss(5 if label else 15, 3), now],
                                   ["feat2", random.gauss(-5 if label else 10, 10), now],
                                   ["label", label, now]])

dataset.commit()

procedure_conf = {
    "type": "classifier.experiment",
    "params": {
        "experimentName": "my_test-exp_$permutation",
        "inputData": "select {* EXCLUDING(label)} as features, label from toy",
        "testingDataOverride": "select {* EXCLUDING(label)} as features, label from toy",
        "datasetFolds" : [
            {
                "trainingWhere": "rowHash() % 5 != 3",
                "testingWhere": "rowHash() % 5 = 3",
                "trainingOrderBy": "rowHash() ASC",
            },
            {
                "trainingWhere": "rowHash() % 5 != 2",
                "testingWhere": "rowHash() % 5 = 2",
            }],
        "modelFileUrlPattern": "file://build/x86_64/tmp/bouya-$runid.cls",
        "algorithm": "glz",
        "equalizationFactor": 0.5,
        "mode": "boolean",
        "configuration": {
            "glz": {
                "type": "glz",
                "verbosity": 3,
                "normalize": False,
                "regularization": 'l2'
            },
            "bglz": {
                "type": "bagging",
                "verbosity": 1,
                "validation_split": 0.1,
                "weak_learner": {
                    "type": "glz",
                    "feature_proportion": 1.0,
                    "verbosity": 0
                },
                "num_bags": 32
            }
        }
    }
}

permutations = {
    "equalizationFactor": [0, 1, 0.5, 0.9],
    "algorithm": ["glz", "bglz"]
}

permutor_conf = {
    "type": "permuter.run",
    "params": {
        "procedure": procedure_conf,
        "permutations": permutations
    }
}


rez = mldb.put("/v1/procedures/rocket_science", permutor_conf)
mldb.log(rez)

rez = mldb.post("/v1/procedures/rocket_science/runs")

js_rez = rez.json()


# did we run all the permutations and get a good auc ?
assert len(js_rez["status"]) == 2 * 4

for permutation in js_rez["status"]:
    assert len(permutation["results"]["folds"]) == 2
    assert permutation["results"]["aggregatedTest"]["auc"]["mean"] > 0.9

mldb.script.set_return("success")
