# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json, random, datetime


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
        "trainingData": "select {* EXCLUDING(label)} as features, label from toy",
        "testingData": "select {* EXCLUDING(label)} as features, label from toy",
        "datasetFolds" : [
            {
                "training_where": "rowHash() % 5 != 3",
                "testing_where": "rowHash() % 5 = 3",
                "orderBy": "rowHash() ASC",
            },
            {
                "training_where": "rowHash() % 5 != 2",
                "testing_where": "rowHash() % 5 = 2",
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
                "link": "linear",
                "ridge_regression": True
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


rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], permutor_conf)
mldb.log(rez)

rez = mldb.perform("POST", "/v1/procedures/rocket_science/runs")
#mldb.log(rez)
assert rez["statusCode"] == 201

jsRez = json.loads(rez["response"])
#mldb.log(jsRez)


# did we run all the permutations and get a good auc ?
assert len(jsRez["status"]) == 2 * 4

for permutation in jsRez["status"]:
    assert len(permutation["results"]["folds"]) == 2
    assert permutation["results"]["aggregated"]["auc"]["mean"] > 0.9

mldb.script.set_return("success")

