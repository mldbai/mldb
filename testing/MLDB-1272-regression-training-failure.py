# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

import datetime
import random
import json


def linear_regression(feat1, feat2):
    return random.gauss(2 * feat1 + feat2 + 5, 3)

dataset = mldb.create_dataset({
    "type": "sparse.mutable",
    "id": "dataset"
})

now = datetime.datetime.now()

# force a linear regression
for i in xrange(500):
    feat1 = random.randint(1, 20)
    feat2 = random.randint(1, 100)
    dataset.record_row("u%d" % i, [["feat1", feat1, now],
                                   ["feat2", feat2, now],
                                   ["label", linear_regression(feat1, feat2), now]])

dataset.commit()

test_dataset = mldb.create_dataset({
    "type": "sparse.mutable",
    "id": "test_dataset"
})

for i in xrange(50):
    feat1 = random.randint(1, 20)
    feat2 = random.randint(1, 100)
    test_dataset.record_row("u%d" % i, [["feat1", feat1, now], ["feat2", feat2, now]])

test_dataset.commit()


mldb.log(json.loads(mldb.perform("GET", "/v1/query", [["q", "SELECT * from dataset limit 10"]])["response"]))

config = {
    "type": "classifier.train",
    "params": {
        "trainingData": """
        SELECT {
                feat1, feat2
            } as features,
            label
        FROM dataset""",
    "algorithm": "glz",
    "configuration":{
        "glz": {
            "type": "glz",
            "verbosity": 3,
            "link_function": "linear"
        }
    },
    "mode": "regression",
    "equalizationFactor": 0.5,  #the test will succeed only if this value is 0.0
    "runOnCreation": True,
    "functionName": "predictor",
    "modelFileUrl":"file://tmp/MLDB-1272.cls"
    }
}

mldb.log(json.loads(mldb.perform("PUT", "/v1/procedures/trainProc", [], config)["response"]))


# make the predictions
response = mldb.perform('GET', '/v1/query', [["q",
                                              """SELECT feat1, 
                                                        feat2, 
                                                        predictor({features:{feat1, feat2}})["score"] as score
                                                 FROM test_dataset"""]])["response"]

scores = json.loads(response)
mldb.log(scores)

diff = []
for score in scores:
    columns = score["columns"]
    feat1 = columns[0][1]
    feat2 = columns[1][1]
    expected_score = linear_regression(feat1, feat2)
    score = columns[2][1]
    # score's type will be dict if it is NaN
    assert type(score) == type(1.0), 'expecting the type of the score to be a float'
    diff.append(abs(score - expected_score))
    mldb.log("expected: " + str(expected_score) + " score: " + str(score))

avg_diff = sum(diff) / float(len(diff))
mldb.log(avg_diff)
assert avg_diff < 3.0, "expecting the regression errors to be lower than 3 on average"


mldb.script.set_return('success')
