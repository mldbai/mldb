# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import datetime
import json
import random

def load_kmeans_dataset():
    kmeans_example = mldb.create_dataset({"type": "sparse.mutable", 'id' : 'kmeans_example'})
    for i in range(0,100):
        val_x = float(random.randint(-5, 5))
        val_y = float(random.randint(-5, 5))
        row = [['x', val_x, now], ['y', val_y, now]]
        kmeans_example.record_row('row_%d' % i, row)
    kmeans_example.commit()

def train_kmeans(trainingData):
    metric = "euclidean"
    result = mldb.perform("PUT", "/v1/procedures/kmeans", [], {
        'type' : 'kmeans.train',
        'params' : {
            'trainingData' : trainingData,
            'centroidsDataset' : {'id' : 'kmeans_centroids', 'type' : 'embedding', 
                           'params': {'metric': metric }},
            'numClusters' : 2,
            'metric': metric
        }
    })

    response = json.loads(result['response'])
    msg = "Could not create the kmeans classifier procedure - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 201, msg

    result = mldb.perform('POST', '/v1/procedures/kmeans/runs')
    response = json.loads(result['response'])
    msg = "Could not train the kmeans cluster - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    assert 300 > result['statusCode'] >= 200, msg

def test_config_error_kmeans(trainingData):
    metric = "euclidean"
    result = mldb.perform("PUT", "/v1/procedures/kmeans", [], {
        'type' : 'kmeans.train',
        'params' : {
            'trainingData' : trainingData,
            'centroidsDataset' : {'id' : 'kmeans_centroids', 'type' : 'embedding', 
                           'params': {'metric': metric }},
            'numClusters' : 2,
            'metric': metric
        }
    })

    response = json.loads(result['response'])
    mldb.log(response)
    msg = "Could not create the kmeans classifier procedure - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    return result['statusCode']

def train_svd(trainingData):
    result = mldb.perform("PUT", "/v1/procedures/svd", [], {
        'type' : 'svd.train',
        'params' : {
            'trainingData' : trainingData,
            'runOnCreation' : True
        }
    })

    response = json.loads(result['response'])
    msg = "Could not create the svd classifier procedure - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 201, msg

def test_config_error_svd(trainingData):
    result = mldb.perform("PUT", "/v1/procedures/svd", [], {
        'type' : 'svd.train',
        'params' : {
            'trainingData' : trainingData
        }
    })

    response = json.loads(result['response'])
    mldb.log(response)
    msg = "Could not create the svd classifier procedure - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    return result['statusCode']

def load_classifier_dataset():
    dataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "iris_dataset" })
    
    with open("./mldb/testing/dataset/iris.data") as f:
        for i, line in enumerate(f):
            
            cols = []
            line_split = line.split(',')
            if len(line_split) != 5:
                continue
            # Jemery's what if a feature is named label
            cols.append(["label", float(line_split[0]), 0]) #sepal length
            cols.append(["labels", float(line_split[1]), 0]) #sepal width
            cols.append(["petal length", float(line_split[2]), 0])
            cols.append(["petal width", float(line_split[3]), 0])
            cols.append(["features", line_split[4].strip('\n"'), 0]) #class
            dataset.record_row(str(i+1), cols)

    dataset.commit()

def train_classifier(trainingData):
    result = mldb.perform("PUT", "/v1/procedures/classifier", [], {
        'type' : 'classifier.train',
        'params' : {
            'trainingData' : trainingData,
            "configuration": {
                "type": "decision_tree",
                "max_depth": 8,
                "verbosity": 3,
                "update_alg": "prob"
            },
            "modelFileUrl": "file://tmp/MLDB-1104.cls",
            "mode": "categorical",
            "functionName": "classifier_apply",
            'runOnCreation' : True
        }
    })

    response = json.loads(result['response'])
    msg = "Could not create the classifier procedure - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 201, msg
    
    return response

    


now = datetime.datetime.now()

# KMEANS TRAIN PROCEDURE WITH BOTH TYPE OF INPUT DATA
load_kmeans_dataset()
train_kmeans('select * from kmeans_example')
train_kmeans('select x + y as x, y + x as y from kmeans_example')
train_kmeans({'select' : '*', 'from' : {'id' : 'kmeans_example'}})

# TEST ERROR CASES
assert test_config_error_kmeans(
    'select x, y from kmeans_example group by x') == 400, "expected 400 when group by is specified"
assert test_config_error_kmeans(
    'select x, y from kmeans_example group by x having y > 2') == 400, "expected 400 when having is specified"

train_svd('select * from kmeans_example')
train_svd('select x, y from kmeans_example')
train_svd('select x AS z, y from kmeans_example')
train_svd('select * EXCLUDING(x) from kmeans_example')
train_svd({'select' : '*', 'from' : {'id' : 'kmeans_example'}})

assert test_config_error_svd(
    'select x, y from kmeans_example group by x') == 400, "expected 400 when group by is specified"
assert test_config_error_svd(
    'select x, y from kmeans_example group by x having y > 2') == 400, "expected 400 when having is specified"
assert test_config_error_svd(
    'select x + 1, y from kmeans_example') == 400, "expected 400 when column operations are specified"

load_classifier_dataset()
mldb.log(train_classifier("select {label, labels} as features, features as label from iris_dataset"))

result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", 
                      [["select", 'classifier_apply({{label, labels} as features}) as *, features']], {})
rows = json.loads(result['response'])

# compare the classifier results on the train data with the original label
count = 0
for row in rows:
    max = 0
    category = ""
    for column in row['columns'][0:3]:
        if column[1] > max:
            max = column[1]
            category = column[0][8:-1] # remove the leading scores. and quotation marks
    if category != row['columns'][3][1]:
        count += 1
    
# misclassified result should be a small fraction
assert float(count) / len(rows) < 0.2, 'the classifier results on the train data are starngely low'
    

mldb.script.set_return('success')
