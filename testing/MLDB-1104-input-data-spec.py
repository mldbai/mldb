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
#assert test_config_error_svd(
#    'select x* from kmeans_example') == 400, "expected 400 when column operations are specified"
assert test_config_error_svd(
    'select x + 1, y from kmeans_example') == 400, "expected 400 when column operations are specified"

mldb.script.set_return('success')
