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
            'outputDataset' : {'id' : 'kmeans_dataset', 'type' : 'embedding',
                        'params': { 'metric': metric }},
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

now = datetime.datetime.now()

# KMEANS TRAIN PROCEDURE WITH BOTH TYPE OF INPUT DATA
load_kmeans_dataset()
train_kmeans('select * from kmeans_example')
train_kmeans('select x + y as x, y + x as y from kmeans_example')
train_kmeans({'select' : '*', 'from' : {'id' : 'kmeans_example'}})

mldb.script.set_return('success')
