#   MLDB-1025-dataset-output-with-default.py
#   Guy Dumais, 4 November 2015
#   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

import json
import datetime
import requests
import random

dataset_index = 1

def run_transform(output):
    global dataset_index
    dataset_index += 1 
    result = mldb.perform("PUT", "/v1/procedures/transform_procedure", [], 
                          {
                              "type": "transform",
                              "params": {
                                  "inputDataset": { "id": "dataset1" },
                                  "outputDataset": output,
                                  "runOnCreation" : True,
                                  "where": "rowName() = '2'"
                                  }
                          })

    mldb.log(result)
    assert result['statusCode'] == 201, "failed to create the procedure with output: %s" % output

    id = output['id'] if 'id' in output else output

    result = mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM " + id]])
    mldb.log(result)
    assert result['statusCode'] == 200, "failed to get the transformed dataset"
    rows = json.loads(result["response"])
    mldb.log(rows)
    return rows

    
def load_test_dataset():
    ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})

    row_count = 10
    for i in xrange(row_count - 1):
        # row name is x's value
        ds1.record_row(str(i), [['x', i, now], ['y', i, now]])
        
    ds1.commit()

def train_svd_with_default():
    svd_procedure = "/v1/procedures/svd"
    # svd procedure configuration
    svd_config = {
        'type' : 'svd.train',
        'params' :
	{
            "trainingDataset": "dataset1", 
            "rowOutputDataset": "svd_row", # first way to specify output dataset using default
            "columnOutputDataset" : {  # second way to specify an output dataset using default
                "id": "svd_column"
            }
	}
    }
    
    result = mldb.perform('PUT', svd_procedure, [], svd_config)
    response = json.loads(result['response'])
    msg = "Could not create the svd procedure - got status {}\n{}"
    mldb.log(response)
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 201, msg

    result = mldb.perform('POST', svd_procedure + '/runs')
    response = json.loads(result['response'])
    msg = "Could not train the svd procedure - got status {}\n{}"
    mldb.log(response)
    msg = msg.format(result['statusCode'], response)
    assert 300 > result['statusCode'] >= 200, msg

    result = mldb.perform('GET', '/v1/datasets/svd_column', [])
    response = json.loads(result['response'])
    #mldb.log(response)
    msg = "Could not get the svd embedding output - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 200, msg
    assert response['type'] == 'embedding', 'expected an embedding output dataset'

    result = mldb.perform('GET', '/v1/datasets/svd_row', [])
    response = json.loads(result['response'])
    #mldb.log(response)
    msg = "Could not get the svd embedding output - got status {}\n{}"
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 200, msg
    assert response['type'] == 'embedding', 'expected an embedding output dataset'

def train_kmeans_with_default(config):
    kmeans_procedure = "/v1/procedures/kmeans"
    # kmeans procedure configuration
  
    result = mldb.perform('PUT', kmeans_procedure, [], config)
    response = json.loads(result['response'])
    msg = "Could not create the kmeans procedure - got status {}\n{}"
    mldb.log(response)
    msg = msg.format(result['statusCode'], response)
    assert result['statusCode'] == 201, msg

    result = mldb.perform('POST', kmeans_procedure + '/runs')
    response = json.loads(result['response'])
    msg = "Could not train the kmeans procedure - got status {}\n{}"
    mldb.log(response)
    msg = msg.format(result['statusCode'], response)
    assert 300 > result['statusCode'] >= 200, msg

    centroids_id = config['params']['centroidsDataset']['id']
    result = mldb.perform('GET', '/v1/datasets/' + centroids_id, [])
    response = json.loads(result['response'])
    mldb.log(response)
    msg = "Could not get the kmeans centroids [{}]- got status {}\n{}"
    msg = msg.format(centroids_id, result['statusCode'], response)
    assert result['statusCode'] == 200, msg
    return response['type']

now = datetime.datetime.now()
same_time_tomorrow = now + datetime.timedelta(days=1)
in_two_hours = now + datetime.timedelta(hours=2)

load_test_dataset()

# check that the transformed dataset is as expected
assert len( run_transform({ "id": "dataset2", "type": "sparse.mutable" })) == 1, 'expected only one row to be returned'
# check that default type works
assert len( run_transform({ "id": "dataset3" })) == 1, 'expected only one row to be returned'
# check that string are interpreted as dataset id
assert len( run_transform("dataset4") ) == 1, 'expected only one row to be returned'
# check that the transformed dataset can be overwritten
assert len( run_transform({ "id": "dataset2", "type": "sparse.mutable" })) == 1, 'expected only one row to be returned'
   
train_svd_with_default();

metric = 'euclidean'
kmeans_config = {
    'type' : 'kmeans.train',
    'params' : {
        'trainingData' : 'select * from dataset1',
        'centroidsDataset' : {'id' : 'kmeans_centroids', 
                          'params': {'metric': metric}},
        'numClusters' : 2,
        'metric': metric
    }
}

# check that the default type is used
result = mldb.perform('GET', '/v1/datasets', []);
dataset_count_before = len(json.loads(result['response']))
assert train_kmeans_with_default(kmeans_config) ==  'embedding', 'expected an embedding output dataset'
result = mldb.perform('GET', '/v1/datasets', []);
dataset_count_after = len(json.loads(result['response']))
assert dataset_count_before + 1 == dataset_count_after, 'only the centroids must have been created'

kmeans_config = {
    'type' : 'kmeans.train',
    'params' : {
        'trainingData' : 'select * from dataset1',
        'centroidsDataset' : {'id' : 'kmeans_centroids_2',
                              'type' : 'sparse.mutable'},
        'outputDataset': { 'type' : 'embedding'},
        'numClusters' : 2,
        'metric': metric
    }
}

# check that the type can be changed and that id are auto-generated when not specified
result = mldb.perform('GET', '/v1/datasets', []);
dataset_count_before = len(json.loads(result['response']))
assert train_kmeans_with_default(kmeans_config) ==  'sparse.mutable', 'expected an sparse.mutable output dataset'
result = mldb.perform('GET', '/v1/datasets', []);
dataset_count_after = len(json.loads(result['response']))
assert dataset_count_before + 2 == dataset_count_after, 'expect the centroids and the outputDataset to be created'

mldb.script.set_return('success')
