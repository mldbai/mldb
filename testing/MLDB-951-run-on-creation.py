# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import datetime
import requests
import random
import requests
import time

def load_test_dataset():
    ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})
    
    row_count = 100
    for i in xrange(row_count - 1):
        # row name is x's value
        ds1.record_row(str(i), [['x', i, datetime.datetime.now()]])

    ds1.commit()

load_test_dataset()

# TRANSFORM PROCEDURE
# without first run
response =  mldb.perform('PUT', "/v1/procedures/transform_procedure", [], 
                         {
                             "type": "transform",
                             "params": {
                                 "inputDataset": { "id": "dataset1" },
                                 "outputDataset": { "id": "dataset2", "type":"sparse.mutable" },
                                 "select": "x + 1 as x",
                                "runOnCreation" : False
                             }
                         })
mldb.log(response)
assert response['statusCode'] == 201, 'expected a 201'
assert 'status' not in json.loads(response['response']), 'not expecting a status field since there are no first run'


# test that location points to the actual created object (that was a bug found during testing)
response = mldb.perform('POST', "/v1/procedures/transform_procedure/runs", [], {}, [['async', 'true']])
mldb.log(response)
location = response['headers'][0][1]
mldb.log(mldb.perform('GET', location, [], {}))
id = location.split('/')[-1]
assert json.loads(mldb.perform('GET', "/v1/procedures/transform_procedure/runs", [], {})
                  ["response"])[0] == id, 'expected ids to match'
assert mldb.perform('GET', location, [], {})['statusCode'] == 200, 'could not find the created run!'


# with a first run
response =  mldb.perform('PUT', "/v1/procedures/transform_procedure", [], 
                         {
                             "type": "transform",
                             "params": {
                                 "inputDataset": { "id": "dataset1" },
                                 "outputDataset": { "id": "dataset3", "type":"sparse.mutable" },
                                 "select": "x + 1 as x",
                                "runOnCreation" : True
                             }
                         })
mldb.log(json.loads(response['response']))
assert response['statusCode'] == 201, 'expected a 201'
assert 'firstRun' in json.loads(response['response'])['status'], 'expected a firstRun param'

# check that the transformed dataset is as expected
transformed_rows = json.loads(mldb.perform('GET', '/v1/query', [['q', 'select x from dataset3']])['response'])
#mldb.log(transformed_rows)
for row in transformed_rows:
    assert int(row['rowName']) + 1 == row['columns'][0][1], 'the transform was not applied correctly'

# with a first run async
response =  mldb.perform('PUT', "/v1/procedures/transform_procedure",[], 
                         {
                             "type": "transform",
                             "params": {
                                 "inputDataset": { "id": "dataset1" },
                                 "outputDataset": { "id": "dataset4", "type":"sparse.mutable" },
                                 "select": "x + 1 as x",
                                "runOnCreation" : True
                             }
                         },  [['async', 'true']])
mldb.log(response)
assert response['statusCode'] == 201, 'expected a 201'
assert 'firstRun' in json.loads(response['response'])['status'], 'expected a firstRun param'

running = True
run_url = '/v1/procedures/transform_procedure/runs/' + json.loads(response['response'])['status']['firstRun']['id']
while(running):
    time.sleep(0.2)
    resp = mldb.perform('GET', run_url, [], {});
    if json.loads(resp['response'])['state'] == 'finished':
        running = False

# check that the transformed dataset is as expected
transformed_rows = json.loads(mldb.perform('GET', '/v1/query', [['q', 'select x from dataset4']])['response'])
for row in transformed_rows:
    assert int(row['rowName']) + 1 == row['columns'][0][1], 'the transform was not applied correctly'

# checking return code for invalid runs
response =  mldb.perform('PUT', "/v1/procedures/transform_procedure", [], 
                         {
                             "type": "transform",
                             "params": {
                                 "inputDataset": { "id": "dataset1" },
                                 "outputDataset": { "id": "dataset2", "type":"beh" }, # try to create a non-mutable
                                 "select": "x + 1 as x",
                                "runOnCreation" : True
                             }
                         })
mldb.log(response)
assert response['statusCode'] == 400, 'expected a 400'
assert 'runError' in json.loads(response['response'])['details'], 'expected a runError param'


mldb.script.set_return('success')
