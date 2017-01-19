#
# MLDB-951-run-on-creation.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import json
import datetime
import time

if False:
    mldb_wrapper = None
_legacy = mldb
mldb = mldb_wrapper.wrap(mldb) # noqa


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
response = mldb.put("/v1/procedures/transform_procedure", {
    "type": "transform",
    "params": {
        "inputData": "select x + 1 as x from dataset1",
        "outputDataset": {"id": "dataset2", "type": "sparse.mutable"},
        "runOnCreation" : False
    }
})
mldb.log(response)
assert 'status' not in response.json(), \
    'not expecting a status field since there are no first run'


# test that location points to the actual created object (that was a bug
# found during testing)
response = mldb.post_async("/v1/procedures/transform_procedure/runs")
mldb.log(response)
location = response.headers['Location']
mldb.log(mldb.get(location))
id = location.split('/')[-1]
res = mldb.get("/v1/procedures/transform_procedure/runs").json()[0]
res == id, 'expected ids to match'


# with a first run
response = mldb.put("/v1/procedures/transform_procedure", {
    "type": "transform",
    "params": {
        "inputData": {
            "select": "x + 1 as x",
            "from" : {"id": "dataset1"}
        },
        "outputDataset": {"id": "dataset3", "type": "sparse.mutable"},
        "runOnCreation" : True
    }
})
mldb.log(response.json())
assert 'firstRun' in response.json()['status'], 'expected a firstRun param'

# check that the transformed dataset is as expected
transformed_rows = mldb.get('/v1/query', q='select x from dataset3').json()
for row in transformed_rows:
    assert int(row['rowName']) + 1 == row['columns'][0][1], \
        'the transform was not applied correctly'

# with a first run async
response = mldb.put_async("/v1/procedures/transform_procedure", {
    "type": "transform",
    "params": {
        "inputData": "select x + 1 as x from dataset1",
        "outputDataset": {"id": "dataset4", "type": "sparse.mutable"},
        "runOnCreation" : True
    }
})
mldb.log(response)
assert 'firstRun' in response.json()['status'], 'expected a firstRun param'

running = True
run_url = '/v1/procedures/transform_procedure/runs/' \
    + response.json()['status']['firstRun']['id']
while(running):
    time.sleep(0.2)
    resp = mldb.get(run_url)
    if resp.json()['state'] == 'finished':
        running = False

# check that the transformed dataset is as expected
transformed_rows = mldb.get('/v1/query', q='select x from dataset4').json()
for row in transformed_rows:
    assert int(row['rowName']) + 1 == row['columns'][0][1], \
        'the transform was not applied correctly'

# checking return code for invalid runs
try:
    mldb.put("/v1/procedures/transform_procedure", {
        "type": "transform",
        "params": {
            "inputData": "select x + 1 as x from dataset1",
            # failing because beh may not be available or is not mutable
            "outputDataset": {"id": "dataset2", "type":"beh"},
            "runOnCreation" : True
        }
    })
except mldb_wrapper.ResponseException as exc:
    res = exc.response
else:
    assert False, 'should not be here'
mldb.log(res)
response = res.json()
mldb.log(response)
assert 'runError' in response['details'], 'expected a runError param'
assert 'error' in response['details']['runError'], \
    'expected an error description for the failure'
assert 'httpCode' in response['details']['runError'], 'expected an error code'

mldb.script.set_return('success')
