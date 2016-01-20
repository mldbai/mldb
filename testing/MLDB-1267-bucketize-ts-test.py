#
# MLDB-1267-bucketize-ts-test.py
# Mich, 2015-11-16
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

if False:
    mldb = None

import json

def log(thing):
    mldb.log(str(thing))


def perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res

url = '/v1/datasets/input'
perform('PUT', url, [], {
    'type' : 'sparse.mutable'
})

perform('POST', url + '/rows', [], {
    'rowName' : 'row1',
    'columns' : [['score', 5, 6]]
})
perform('POST', url + '/rows', [], {
    'rowName' : 'row2',
    'columns' : [['score', 1, 5]]
})

perform('POST', url + '/commit', [], {})

perform('POST', '/v1/procedures', [], {
    'type' : 'bucketize',
    'params' : {
        'inputData' : 'SELECT * FROM input ORDER BY score',
        'outputDataset' : {
            'id' : 'output',
            'type' : 'sparse.mutable'
        },
        'percentileBuckets': {'b1': [0, 50], 'b2': [50, 100]},
        'runOnCreation' : True
    }
})

res = perform('GET', '/v1/query', [['q', 'SELECT when({*}) FROM output'],
                                   ['format', 'table']])
data = json.loads(res['response'])
assert data[1][1] == '1970-01-01T00:00:06Z'
mldb.script.set_return("success")
