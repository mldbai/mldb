#
# ranking_procedure_test.py
# Mich, 2016-01-11
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import json

if False:
    mldb = None


def log(thing):
    mldb.log(str(thing))


def mldb_perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res

mldb_perform('PUT', '/v1/datasets/ds', [], {
    'type' : 'sparse.mutable',
})

size = 123
for i in xrange(size):
    mldb_perform('POST', '/v1/datasets/ds/rows', [], {
        'rowName' : 'row{}'.format(i),
        'columns' : [['score', i, 1], ['index', i * 2, 2], ['prob', i * 3, 3]]
    })

mldb_perform('POST', '/v1/procedures', [], {
    'type' : 'ranking',
    'params' : {
        'inputData' : 'SELECT * FROM ds ORDER BY score',
        'outputDataset' : 'out',
        'rankingType' : 'index',
        'runOnCreation' : True
    }
})

# MLDB-1267
res = mldb_perform('GET', "/v1/query",
                   [['q',  "SELECT when({*}) FROM out"], ['format', 'table']])
data = json.loads(res['response'])
assert data[1][1] == '1970-01-01T00:00:01Z', str(data)

log(data[1])
mldb_perform('PUT', '/v1/datasets/result', [], {
    'type' : 'merged',
    'params' : {
        'datasets' : [
            {'id' : 'ds'},
            {'id' : 'out'}
        ]
    }
})

res = mldb_perform('GET', '/v1/query',
                   [['q', 'SELECT score, rank FROM result ORDER BY rank'],
                    ['format', 'table']])
data = json.loads(res['response'])
assert data[1][1] == 0, str(data[1])
assert data[1][2] == 0, str(data[1])
assert data[2][1] == 1, str(data[2])
assert data[2][2] == 1, str(data[2])
assert data[size][1] == size - 1, str(data[size])
assert data[size][2] == size - 1, str(data[size])

mldb.script.set_return("success")
