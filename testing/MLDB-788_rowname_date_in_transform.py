# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import json

# Dataset with one column and one row
ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

for i in xrange(1):
    ds1.record_row('row_' + str(i),
                   [['x', i, 1438895158]])
ds1.commit()

# Simple query of "x, rowName() as rowname" passed through our function
res = mldb.perform('GET', '/v1/query', [
    ['q', 'SELECT x, 1 as y, rowName() as rowname FROM ds1']])

response = json.loads(res['response'])
expected = response
mldb.log(response)
assert res['statusCode'] == 200

# Same query but in a transform procedure
res = mldb.perform('PUT', '/v1/procedures/poil', [], {
    'type': 'transform',
    'params': {
        'inputData': {
            'select': 'x, 1 as y, rowName() as rowname',
            'from' : {'id': 'ds1'}
        },
        'outputDataset': {
            'id': 'ds2',
            'type': 'sparse.mutable'
        }
    }
})

assert res['statusCode'] == 201
response = json.loads(res['response'])

res = mldb.perform('POST', '/v1/procedures/poil/runs', [], {})
assert res['statusCode'] == 201

res = mldb.perform('GET', '/v1/query', [['q', 'select x, y, rowname from ds2']], {})
response = json.loads(res['response'])
mldb.log(response)

assert res['statusCode'] == 200

# assert that this query returns the same thing as the first one

assert expected == response

mldb.script.set_return('success')
