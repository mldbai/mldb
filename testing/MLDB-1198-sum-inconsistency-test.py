#
# MLDB-1198-sum-inconsistency-test.py
# Mich, 2015-12-15
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import json

if False:
    mldb = None

res = mldb.perform('PUT', '/v1/datasets/ds', [], {
    'type' : 'sparse.mutable'
})
assert res['statusCode'] == 201, str(res)

def insert_with_ts(ts):
    res = mldb.perform('POST', '/v1/datasets/ds/rows', [], {
        'rowName' : 'row1',
        'columns' : [
            ['colA', 1, ts],
        ]
    })
    assert res['statusCode'] == 200, str(res)

insert_with_ts(1)
insert_with_ts(10)
insert_with_ts(100)
insert_with_ts(1000)

res = mldb.perform('POST', '/v1/datasets/ds/commit', [], {})
assert res['statusCode'] == 200, str(res)

query = 'SELECT sum("colA") as "colA" FROM ds'
res = mldb.perform('GET', '/v1/query', [['q', query], ['format', 'table']])
assert res['statusCode'] == 200, str(res)
count = json.loads(res['response'])[1][1]
mldb.log("First query count: {}".format(count))

query = "SELECT sum({*}) AS * FROM ds"
res = mldb.perform('GET', '/v1/query', [['q', query], ['format', 'table']])
assert res['statusCode'] == 200, str(res)
data = json.loads(res['response'])
cols = data[0]
vals = data[1]
for col, val in zip(cols, vals):
    if col == 'colA':
        mldb.log(val)
        assert count == val, ('First sum ({}) != second sum ({})'
                              .format(count, val))

query = 'SELECT count("colA") as "colA" FROM ds'
res = mldb.perform('GET', '/v1/query', [['q', query], ['format', 'table']])
assert res['statusCode'] == 200, str(res)
count = json.loads(res['response'])[1][1]
mldb.log("First query count: {}".format(count))

query = "SELECT count({*}) AS * FROM ds"
res = mldb.perform('GET', '/v1/query', [['q', query], ['format', 'table']])
assert res['statusCode'] == 200, str(res)
data = json.loads(res['response'])
cols = data[0]
vals = data[1]
for col, val in zip(cols, vals):
    if col == 'colA':
        mldb.log(val)
        assert count == val, ('First sum ({}) != second sum ({})'
                              .format(count, val))

mldb.script.set_return("success")
