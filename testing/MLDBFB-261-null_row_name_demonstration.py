# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDBFB-261-null_row_name_demonstration.py
# Mich, 2015-11-25
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import json

if False:
    mldb = None

res = mldb.perform('PUT', '/v1/datasets/testDs', [], {
    'type' : 'sparse.mutable'
})
assert res['statusCode'] == 201, res

def record_row(train, test, metric, bucket, count):
    res = mldb.perform('POST', '/v1/datasets/testDs/rows' , [], {
        'columns' : [
                ['train', train, 0],
                ['test', test, 0],
                ['metric', metric, 0],
                ['bucket', bucket, 0],
                ['value', count, 0]
            ]
    })
    assert res['statusCode'] == 200, res

record_row('active', 'active', 'count', '0-100', 10)
record_row('active', 'active', 'cummul count', '0-100', 10)
record_row('active', 'inactive', 'count', '0-100', 22)
record_row('active', 'inactive', 'cummul count', '0-100', 22)
record_row('inactive', 'active', 'count', '0-100', 32)
record_row('inactive', 'active', 'cummul count', '0-100', 32)

res = mldb.perform('POST', '/v1/datasets/testDs/commit', [], {})
assert res['statusCode'] == 200, res

res = mldb.perform('GET', '/v1/query', [['q', 'SELECT * FROM testDs']])
data1 = json.loads(res['response'])
mldb.log(data1)

res = mldb.perform('GET', '/v1/query', [['q', 'SELECT * FROM testDs'], ['format', 'table']])
data2 = json.loads(res['response'])
mldb.log(data2)

assert len(data1) == 6, "There should be six rows"
for row in data1:
    assert len(row['columns']) == 5, "There should be 5 columns per row"

assert len(data2) == 7, "There should be seven rows (6 data + 1 header)"
for row in data2:
    assert len(row) == 6, "There should be 6 columns per row"

mldb.script.set_return("success")
