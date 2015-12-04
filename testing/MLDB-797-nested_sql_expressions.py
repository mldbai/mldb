# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

def check_res(res, code):
    if res['statusCode'] != code:
        mldb.log(json.loads(res['response']))
        assert False

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})


ds1.record_row('row_0', [['x', 1, 0], ['y', 2, 0]])
ds1.record_row('row_1', [['x', 1, 0], ['y', 3, 0]])
ds1.record_row('row_2', [['y', 4, 0]])
ds1.commit()

# first void sql.expression
res = mldb.perform('PUT', '/v1/functions/patate', [], {
    'type': 'sql.expression',
    'params': {
        'expression': '*'}})
check_res(res, 201)

# second void sql.expression that uses the first one
res = mldb.perform('PUT', '/v1/functions/poil', [], {
    'type': 'sql.expression',
    'params': {
        'expression': 'patate({*})'}})
check_res(res, 201)

# query calling through both
res = mldb.perform('GET', '/v1/datasets/ds1/query',
                   [['select', 'poil({*})']],
                   {})
check_res(res, 200)


mldb.script.set_return('success')
