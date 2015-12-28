# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, random

def assert_res_status_equal(res, value):
    if res['statusCode'] != value:
        mldb.log(json.loads(res['response']))
        assert False

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

for i in xrange(50):
    ds1.record_row('row_' + str(i),
                   [['x', random.random(), 0], ['y', random.random(), 0]])
ds1.commit()

svd_file = 'file://tmp/MLDB-805.svd'
res = mldb.perform('PUT', '/v1/procedures/train_svd', [], {
    'type': 'svd.train',
    'params': {
        'trainingData': 'select * from ds1',
        'modelFileUrl': svd_file,
        'numSingularValues': 5,
        'numDenseBasisVectors': 10}})
assert_res_status_equal(res, 201)

res = mldb.perform('POST', '/v1/procedures/train_svd/runs', [], {})
assert_res_status_equal(res, 201)

res = mldb.perform('PUT', '/v1/functions/embed', [], {
    'type': 'svd.embedRow',
    'params': {
        'modelFileUrl': svd_file}})
assert_res_status_equal(res, 201)

res = mldb.perform('GET', '/v1/functions/embed/info')
mldb.log(json.loads(res['response']))

# verify that the function works
res = mldb.perform('GET', '/v1/functions/embed/application', [
    ['input', {'row': {
        'x': .5,
        'y': .5}}]], {})
assert_res_status_equal(res, 200)

# now use it in a query
res = mldb.perform('GET', '/v1/datasets/ds1/query', [
    ['select',
     'embed({{*} as row})']])
assert_res_status_equal(res, 200)

mldb.script.set_return('success')
