# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, random

def assert_res_status_equal(res, value):
    if res['statusCode'] != value:
        mldb.log(json.loads(res['response']))
        assert False

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})
ds2 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset2'})

for i in xrange(10):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0]])
for i in xrange(5):
    ds2.record_row('row_' + str(i),
                   [['ds1_row', 'row_' + str(i), 0], ['y', i, 0]])
ds1.commit()
ds2.commit()

res = mldb.perform('GET', '/v1/query', [
    ['q', 'SELECT d1.x, d2.y FROM dataset1 AS d1 JOIN dataset2 AS d2 ON d1.rowName() = d2.ds1_row order by rowName()'], ['format', 'table']],
    {})
assert_res_status_equal(res, 200)

mldb.log(json.loads(res['response']))

expected = [
   [ "_rowName", "d1.x", "d2.y" ],
   [ "row_0-row_0", 0, 0 ],
   [ "row_1-row_1", 1, 1 ],
   [ "row_2-row_2", 2, 2 ],
   [ "row_3-row_3", 3, 3 ],
   [ "row_4-row_4", 4, 4 ]
];

assert json.loads(res['response']) == expected

# TODO check that the it returns the right thing

mldb.script.set_return('success')
