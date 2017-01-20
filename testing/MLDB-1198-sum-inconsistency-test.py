#
# MLDB-1198-sum-inconsistency-test.py
# Mich, 2015-12-15
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

mldb.put('/v1/datasets/ds', {
    'type' : 'sparse.mutable'
})

def insert_with_ts(ts):
    mldb.post('/v1/datasets/ds/rows', {
        'rowName' : 'row1',
        'columns' : [
            ['colA', 1, ts],
        ]
    })

insert_with_ts(1)
insert_with_ts(10)
insert_with_ts(100)
insert_with_ts(1000)

mldb.post('/v1/datasets/ds/commit')

query = 'SELECT sum("colA") as "colA" FROM ds'
res = mldb.query(query)
count = res[1][1]
mldb.log("First query count: {}".format(count))

query = "SELECT sum({*}) AS * FROM ds"
data = mldb.query(query)
mldb.log(data)
cols = data[0]
vals = data[1]
for col, val in zip(cols, vals):
    if col == 'colA':
        mldb.log(val)
        assert count == val, ('First sum ({}) != second sum ({})'
                              .format(count, val))

query = 'SELECT count("colA") as "colA" FROM ds'
res = mldb.query(query)
count = res[1][1]
mldb.log("First query count: {}".format(count))

query = "SELECT count({*}) AS * FROM ds"
data = mldb.query(query)
cols = data[0]
vals = data[1]
for col, val in zip(cols, vals):
    if col == 'colA':
        mldb.log(val)
        assert count == val, ('First sum ({}) != second sum ({})'
                              .format(count, val))

mldb.script.set_return("success")
