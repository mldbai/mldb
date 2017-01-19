#
# MLDB-505-in-expression.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa


def query_and_check(query, expectedSize):
    mldb.log(query)
    result = mldb.get('/v1/query', q=query)
    mldb.log(result)
    response = result.json()
    assert len(response) == expectedSize

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'
})

for i in xrange(5):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0], ['y', i*2, 0]])
ds1.commit()

query_and_check('SELECT * FROM ds1 WHERE y IN (4,8)', 2)

query_and_check('SELECT * FROM ds1 WHERE y NOT IN (4,8)', 3)

query_and_check('SELECT x FROM ds1 WHERE x IN (SELECT y from ds1)', 3)

query_and_check('SELECT x FROM ds1 WHERE x NOT IN (SELECT y from ds1)', 2)

# Then check that there are no datasets besides the two that we explicitly created
result = mldb.get('/v1/datasets')
response = result.json()
assert len(response) == 1;

mldb.script.set_return('success')
