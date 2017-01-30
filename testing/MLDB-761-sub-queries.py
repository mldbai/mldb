#
# MLDB-761-sub-queries.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

mldb.log("Start")

def assert_res_status_equal(res, value):
    assert res.status_code == value, res.text

def query_and_check(query, expectedSize):
    mldb.log(query)
    result = mldb.get('/v1/query', q=query)
    mldb.log(result)
    response = result.json()
    assert len(response) == expectedSize;

#First create the datasets we'll need
ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

for i in xrange(10):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0]])
ds1.commit()

ds2 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset2'})

for i in xrange(10):
    ds2.record_row('d2row_' + str(i),
                   [['y', i*2, 0]])
ds2.commit()

#Then Queries
query_and_check('SELECT * FROM dataset1', 10)

query_and_check('SELECT dataset1.* from dataset1', 10)

query_and_check('SELECT blah.* FROM dataset1 AS blah', 10)

query_and_check('SELECT blah.* FROM (SELECT * FROM dataset1) AS blah', 10)

query_and_check(
    'SELECT blah.* FROM (SELECT * FROM dataset1 WHERE (x % 2) = 0) AS blah', 5)

query_and_check(
    'SELECT blah.* FROM (SELECT * FROM dataset1 WHERE (x % 2) = 0) AS blah WHERE (x%4) = 0', 3)

query_and_check(
    'SELECT blah.* FROM (SELECT * FROM dataset1 AS t1 JOIN dataset2 AS t2 ON t1.x = t2.y) AS blah', 5)

query_and_check(
    'SELECT blah.* FROM (SELECT * FROM dataset1 AS t1 JOIN dataset2 AS t2 ON t1.x = t2.y) AS blah WHERE t1.x = 0', 1)

query_and_check(
    'SELECT * FROM (SELECT * FROM dataset1 WHERE x > 4) ORDER BY x', 5)

#MLDB-853 sub queries without dataset

query_and_check('SELECT 1 FROM (SELECT 1)', 1)

query_and_check('SELECT x.* FROM (select {1 as y} as z ) as x', 1)

#MLDB-855
query_and_check('SELECT * FROM (select {*} as y from dataset1) as x', 10)

# MLDB-1257 test case
res1 = mldb.get('/v1/query', q='SELECT ln(10) as r')

equiv = [
    '                 SELECT ln(x)   as r FROM ( SELECT 10 as x )        ',
    '                 SELECT ln(x)   as r FROM ( SELECT 10 as x )   as t ',
    '                 SELECT ln(t.x) as r FROM ( SELECT 10 as x )   as t ',
    'SELECT * FROM  ( SELECT ln(x)   as r FROM ( SELECT 10 as x ) )      ',
    'SELECT * FROM  ( SELECT ln(x)   as r FROM ( SELECT 10 as x )   as t )',
    'SELECT * FROM  ( SELECT ln(t.x) as r FROM ( SELECT 10 as x )   as t )'
]

for q in equiv:
    res2 = mldb.get('/v1/query', q=q)
    mldb.log(res2)
    assert res1.json() == res2.json()


# Then check that there are no datasets besides the two that we explicitly created
result = mldb.get('/v1/datasets')
assert_res_status_equal(result, 200)
response = result.json()
assert len(response) == 2;

mldb.script.set_return('success')
