# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

mldb.log("Start")

def assert_res_status_equal(res, value):
    if res['statusCode'] != value:
        mldb.log(json.loads(res['response']))
        assert False

def QueryAndCheck(query, expectedSize):
    mldb.log(query)
    result = mldb.perform('GET', '/v1/query', [['q', query]])
    mldb.log(result)
    assert(result['statusCode'] == 200)
    response = json.loads(result['response']);
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
QueryAndCheck('SELECT * FROM dataset1', 10)

QueryAndCheck('SELECT dataset1.* from dataset1', 10)

QueryAndCheck('SELECT blah.* FROM dataset1 AS blah', 10)

QueryAndCheck('SELECT blah.* FROM (SELECT * FROM dataset1) AS blah', 10)

QueryAndCheck('SELECT blah.* FROM (SELECT * FROM dataset1 WHERE (x % 2) = 0) AS blah', 5)

QueryAndCheck('SELECT blah.* FROM (SELECT * FROM dataset1 WHERE (x % 2) = 0) AS blah WHERE (x%4) = 0', 3)

QueryAndCheck('SELECT blah.* FROM (SELECT * FROM dataset1 AS t1 JOIN dataset2 AS t2 ON t1.x = t2.y) AS blah', 5)

QueryAndCheck('SELECT blah.* FROM (SELECT * FROM dataset1 AS t1 JOIN dataset2 AS t2 ON t1.x = t2.y) AS blah WHERE t1.x = 0', 1)

QueryAndCheck('SELECT * FROM (SELECT * FROM dataset1 WHERE x > 4) ORDER BY x', 5)

#MLDB-853 sub queries without dataset

QueryAndCheck('SELECT 1 FROM (SELECT 1)', 1)

QueryAndCheck('SELECT x.* FROM (select {1 as y} as z ) as x', 1)

#MLDB-855
QueryAndCheck('SELECT * FROM (select {*} as y from dataset1) as x', 10)

# Then check that there are no datasets besides the two that we explicitly created
result = mldb.perform('GET', '/v1/datasets')
assert_res_status_equal(result, 200)
response = json.loads(result['response']);
assert len(response) == 2;

mldb.script.set_return('success')        
