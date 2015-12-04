# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

#First create the datasets we'll need
ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

for i in xrange(10):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0], ['y', i%2, 0]])
ds1.commit()

#run the tests

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT 1 + sum(x + 1) AS "sum_x" FROM dataset1']])

mldb.log(result)

assert json.loads(result['response'])[0]['columns'][0][1] == 45 + 11

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT sum(x) + max(x) AS "sum_max_x" FROM dataset1']])

mldb.log(result)

assert json.loads(result['response'])[0]['columns'][0][1] == 45 + 9

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT sum(x) as "sum_x", max(x) as "max_x" FROM dataset1']])

mldb.log(result)

assert json.loads(result['response'])[0]['columns'][0][1] == 45
assert json.loads(result['response'])[0]['columns'][1][1] == 9

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT 3 + min(x) as "col1", sum(y) / 2 as "col2" FROM dataset1']])

mldb.log(result)

assert json.loads(result['response'])[0]['columns'][0][1] == 3
assert json.loads(result['response'])[0]['columns'][1][1] == 2.5

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT max(x) + max(x) as "col1", max(x) - max(x) as "col2" FROM dataset1']])

mldb.log(result)

assert json.loads(result['response'])[0]['columns'][0][1] == 18
assert json.loads(result['response'])[0]['columns'][1][1] == 0

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT y as "label", min(x) as "min_x", max(x) as "max_x" FROM dataset1 GROUP BY y']])

mldb.log(result)

assert json.loads(result['response'])[0]['columns'][0][1] == 0
assert json.loads(result['response'])[0]['columns'][1][1] == 0
assert json.loads(result['response'])[0]['columns'][2][1] == 8
assert json.loads(result['response'])[1]['columns'][0][1] == 1
assert json.loads(result['response'])[1]['columns'][1][1] == 1
assert json.loads(result['response'])[1]['columns'][2][1] == 9

#MLDB-234
result = mldb.perform('GET', '/v1/query', [['q', 'SELECT x FROM dataset1 GROUP BY 1']])
mldb.log(result)
assert result['statusCode'] == 400

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT x FROM dataset1 GROUP BY y']])
mldb.log(result)
assert result['statusCode'] == 400

#MLDB-331
result = mldb.perform('GET', '/v1/query', [['q', 'SELECT count(y) FROM dataset1']])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == 10

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT count(*) FROM dataset1']])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == 10

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT count(*) FROM dataset1 WHERE y = 3']])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == 0

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT min(x) as empty_min, count(*) AS mycount FROM dataset1 WHERE y = 3']])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == None
assert json.loads(result['response'])[0]['columns'][1][1] == 0

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT min(x) as mymin, count(*) AS mycount FROM dataset1 GROUP BY y']])
mldb.log(result)
assert result['statusCode'] == 200

assert json.loads(result['response'])[0]['columns'][0][1] == 0
assert json.loads(result['response'])[0]['columns'][1][1] == 5
assert json.loads(result['response'])[1]['columns'][0][1] == 1
assert json.loads(result['response'])[1]['columns'][1][1] == 5

mldb.script.set_return('success')
