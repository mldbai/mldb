#
# MLDB-162-min-max-function.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

#First create the datasets we'll need
ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'
})

for i in xrange(10):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0], ['y', i%2, 0]])
ds1.commit()

#run the tests

result = mldb.get('/v1/query',
                  q='SELECT 1 + sum(x + 1) AS "sum_x" FROM dataset1')

mldb.log(result.text)

assert result.json()[0]['columns'][0][1] == 45 + 11

result = mldb.get(
    '/v1/query', q='SELECT sum(x) + max(x) AS "sum_max_x" FROM dataset1')

mldb.log(result.text)

assert result.json()[0]['columns'][0][1] == 45 + 9

result = mldb.get(
    '/v1/query', q='SELECT sum(x) as "sum_x", max(x) as "max_x" FROM dataset1')

mldb.log(result.json())

assert result.json()[0]['columns'][1][1] == 45
assert result.json()[0]['columns'][0][1] == 9

result = mldb.get(
    '/v1/query',
    q='SELECT 3 + min(x) as "col1", sum(y) / 2 as "col2" FROM dataset1')

mldb.log(result.text)

assert result.json()[0]['columns'][0][1] == 3
assert result.json()[0]['columns'][1][1] == 2.5

result = mldb.get(
    '/v1/query',
    q='SELECT max(x) + max(x) as "col1", max(x) - max(x) as "col2" FROM dataset1')

mldb.log(result.text)

assert result.json()[0]['columns'][0][1] == 18
assert result.json()[0]['columns'][1][1] == 0

result = mldb.get(
    '/v1/query',
    q='SELECT y as "label", min(x) as "min_x", max(x) as "max_x" FROM dataset1 GROUP BY y')

mldb.log(result.text)

assert result.json()[0]['columns'][0][1] == 0
assert result.json()[0]['columns'][2][1] == 0
assert result.json()[0]['columns'][1][1] == 8
assert result.json()[1]['columns'][0][1] == 1
assert result.json()[1]['columns'][2][1] == 1
assert result.json()[1]['columns'][1][1] == 9

#MLDB-234
try:
    mldb.get('/v1/query', q='SELECT x FROM dataset1 GROUP BY 1')
except mldb_wrapper.ResponseException as exc:
    mldb.log(exc.response.text)
else:
    assert False, 'should not be here'

try:
    mldb.get('/v1/query', q='SELECT x FROM dataset1 GROUP BY y')
except mldb_wrapper.ResponseException as exc:
    mldb.log(exc.response.text)
else:
    assert False, 'should not be here'

#MLDB-331
result = mldb.get('/v1/query', q='SELECT count(y) FROM dataset1')
mldb.log(result.text)
assert result.json()[0]['columns'][0][1] == 10

result = mldb.get('/v1/query', q='SELECT count(*) FROM dataset1')
mldb.log(result.text)
assert result.json()[0]['columns'][0][1] == 10

result = mldb.get('/v1/query', q='SELECT count(*) FROM dataset1 WHERE y = 3')
mldb.log(result.text)
assert result.json()[0]['columns'][0][1] == 0

result = mldb.get(
    '/v1/query',
    q='SELECT min(x) as empty_min, count(*) AS mycount FROM dataset1 WHERE y = 3')
mldb.log(result.text)
assert result.json()[0]['columns'][0][1] == None
assert result.json()[0]['columns'][1][1] == 0

result = mldb.get(
    '/v1/query',
    q='SELECT min(x) as mymin, count(*) AS mycount FROM dataset1 GROUP BY y')
mldb.log(result.text)

assert result.json()[0]['columns'][1][1] == 0
assert result.json()[0]['columns'][0][1] == 5
assert result.json()[1]['columns'][1][1] == 1
assert result.json()[1]['columns'][0][1] == 5

#MLDB-1599 clamp
result = mldb.query('SELECT clamp(x, 3, y + 3) FROM dataset1 ORDER BY x')
expected = [["_rowName","clamp(x, 3, y + 3)"],
    ["row_0",3],["row_1",3],["row_2",3],["row_3",3],["row_4",3],["row_5",4],["row_6",3],["row_7",4],["row_8",3],["row_9",4]]
assert result == expected

result = mldb.query('SELECT clamp({x, y}, 3, 7) FROM dataset1 ORDER BY x')
expected = [["_rowName","clamp({x, y}, 3, 7).x","clamp({x, y}, 3, 7).y"],
     ["row_0",3,3],["row_1",3,3],["row_2",3,3],["row_3",3,3],["row_4",4,3],["row_5",5,3],["row_6",6,3],["row_7",7,3],["row_8",7,3],["row_9",7,3]]
assert result == expected

result = mldb.query('SELECT clamp(null, 3, 7)')
expected = [["_rowName","clamp(null, 3, 7)"],["result", None]]
assert result == expected

result = mldb.query('SELECT clamp({null, 11}, 3, 7) as v')
mldb.log(result)
expected = [["_rowName","v.11","v.null"],["result",7,None]]
assert result == expected

result = mldb.query('SELECT clamp(NaN, 3, 7) as v')
expected = [["_rowName","v"],["result",3]]
assert result == expected 

result = mldb.query('SELECT clamp(-NaN, 3, 7) as v')
expected = [["_rowName","v"],["result",3]]
assert result == expected 

result = mldb.query('SELECT clamp(inf, 3, 7) as v')
expected = [["_rowName","v"],["result",7]]
assert result == expected 

result = mldb.query('SELECT clamp(-inf, 3, 7) as v')
expected = [["_rowName","v"],["result",3]]
assert result == expected 

result = mldb.query('SELECT clamp(2, null, 3) as v')
expected = [["_rowName","v"],["result",2]]
assert result == expected 

result = mldb.query('SELECT clamp(2, null, 1) as v')
expected = [["_rowName","v"],["result",1]]
assert result == expected

result = mldb.query('SELECT clamp(NaN, null, 1) as v')
expected = [["_rowName","v"],["result","NaN"]]
assert result == expected

result = mldb.query('SELECT clamp(-NaN, null, 1) as v')
expected = [["_rowName","v"],["result","-NaN"]]
assert result == expected

result = mldb.query('SELECT clamp(inf, null, 1) as v')
expected = [["_rowName","v"],["result",1]]
assert result == expected

result = mldb.query('SELECT clamp(-inf, null, 1) as v')
expected = [["_rowName","v"],["result","-Inf"]]
assert result == expected

result = mldb.query('SELECT clamp(null, null, 1) as v')
expected = [["_rowName","v"],["result",None]]
assert result == expected

result = mldb.query('SELECT clamp(-2, -1, null) as v')
expected = [["_rowName","v"],["result",-1]]
assert result == expected

result = mldb.query('SELECT clamp(0, -1, null) as v')
expected = [["_rowName","v"],["result",0]]
assert result == expected

result = mldb.query('SELECT clamp(2, -1, null) as v')
expected = [["_rowName","v"],["result",2]]
assert result == expected

result = mldb.query('SELECT clamp(nan, -1, null) as v')
expected = [["_rowName","v"],["result",-1]]
assert result == expected

result = mldb.query('SELECT clamp(-nan, -1, null) as v')
expected = [["_rowName","v"],["result",-1]]
assert result == expected

result = mldb.query('SELECT clamp(inf, -1, null) as v')
expected = [["_rowName","v"],["result","Inf"]]
assert result == expected

result = mldb.query('SELECT clamp(-inf, -1, null) as v')
expected = [["_rowName","v"],["result",-1]]
assert result == expected

result = mldb.query('SELECT clamp(null, -1, null) as v')
expected = [["_rowName","v"],["result",None]]
assert result == expected

result = mldb.query('SELECT clamp(2, null, null) as v')
expected = [["_rowName","v"],["result",2]]
assert result == expected

result = mldb.query('SELECT clamp(null, null, null) as v')
expected = [["_rowName","v"],["result",None]]
assert result == expected

result = mldb.query('SELECT clamp(nan, null, null) as v')
expected = [["_rowName","v"],["result","NaN"]]
assert result == expected

mldb.script.set_return('success')
