#
# MLDB-953-normalize.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

result = mldb.get('/v1/query', q='select normalize({1, 2, 3, 4}, 1) as x')
mldb.log(result)

result = mldb.get('/v1/query', q='select norm(normalize({1, 2, 3, 4}, 1), 1) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select normalize([1, 2, 3, 4], 1) as x')
mldb.log(result)

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], 1), 1) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], 2), 2) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], 3.2), 3.2) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], 0), 0) as x')
mldb.log(result)
# NOTE: the 0-norm is the number of non-zero elements, so normalize
# followed by norm can't give 1.0
assert 3.999 < result.json()[0]['columns'][0][1] < 4.001

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], inf), inf) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select norm([1,2,0,4],0)')
assert result.json()[0]['columns'][0][1] == 3
result = mldb.get('/v1/query', q='select norm([1,2,0,4],1)')
assert result.json()[0]['columns'][0][1] == 7
result = mldb.get('/v1/query', q='select norm([1,2,0,4],2)')
assert -0.00001 < result.json()[0]['columns'][0][1] - 4.582575 < 0.00001
result = mldb.get('/v1/query', q='select norm([1,2,0,4],inf)')
assert result.json()[0]['columns'][0][1] == 4


result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],0) - [1,2,3,4]/norm([1,2,3,4],0) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],1) - [1,2,3,4]/norm([1,2,3,4],1) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],2) - [1,2,3,4]/norm([1,2,3,4],2) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],3.2) - [1,2,3,4]/norm([1,2,3,4],3.2) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],inf) - [1,2,3,4]/norm([1,2,3,4],inf) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001


mldb.script.set_return('success')
