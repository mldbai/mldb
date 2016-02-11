#
# MLDB-953-normalize.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
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

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], 0), 0) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], inf), inf) as x')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

#result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],0) - [1,2,3,4]/norm([1,2,3,4],0) )')
#assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],1) - [1,2,3,4]/norm([1,2,3,4],1) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],2) - [1,2,3,4]/norm([1,2,3,4],2) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001
result = mldb.get('/v1/query', q='select horizontal_sum( normalize([1,2,3,4],inf) - [1,2,3,4]/norm([1,2,3,4],inf) )')
assert -0.00001 < result.json()[0]['columns'][0][1] < 0.00001


mldb.script.set_return('success')
