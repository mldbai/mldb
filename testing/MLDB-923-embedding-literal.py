#
# MLDB-923-embedding-literal.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

result = mldb.get('/v1/query', q='select [3,2,1] as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 3
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 2
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 1

result = mldb.get('/v1/query', q='select {"0":3, "1":2, "2":1} as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 3
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 2
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 1

result = mldb.get('/v1/query', q='select vector_sum([1,2,3],[3,2,1]) as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 4
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 4
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 4

# Order of declaration of elements shouldn't matter
result = mldb.get('/v1/query', q='select vector_sum([1,2,3],{"0":3, "1":2, "2":1}) as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 4
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 4
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 4

result = mldb.get('/v1/query', q='select [1,2,3] + [3,2,1] as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 4
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 4
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 4

result = mldb.get('/v1/query', q='select [1,2,3] + {"0":3, "1":2, "2":1} as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 4
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 4
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 4

result = mldb.get('/v1/query', q='select [1,2,3] + {"2":1, "1":2, "0":3} as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 4
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 4
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 4

result = mldb.get('/v1/query', q='select {"2":1, "1":2, "0":3} + [1,2,3] as x')
mldb.log(result)

response = result.json()
assert response[0]['columns'][0][0] == 'x.0'
assert response[0]['columns'][0][1] == 4
assert response[0]['columns'][1][0] == 'x.1'
assert response[0]['columns'][1][1] == 4
assert response[0]['columns'][2][0] == 'x.2'
assert response[0]['columns'][2][1] == 4

mldb.script.set_return("success")
