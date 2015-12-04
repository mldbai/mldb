# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

result = mldb.perform('GET', '/v1/query', [['q', 'select [3,2,1] as x']])
mldb.log(result)

assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][0] == 'x.000000'
assert json.loads(result['response'])[0]['columns'][0][1] == 3
assert json.loads(result['response'])[0]['columns'][1][0] == 'x.000001'
assert json.loads(result['response'])[0]['columns'][1][1] == 2
assert json.loads(result['response'])[0]['columns'][2][0] == 'x.000002'
assert json.loads(result['response'])[0]['columns'][2][1] == 1

result = mldb.perform('GET', '/v1/query', [['q', 'select vector_sum([1,2,3],[3,2,1]) as x']])
mldb.log(result)

assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][0] == 'x.000000'
assert json.loads(result['response'])[0]['columns'][0][1] == 4
assert json.loads(result['response'])[0]['columns'][1][0] == 'x.000001'
assert json.loads(result['response'])[0]['columns'][1][1] == 4
assert json.loads(result['response'])[0]['columns'][2][0] == 'x.000002'
assert json.loads(result['response'])[0]['columns'][2][1] == 4


mldb.script.set_return("success")
