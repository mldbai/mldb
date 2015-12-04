# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

dataset_config = {
    'type'    : 'beh.mutable',
    'id'      : 'example'
}

result = mldb.perform('GET', '/v1/query', [['q', 'select normalize({1, 2, 3, 4}, 1)']])
mldb.log(result)
assert result['statusCode'] == 200

result = mldb.perform('GET', '/v1/query', [['q', 'select norm(normalize({1, 2, 3, 4}, 1), 1)']])
mldb.log(result)
assert 0.999 < json.loads(result['response'])[0]['columns'][0][1] < 1.001

result = mldb.perform('GET', '/v1/query', [['q', 'select normalize([1, 2, 3, 4], 1)']])
mldb.log(result)
assert result['statusCode'] == 200

result = mldb.perform('GET', '/v1/query', [['q', 'select norm(normalize([1, 2, 3, 4], 1), 1)']])
mldb.log(result)
assert 0.999 < json.loads(result['response'])[0]['columns'][0][1] < 1.001

mldb.script.set_return('success')