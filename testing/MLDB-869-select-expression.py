# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

for i in xrange(10):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0]])
ds1.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT 5 golden rings, 3 french hens FROM dataset1']])

mldb.log(result)
assert result['statusCode'] >= 400

#MLDB-835
result = mldb.perform('GET', '/v1/query', [['q', 'SELECT x.* FROM dataset1']])

mldb.log(result)
assert result['statusCode'] >= 400

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT x.* FROM dataset1 as x']])

mldb.log(result)
assert result['statusCode'] == 200

#MLDB-958 rowhash printing when rowhash bigger than 7FFFFFFFFFFFFFFF
result = mldb.perform('GET', '/v1/query', [['q', 'SELECT rowHash() FROM dataset1 as x where x = 1']])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 17390182720330652622

mldb.script.set_return('success')
