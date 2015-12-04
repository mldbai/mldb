# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

for i in xrange(1):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0]])
ds1.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT { 1 as x } as y FROM dataset1']])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][0] == 'y.x'

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT { 1 as x } as y']])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][0] == 'y.x'

mldb.script.set_return('success')  
