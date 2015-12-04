# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

def QueryAndCheck(query, expectedSize):
    mldb.log(query)
    result = mldb.perform('GET', '/v1/query', [['q', query]])
    mldb.log(result)
    assert(result['statusCode'] == 200)
    response = json.loads(result['response']);
    assert len(response) == expectedSize;

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

for i in xrange(5):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0], ['y', i*2, 0]])
ds1.commit()

QueryAndCheck('SELECT * FROM ds1 WHERE y IN (4,8)', 2)

QueryAndCheck('SELECT * FROM ds1 WHERE y NOT IN (4,8)', 3)

QueryAndCheck('SELECT x FROM ds1 WHERE x IN (SELECT y from ds1)', 3)

QueryAndCheck('SELECT x FROM ds1 WHERE x NOT IN (SELECT y from ds1)', 2)

# Then check that there are no datasets besides the two that we explicitly created
result = mldb.perform('GET', '/v1/datasets')
response = json.loads(result['response']);
assert len(response) == 1;

mldb.script.set_return('success')

