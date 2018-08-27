#
# MLDB-668-object-column-name.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
from mldb import mldb

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

for i in range(1):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0]])
ds1.commit()

result = mldb.get('/v1/query', q='SELECT { 1 as x } as y FROM dataset1')
mldb.log(result.text)
assert result.json()[0]['columns'][0][0] == 'y.x'

result = mldb.get('/v1/query', q='SELECT { 1 as x } as y')
mldb.log(result.text)
assert result.json()[0]['columns'][0][0] == 'y.x'

request.set_return('success')
