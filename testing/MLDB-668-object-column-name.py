#
# MLDB-668-object-column-name.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

for i in xrange(1):
    ds1.record_row('row_' + str(i),
                   [['x', i, 0]])
ds1.commit()

result = mldb.get('/v1/query', q='SELECT { 1 as x } as y FROM dataset1')
mldb.log(result.text)
assert result.json()[0]['columns'][0][0] == 'y.x'

result = mldb.get('/v1/query', q='SELECT { 1 as x } as y')
mldb.log(result.text)
assert result.json()[0]['columns'][0][0] == 'y.x'

mldb.script.set_return('success')
