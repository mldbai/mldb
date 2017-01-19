#
# filename
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import random

mldb = mldb_wrapper.wrap(mldb) # noqa

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

for i in xrange(50):
    ds1.record_row('row_' + str(i),
                   [['x', random.random(), 0], ['y', random.random(), 0]])
ds1.commit()

svd_file = 'file://tmp/MLDB-805.svd'
mldb.put('/v1/procedures/train_svd', {
    'type': 'svd.train',
    'params': {
        'trainingData': 'select * from ds1',
        'modelFileUrl': svd_file,
        'numSingularValues': 5,
        'numDenseBasisVectors': 10}})

mldb.post('/v1/procedures/train_svd/runs', {})

mldb.put('/v1/functions/embed', {
    'type': 'svd.embedRow',
    'params': {
        'modelFileUrl': svd_file}})

res = mldb.get('/v1/functions/embed/info')
mldb.log(res.json())

# verify that the function works
res = mldb.get('/v1/functions/embed/application',
               input={'row': { 'x': .5, 'y': .5}})

# now use it in a query
res = mldb.get('/v1/query', q='SELECT embed({{*} as row}) from ds1')

mldb.script.set_return('success')
