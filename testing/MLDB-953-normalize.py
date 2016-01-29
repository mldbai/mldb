#
# MLDB-953-normalize.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'beh.mutable',
    'id'      : 'example'
}

result = mldb.get('/v1/query', q='select normalize({1, 2, 3, 4}, 1)')
mldb.log(result)

result = mldb.get('/v1/query', q='select norm(normalize({1, 2, 3, 4}, 1), 1)')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

result = mldb.get('/v1/query', q='select normalize([1, 2, 3, 4], 1)')
mldb.log(result)

result = mldb.get('/v1/query', q='select norm(normalize([1, 2, 3, 4], 1), 1)')
mldb.log(result)
assert 0.999 < result.json()[0]['columns'][0][1] < 1.001

mldb.script.set_return('success')
