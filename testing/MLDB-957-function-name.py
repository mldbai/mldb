#
# MLDB-957-function-name.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example'
}

dataset = mldb.create_dataset(dataset_config)

dataset.record_row('row1', [[ "x", 15, 0 ]])

mldb.log("Committing dataset")
dataset.commit()

result = mldb.get('/v1/query', q='select power(x, 2) from example')
mldb.log(result)

assert result.json()[0]['columns'][0][1] == 225


try:
    mldb.get('/v1/query', q='select POWER(x, 2) from example')
except mldb_wrapper.ResponseException as exc:
    mldb.log(exc.response)
else:
    assert False, "should have failed"

mldb.script.set_return('success')
