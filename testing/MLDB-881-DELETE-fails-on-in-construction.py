#
# MLDB-881-DELETE-fails-on-in-construction.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


# create an expensive resource async
resp = mldb.put_async("/v1/datasets/dummy2", {
    'type' : 'import.text',
    'params' : {
        'dataFileUrl': 'http://public.mldb.ai/reddit.csv.gz'
    }
})
assert resp.json()['state'] == 'initializing', \
    'the resource should still be under construction'

# deleting that resource will wait until it is constructed
resp = mldb.delete_async("/v1/datasets/dummy2")
assert resp.status_code == 204

# once the DELETE returns the resource should have been deleted
try:
    mldb.get("/v1/datasets/dummy2")
except mldb_wrapper.ResponseException:
    pass
else:
    assert False, 'should not be here'

mldb.script.set_return('success')
