#
# MLDB-881-DELETE-fails-on-in-construction.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

from mldb import mldb, ResponseException

# Do non-trivial work as we don't want trival creation to race with returning
# the first status call

config = {
    "type": "python",
    "params":{
        "source":{
            "routes": "from mldb import mldb\nmldb.log(str(request.rest_params))\nmldb.log(str(request.payload))\nrequest.set_return({'args': request.rest_params, 'payload': request.payload})"
        }
    }
}

# create an expensive resource async
resp = mldb.put_async("/v1/plugins/dummy2", config)

assert resp.json()['state'] == 'initializing', \
    'the resource should still be under construction'

# deleting that resource will wait until it is constructed
resp = mldb.delete_async("/v1/plugins/dummy2")
assert resp.status_code == 204

# once the DELETE returns the resource should have been deleted
try:
    mldb.get("/v1/plugins/dummy2")
except ResponseException:
    pass
else:
    assert False, 'should not be here'

request.set_return('success')
