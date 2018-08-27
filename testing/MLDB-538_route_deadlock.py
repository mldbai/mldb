#
# MLDB-538_route_deadlock.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
from mldb import mldb


# surprisingly enough, this works: a python script calling a python script !
result = mldb.post("/v1/types/plugins/python/routes/run", {"source":'''
from mldb import mldb
print(mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], {"source":"print(1)"}))
'''})

# we create a plugin which declares 2 routes: /deadlock calls /deadlock2
result = mldb.put("/v1/plugins/deadlocker", {
    "type": "python",
    "params":{
        "source":{
            "routes":
"""
from mldb import mldb
mldb.log("got request " + request.verb + " " + request.remaining)
rp = request
if str(rp.verb) == "GET" and str(rp.remaining) == "/deadlock":
    rval = mldb.perform("GET", "/v1/plugins/deadlocker/routes/deadlock2", [], {})
    request.set_return(rval)
else:
    request.set_return("phew")

"""}}})

# we call /deadlock, and we must not deadlock...
result = mldb.get("/v1/plugins/deadlocker/routes/deadlock")

request.set_return("success")
