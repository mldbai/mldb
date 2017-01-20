#
# MLDB-538_route_deadlock.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa


# surprisingly enough, this works: a python script calling a python script !
result = mldb.post("/v1/types/plugins/python/routes/run", {"source":'''
print mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], {"source":"print 1"})
'''})

# we create a plugin which declares 2 routes: /deadlock calls /deadlock2
result = mldb.put("/v1/plugins/deadlocker", {
    "type": "python",
    "params":{
        "source":{
            "routes":
"""

rp = mldb.plugin.rest_params
if str(rp.verb) == "GET" and str(rp.remaining) == "/deadlock":
    rval = mldb.perform("GET", "/v1/plugins/deadlocker/routes/deadlock2", [], {})
    mldb.plugin.set_return(rval)
else:
    mldb.plugin.set_return("phew")

"""}}})

# we call /deadlock, and we deadlock :)
result = mldb.get("/v1/plugins/deadlocker/routes/deadlock")

mldb.script.set_return("success")
