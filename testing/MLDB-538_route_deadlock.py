# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


# surprisingly enough, this works: a python script calling a python script !
result =mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], {"source":'''
print mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], {"source":"print 1"})
'''})
assert result["statusCode"] < 400, result["response"]


# we create a plugin which declares 2 routes: /deadlock calls /deadlock2

result = mldb.perform("PUT", "/v1/plugins/deadlocker", [], {
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
assert result["statusCode"] < 400, result["response"]

# we call /deadlock, and we deadlock :)
result = mldb.perform("GET", "/v1/plugins/deadlocker/routes/deadlock", [], {})
assert result["statusCode"] < 400, result["response"]

mldb.script.set_return("success")
