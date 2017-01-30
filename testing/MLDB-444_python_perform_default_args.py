# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


import json

result = mldb.perform("PUT", "/v1/plugins/mirror", [], {
    "type": "python",
    "params":{
        "source":{
            "routes":
"""

mldb.log(str(mldb.plugin.rest_params.rest_params))
mldb.log(str(mldb.plugin.rest_params.payload))

mldb.plugin.set_return({
    "args": mldb.plugin.rest_params.rest_params,
    "payload": mldb.plugin.rest_params.payload
})
"""}}})
assert result["statusCode"] < 400, result["response"]

successes = 0

rtn = mldb.perform("POST", "/v1/plugins/mirror/routes/pwet")
if rtn["response"] == '{"args":[],"payload":"null\\n"}':
    successes += 1

rtn = mldb.perform("POST", "/v1/plugins/mirror/routes/pwet", [["patate", "5"]])
mldb.log(str(rtn))
if rtn["response"] == '{"args":[["patate","5"]],"payload":"null\\n"}':
    successes += 1

rtn = mldb.perform("POST", "/v1/plugins/mirror/routes/pwet", [["patate", "10"]], {"ataboy": 5})
mldb.log(str(rtn))
if rtn["response"] == '{"args":[["patate","10"]],"payload":"{\\"ataboy\\":5}\\n"}':
    successes += 1

if successes == 3:
    mldb.script.set_return("success")
