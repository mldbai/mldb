# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

conf = {
    "source": "a=1"
}
rtn = mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], conf)
mldb.log(rtn["response"])

conf = {
    "source": "print a"
}
rtn = mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], conf)

jsRtn = json.loads(rtn["response"])
mldb.log(jsRtn)

assert jsRtn["exception"]["message"] == "name 'a' is not defined"

mldb.script.set_return("success")

