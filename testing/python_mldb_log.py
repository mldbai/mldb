# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


###
#   test that mldb.log correnctly handles all types
###

import json

conf = {
    "source": """
mldb.log("patate")
mldb.log({"patate":2.44})
mldb.log(["patate", "pwel"])
mldb.log(25)
mldb.log("a", "b", 2)
mldb.log()
"""
}
rtn = mldb.perform("POST", "/v1/types/plugins/python/routes/run", [], conf)

jsRtn = json.loads(rtn["response"])
mldb.log(jsRtn)

assert jsRtn["logs"][0]["c"] == "patate"
assert jsRtn["logs"][1]["c"] == "{\n   \"patate\" : 2.440000057220459\n}\n"
assert jsRtn["logs"][2]["c"] == "[ \"patate\", \"pwel\" ]\n"
assert jsRtn["logs"][3]["c"] == "25"
assert jsRtn["logs"][4]["c"] == "a b 2"
#assert jsRtn["logs"][5]["c"] == ""

mldb.script.set_return("success")

