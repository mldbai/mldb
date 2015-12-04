# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json
import datetime

#add an explain function
script_func_conf = {
    "id":"scriptApplier",
    "type":"script.apply",
    "params": {
            "language": "python",
            "scriptConfig": {
                "source": """
mldb.log(str(mldb.script.args))

rtn = [[mldb.script.args[0][0][0], mldb.script.args[0][0][1][0], mldb.script.args[0][0][1][1]]];
    
mldb.script.set_return(rtn)
"""
            }
        }
    }
script_func_output = mldb.perform("PUT", "/v1/functions/" + script_func_conf["id"], [], 
                                    script_func_conf)
mldb.log("The resulf of the script function creation " + json.dumps(script_func_output))
assert script_func_output["statusCode"] < 400
mldb.log("passed assert")


# now call the script
args = {"Warp": 9}
rest_params = [["input", { "args": args}]]

res = mldb.perform("GET", "/v1/functions/"+script_func_conf["id"]+"/application", rest_params)
mldb.log("the result of calling the script ")
mldb.log(json.dumps(res))

mldb.log(json.loads(res["response"]))
mldb.log(json.loads(res["response"])["output"]["return"])

assert json.loads(res["response"])["output"]["return"][0][0] == "Warp"
mldb.log("passed assert")



###
###

#add an explain function
script_func_conf = {
    "id":"scriptApplier2",
    "type":"script.apply",
    "params": {
            "language": "python",
            "scriptConfig": {
                "source": """

mldb.log(mldb.script.args)
results = []
for colName, cellValue in mldb.script.args[0]:
    results.append([colName, cellValue[0]*2, cellValue[1]])


mldb.log("returning:")
mldb.log(results)

mldb.script.set_return(results)
"""
            }
        }
    }
script_func_output = mldb.perform("PUT", "/v1/functions/" + script_func_conf["id"], [], 
                                    script_func_conf)
mldb.log("The resulf of the script function creation " + json.dumps(script_func_output))
assert script_func_output["statusCode"] < 400
mldb.log("passed assert")



dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
mldb.log("data loader created dataset")

now = datetime.datetime.now()

for i in xrange(2):
    dataset.record_row("example-%d" % i, [["fwin", i, now],
                                          ["fwine", i*2, now]])

mldb.log("Committing dataset")
dataset.commit()

# requires "as args" because args is the input argument
select = "scriptApplier2({{*} as args})[{return}] as *"

queryOutput = mldb.perform("GET", "/v1/datasets/toy/query", [["select", select], ["limit", "10" ]])

jsResp = json.loads(queryOutput["response"])
mldb.log(jsResp)

for row in jsResp:
    assert row["rowName"] in ["example-0", "example-1"]
    assert len(row["columns"]) == 2
    vals = {"return.fwine": 0, "return.fwin": 0}
    if row["rowName"] == "example-1":
        vals = {"return.fwine": 4, "return.fwin": 2}

    for col in row["columns"]:
        assert vals[col[0]] == col[1]


mldb.script.set_return("success")
