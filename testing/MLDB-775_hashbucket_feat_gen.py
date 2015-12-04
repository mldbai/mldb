# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import datetime, json

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
mldb.log("data loader created dataset")

now = datetime.datetime.now()

for i in xrange(5):
    dataset.record_row("example-%d" % i, [["fwin", i, now],
                                          ["fwine", i*2, now]])

mldb.log("Committing dataset")
dataset.commit()



#add an explain function
script_func_conf = {
    "id":"featHasher",
    "type":"experimental.feature_generator.hashed_column",
    "params": {
        "numBits": 8
    }
}
script_func_output = mldb.perform("PUT", "/v1/functions/" + script_func_conf["id"], [], 
                                    script_func_conf)
mldb.log("The resulf of the script function creation " + json.dumps(script_func_output))
assert script_func_output["statusCode"] < 400
mldb.log("passed assert")





# requires "as args" because args is the input pin
select = "featHasher({{*} as columns})[hash]";

queryOutput = mldb.perform("GET", "/v1/datasets/toy/query", [["select", select], ["limit", "10" ]])

jsResp = json.loads(queryOutput["response"])
#mldb.log(jsResp)

for line in jsResp:
    assert len(line["columns"]) == 256
    mldb.log(line)
    mldb.log("----")


mldb.script.set_return("success")

