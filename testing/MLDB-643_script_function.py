#
# MLDB-643_script_function.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

import datetime

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
mldb.log("data loader created dataset")

now = datetime.datetime.now()

for i in range(2):
    dataset.record_row("example-%d" % i, [["fwin", i, now],
                                          ["fwine", i*2, now]])

mldb.log("Committing dataset")
dataset.commit()


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

request.set_return(rtn)
"""
            }
        }
    }

script_func_output = mldb.put("/v1/functions/" + script_func_conf["id"],
                              script_func_conf)
mldb.log("The resulf of the script function creation "
         + script_func_output.text)
mldb.log("passed assert")


# now call the script
args = {"Warp": 9}
res = mldb.get("/v1/functions/" + script_func_conf["id"] + "/application",
               input={"args": args})
mldb.log("the result of calling the script ")
mldb.log(res.text)

mldb.log(res.json())
mldb.log(res.json()["output"]["return"])

assert res.json()["output"]["return"][0][0] == "Warp"
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

request.set_return(results)
"""
            }
        }
    }
script_func_output = mldb.put("/v1/functions/" + script_func_conf["id"],
                              script_func_conf)
mldb.log("The resulf of the script function creation "
         + script_func_output.text)
mldb.log("passed assert")


mldb.log("Querying dataset")
# requires "as args" because args is the input argument
select = "SELECT scriptApplier2({{*} as args})[{return}] as * from row_dataset({example0: { fwin:0, fwine: 0}, example1: { fwin: 1, fwine: 2}}) limit 10"

mldb.log(mldb.get("/v1/query", q="select * from toy").json())
mldb.log(mldb.get("/v1/query", q="select scriptApplier2({{fwin: 1, fwine: 2} as args})[{return}] as *").json())

select = "SELECT scriptApplier2({{*} as args})[{return}] as * from toy limit 10"
query_output = mldb.get("/v1/query", q=select,)

js_resp = query_output.json()
mldb.log(js_resp)

for row in js_resp:
    assert row["rowName"] in ["example-0", "example-1"]
    assert len(row["columns"]) == 2
    vals = {"return.fwine": 0, "return.fwin": 0}
    if row["rowName"] == "example-1":
        vals = {"return.fwine": 4, "return.fwin": 2}

    for col in row["columns"]:
        assert vals[col[0]] == col[1]


request.set_return("success")
