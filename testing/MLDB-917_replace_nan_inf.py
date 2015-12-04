# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json, random, datetime

rez = mldb.perform("GET", "/v1/query", [["q", "select 0/0"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1]["num"] for x in jsRez[0]["columns"]] == ["-NaN"]

rez = mldb.perform("GET", "/v1/query", [["q", "select replace_nan(0/0, 5)"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1] for x in jsRez[0]["columns"]] == [5]

rez = mldb.perform("GET", "/v1/query", [["q", "select replace_nan({0/0, 2, 6}, 5)"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1] for x in jsRez[0]["columns"]] == [5, 2, 6]

rez = mldb.perform("GET", "/v1/query", [["q", "select 1/0"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1]["num"] for x in jsRez[0]["columns"]] == ["Inf"]

rez = mldb.perform("GET", "/v1/query", [["q", "select replace_inf(1/0, 98)"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1] for x in jsRez[0]["columns"]] == [98]

rez = mldb.perform("GET", "/v1/query", [["q", "select replace_inf({1/0, 5/0, 23}, 98)"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1] for x in jsRez[0]["columns"]] == [98, 98, 23]



## Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()
dataset.record_row("row1", [["feat1", 54, now],
                            ["feat2", float("nan"), now],
                            ["label", float("inf"), now]])
dataset.commit()

rez = mldb.perform("GET", "/v1/query", [["q", "select * from toy"], ["format", "sparse"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

rez = mldb.perform("GET", "/v1/query", [["q", "select replace_inf(replace_nan({*}, 0), 1) from toy"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert [x[1] for x in jsRez[0]["columns"]] == [54, 0, 1]



mldb.script.set_return("success")

