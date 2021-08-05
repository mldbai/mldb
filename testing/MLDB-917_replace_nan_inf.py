#
# MLDB-917_replace_nan_inf.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import datetime

from mldb import mldb

nans = { "NaN", "-NaN" }

rez = mldb.get("/v1/query", q="select 0/0")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1]["num"] in nans for x in js_rez[0]["columns"]] == [True]

rez = mldb.get("/v1/query", q="select replace_nan(0/0, 5)")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1] for x in js_rez[0]["columns"]] == [5]

rez = mldb.get("/v1/query", q="select replace_nan({0/0, 2, 6}, 5)")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1] for x in js_rez[0]["columns"]] == [5, 2, 6]

rez = mldb.get("/v1/query", q="select 1/0")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1]["num"] for x in js_rez[0]["columns"]] == ["Inf"]

rez = mldb.get("/v1/query", q="select replace_inf(1/0, 98)")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1] for x in js_rez[0]["columns"]] == [98]

rez = mldb.get("/v1/query", q="select replace_inf([1/0, 5/0, 23], 98)")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1] for x in js_rez[0]["columns"]] == [98, 98, 23]

rez = mldb.get("/v1/query", q="select replace_not_finite([1/0, 0/0, -1/0, -0/0, 23], 98)")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1] for x in js_rez[0]["columns"]] == [98, 98, 98, 98, 23]

rez = mldb.get("/v1/query", q="select replace_null([1/0, 0/0, -1/0, -0/0, null, 23], 98)")
js_rez = rez.json()
mldb.log(js_rez)
assert js_rez[0]["columns"][0][1]["num"] == "Inf"
assert js_rez[0]["columns"][1][1]["num"] in nans
assert js_rez[0]["columns"][2][1]["num"] == "-Inf"
assert js_rez[0]["columns"][3][1]["num"] in nans
assert js_rez[0]["columns"][4][1] == 98
assert js_rez[0]["columns"][5][1] == 23

# Create toy dataset
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

rez = mldb.get("/v1/query", q="select * from toy", format="sparse")
js_rez = rez.json()
mldb.log(js_rez)

rez = mldb.get("/v1/query",
               q="select replace_inf(replace_nan({*}, 0), 1) from toy")
js_rez = rez.json()
mldb.log(js_rez)
assert [x[1] for x in js_rez[0]["columns"]] == [54, 0, 1]

request.set_return("success")
