# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json, datetime

dataset = mldb.create_dataset({
    "type": "sparse.mutable",
    "id": "x"
})

now = datetime.datetime.now()

dataset.record_row("x",[
    ["col1", 1, now],
    ["col2", 1, now],
    ["pwet", 1, now]])
dataset.record_row("y",[
    ["col1", 0, now],
    ["col2", 1, now],
    ["prout", 1, now]])
dataset.record_row("z",[
    ["col1", 5, now],
    ["col2", 1, now]])

dataset.commit()


rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_count({*}) from x"]])
resp = json.loads(rez["response"])
mldb.log(resp)


answers = {
        "x": 3,
        "y": 3,
        "z": 2
}
assert len(resp) == 3
for r in resp:
    key = r["rowName"]
    assert r["columns"][0][1] == answers[key]



rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_count({p*}) from x"]])
resp = json.loads(rez["response"])
mldb.log(resp)
answers = {
        "x": 1,
        "y": 1,
        "z": 0
}
assert len(resp) == 3
for r in resp:
    key = r["rowName"]
    assert r["columns"][0][1] == answers[key]



rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_sum({*}) from x"]])
resp = json.loads(rez["response"])
mldb.log(resp)

assert len(resp) == 3
answers = {
        "x": 3,
        "y": 2,
        "z": 6
}

for r in resp:
    key = r["rowName"]
    assert r["columns"][0][1] == answers[key]


mldb.script.set_return("success")

