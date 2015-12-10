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


def checkRes(rez, answers):
    resp = json.loads(rez["response"])
    mldb.log(resp)

    assert len(resp) == 3
    for r in resp:
        key = r["rowName"]
        assert abs(r["columns"][0][1] - answers[key]) < 0.001


rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_count({*}) from x"]])
answers = {
        "x": 3,
        "y": 3,
        "z": 2
}
checkRes(rez, answers)



rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_count({p*}) from x"]])
answers = {
        "x": 1,
        "y": 1,
        "z": 0
}
checkRes(rez, answers)



rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_sum({*}) from x"]])
answers = {
        "x": 3,
        "y": 2,
        "z": 6
}
checkRes(rez, answers)


rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_avg({*}) from x"]])
answers = {
        "x": 1,
        "y": 0.667,
        "z": 3
}
checkRes(rez, answers)

rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_min({*}) from x"]])
answers = {
        "x": 1,
        "y": 0,
        "z": 1
}
checkRes(rez, answers)

rez = mldb.perform("GET", "/v1/query", [["q", "select horizontal_max({*}) from x"]])
answers = {
        "x": 1,
        "y": 1,
        "z": 5
}
checkRes(rez, answers)




mldb.script.set_return("success")

