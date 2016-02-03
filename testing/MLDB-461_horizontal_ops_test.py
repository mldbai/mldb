#
# MLDB-461_horizontal_ops_test.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

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


def check_res(rez, answers):
    resp = rez.json()
    mldb.log(resp)

    assert len(resp) == 3
    for r in resp:
        key = r["rowName"]
        assert abs(r["columns"][0][1] - answers[key]) < 0.001


rez = mldb.get("/v1/query", q="select horizontal_count({*}) from x")
answers = {
        "x": 3,
        "y": 3,
        "z": 2
}
check_res(rez, answers)



rez = mldb.get("/v1/query", q="select horizontal_count({p*}) from x")
answers = {
        "x": 1,
        "y": 1,
        "z": 0
}
check_res(rez, answers)



rez = mldb.get("/v1/query", q="select horizontal_sum({*}) from x")
answers = {
        "x": 3,
        "y": 2,
        "z": 6
}
check_res(rez, answers)


rez = mldb.get("/v1/query", q="select horizontal_avg({*}) from x")
answers = {
        "x": 1,
        "y": 0.667,
        "z": 3
}
check_res(rez, answers)

rez = mldb.get("/v1/query", q="select horizontal_min({*}) from x")
answers = {
        "x": 1,
        "y": 0,
        "z": 1
}
check_res(rez, answers)

rez = mldb.get("/v1/query", q="select horizontal_max({*}) from x")
answers = {
        "x": 1,
        "y": 1,
        "z": 5
}
check_res(rez, answers)

mldb.script.set_return("success")
