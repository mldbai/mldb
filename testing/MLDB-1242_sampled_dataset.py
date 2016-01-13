# ##
# Francois Maillet, 11 janvier 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
# ##

import json, random, datetime, os

## Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : "toy"
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

for i in xrange(500):
    dataset.record_row("u%d" % i, [["feat1", random.gauss(5, 3), now]])

dataset.commit()

sampled_dataset_conf = {
    "type": "sampled",
    "params": {
        "dataset": {"id": "toy"},
        "rows": 10
    }
}
rez = mldb.perform("PUT", "/v1/datasets/pwet", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))


rez = mldb.perform("GET", "/v1/query", [["q", "SELECT * FROM pwet"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert len(jsRez) == 10


## too many requested rows without sampling
sampled_dataset_conf = {
    "type": "sampled",
    "params": {
        "dataset": {"id": "toy"},
        "rows": 25000,
        "withReplacement": False
    }
}
rez = mldb.perform("PUT", "/v1/datasets/patate", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))
assert rez["statusCode"] == 400

sampled_dataset_conf["params"]["withReplacement"] = True
rez = mldb.perform("PUT", "/v1/datasets/patate", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))
assert rez["statusCode"] == 201


# with fraction
sampled_dataset_conf = {
    "type": "sampled",
    "params": {
        "dataset": {"id": "toy"},
        "fraction": 0.5
    }
}
rez = mldb.perform("PUT", "/v1/datasets/pwet", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))


rez = mldb.perform("GET", "/v1/query", [["q", "SELECT * FROM pwet"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert len(jsRez) == 250


sampled_dataset_conf["params"]["fraction"] = 5
rez = mldb.perform("PUT", "/v1/datasets/pwet", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))
assert rez["statusCode"] == 400

sampled_dataset_conf["params"]["fraction"] = 0
rez = mldb.perform("PUT", "/v1/datasets/pwet", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))
assert rez["statusCode"] == 400

sampled_dataset_conf["params"]["fraction"] = -1
rez = mldb.perform("PUT", "/v1/datasets/pwet", [], sampled_dataset_conf)
mldb.log(json.loads(rez["response"]))
assert rez["statusCode"] == 400



rez = mldb.perform("GET", "/v1/query", [["q", "select * from sample(toy, {rows: 25000, withReplacement: 1})"]])
assert rez["statusCode"] == 200
assert len(json.loads(rez["response"])) == 25000

rez = mldb.perform("GET", "/v1/query", [["q", "select * from sample(toy, {rows: 25})"]])
mldb.log(json.loads(rez["response"]))
assert rez["statusCode"] == 200
assert len(json.loads(rez["response"])) == 25

# test seed works
rez = mldb.perform("GET", "/v1/query", [["q", "select * from sample(toy, {rows: 1, seed: 5})"]])
rez2 = mldb.perform("GET", "/v1/query", [["q", "select * from sample(toy, {rows: 1, seed: 5})"]])
assert rez["statusCode"] == 200
assert json.loads(rez["response"])[0] == json.loads(rez2["response"])[0]

rez = mldb.perform("GET", "/v1/query", [["q", "select * from sample(toy, {rows: 1})"]])
rez2 = mldb.perform("GET", "/v1/query", [["q", "select * from sample(toy, {rows: 1})"]])
assert rez["statusCode"] == 200
assert json.loads(rez["response"])[0] != json.loads(rez2["response"])[0]


mldb.script.set_return("success")

