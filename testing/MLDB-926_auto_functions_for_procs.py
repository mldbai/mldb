#
# Test if all procedures,s functionName parameter works correctly
# Francois Maillet, 22 sept 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import random, datetime

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

# Create toy dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : "toy"
}


dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

for i in xrange(50):
    label = random.random() < 0.2
    dataset.record_row("u%d" % i, [["feat1", random.gauss(5 if label else 15, 3), now],
                                   ["feat2", random.gauss(-5 if label else 10, 10), now],
                                   ["feat3", random.gauss(52 if label else 30, 40), now],
                                   ["label", label, now]])

dataset.commit()


def do_checks(conf):
    mldb.log(">> Checking " + conf["type"])
    rez = mldb.put("/v1/procedures/" + conf["type"], conf)
    mldb.log(rez)

    rez = mldb.post("/v1/procedures/"+conf["type"] + "/runs")
    mldb.log(rez)

    rez = mldb.get("/v1/functions/" + conf["params"]["functionName"])
    mldb.log(rez)

# classifier.train -> classifier
conf = {
    "type": "classifier.train",
    "params": {
        "trainingData":
            "select {* EXCLUDING(label)} as features, label from toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya.cls",
        "algorithm": "glz",
        "mode": "boolean",
        "configuration": {
            "glz": {
                "type": "glz",
                "verbosity": 3,
                "normalize": False,
               "regularization": 'l2'
            }
        },
        "functionName": "cls_func"
    }
}
do_checks(conf)

# kmeans.train -> kmeans
conf = {
    "type": "kmeans.train",
    "params": {
        "trainingData": "select * excluding(label) from toy",
        "modelFileUrl": "file://tmp/MLDB-926.mks",
        "centroidsDataset": {"id": "kmean_out", "type": "sparse.mutable" },
        "functionName": "kmeans_func"
    }
}
do_checks(conf)

# test also the error code returned
del conf['params']['modelFileUrl']
conf['params']['runOnCreation'] = True
try:
    mldb.put("/v1/procedures/failing_kmeans", conf)
except mldb_wrapper.ResponseException as exc:
    rez = exc.response
else:
    assert False, 'should not be here 1'
response = rez.json()
mldb.log(response)

assert rez.status_code == 400, 'expecting call to fail when no model file URL'
assert 'error' in response, 'expecting the error message to appear'
assert 'httpCode' in response, 'expecting an httpCode for the run error'

conf['params']['modelFileUrl'] = "not://a/valid/path"
conf['params']['runOnCreation'] = True
try:
    mldb.put("/v1/procedures/failing_kmeans2", conf)
except mldb_wrapper.ResponseException as exc:
    rez = exc.response
else:
    assert False, 'should not be here 2'
response = rez.json()
mldb.log(response)

assert rez.status_code == 400, 'expecting call to fail when no model file URL'
assert 'error' in response, 'expecting the error message to appear'
assert 'httpCode' in response, 'expecting an httpCode for the run error'


# probabilizer.train -> probabilizer
conf = {
    "type": "probabilizer.train",
    "params": {
        "trainingData":
            "select cls_func({{* EXCLUDING(label)} as features})[score] as score, label from toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya-proba.json",
        "functionName": "probabilizer_func"
    }
}
do_checks(conf)


# svd.train -> svd.embedRow
conf = {
    "type": "svd.train",
    "params": {
        "trainingData": "select * from toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya-svd.model",
        "functionName": "svd_func"
    }
}
do_checks(conf)



# tsne.train -> tsne.embedRow
conf = {
    "type": "tsne.train",
    "params": {
        "trainingData": "select * from toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya-tsne.model",
        "numOutputDimensions": 2,
        "functionName": "tsne_func"
    }
}
do_checks(conf)

mldb.script.set_return("success")
