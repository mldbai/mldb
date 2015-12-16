# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#####
#   Test if all procedures,s functionName parameter works correctly
#   Francois Maillet, 22 sept 2015
#   Copyright Datacratic 2015
####

import json, random, datetime

## Create toy dataset
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


def doChecks(conf):
    mldb.log(">> Checking " + conf["type"])
    rez = mldb.perform("PUT", "/v1/procedures/" + conf["type"], [], conf)
    mldb.log(rez)

    rez = mldb.perform("POST", "/v1/procedures/"+conf["type"]+"/runs")
    mldb.log(rez)
    assert rez["statusCode"] == 201

    rez = mldb.perform("GET", "/v1/functions/"+conf["params"]["functionName"])
    mldb.log(rez)
    assert rez["statusCode"] == 200



# classifier.train -> classifier
conf = {
    "type": "classifier.train",
    "params": {
        "trainingData": "select {* EXCLUDING(label)} as features, label from toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya.cls",
        "algorithm": "glz",
        "mode": "boolean",
        "configuration": {
            "glz": {
                "type": "glz",
                "verbosity": 3,
                "normalize": False,
                "link": "linear",
                "ridge_regression": True
            }
        },
        "functionName": "cls_func"
    }
}
doChecks(conf)

# kmeans.train -> kmeans
conf = {
    "type": "kmeans.train",
    "params": {
        "trainingData": "select * excluding(label) from toy",
        "centroidsDataset": {"id": "kmean_out", "type": "sparse.mutable" },
        "functionName": "kmeans_func"
    }
}
doChecks(conf)


# probabilizer.train -> probabilizer
conf = {
    "type": "probabilizer.train",
    "params": {
        "trainingDataset": "toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya-proba.json",
        "select": "cls_func({{* EXCLUDING(label)} as features})[score]",
        "label": "label",
        "functionName": "probabilizer_func"
    }
}
doChecks(conf)


# svd.train -> svd.embedRow
conf = {
    "type": "svd.train",
    "params": {
        "trainingData": "select * from toy",
        "modelFileUrl": "file://build/x86_64/tmp/bouya-svd.model",
        "functionName": "svd_func"
    }
}
doChecks(conf)



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
doChecks(conf)


mldb.script.set_return("success")

