
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, random, datetime, os


conf = {
    "type": "classifier.train",
    "params": {
        "trainingData": """
            select {* EXCLUDING(quality)} as features, quality as label
        """,
        "modelFileUrl": "file://my_model.cls",
        "algorithm": "glz",
        "equalizationFactor": 0.5,
        "mode": "regression",
        "functionName": "myScorer",
        "runOnCreation": True
    }
}
rez = mldb.perform("PUT", "/v1/procedures/trainer", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400



conf = {
    "type": "probabilizer.train",
    "params": {
        "trainingData": """
            select {* EXCLUDING(quality)} as features, quality as label
        """,
        "modelFileUrl": "file://my_model.cls",
        "runOnCreation": True
    }
}
rez = mldb.perform("PUT", "/v1/procedures/trainer2", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400


conf = {
    "type": "classifier.test",
    "params": {
        "testingData": """
            select {* EXCLUDING(quality)} as score, quality as label
        """,
        "runOnCreation": True
    }
}
rez = mldb.perform("PUT", "/v1/procedures/trainer3", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400



conf = {
    "type": "tsne.train",
    "params": {
        "trainingData": """
            select {* EXCLUDING(quality)} as features, quality as label
        """,
        "runOnCreation": True
    }
}
rez = mldb.perform("PUT", "/v1/procedures/trainer3", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400


conf = {
    "type": "kmeans.train",
    "params": {
        "trainingData": """
            select {* EXCLUDING(quality)} as features, quality as label
        """,
        "runOnCreation": True
    }
}
rez = mldb.perform("PUT", "/v1/procedures/trainer3", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400


conf = {
    "type": "svm.train",
    "params": {
        "trainingData": """
            select {* EXCLUDING(quality)} as features, quality as label
        """,
        "runOnCreation": True
    }
}
rez = mldb.perform("PUT", "/v1/procedures/trainer3", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400

mldb.script.set_return("success")

