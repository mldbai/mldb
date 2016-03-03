

import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

start = datetime.datetime.now();
mldb.put("/v1/procedures/airline", {
    "type":"import.text",
    "params": {
        "dataFileUrl": "file://train-10m.csv",
        "offset" : 0,
        "ignoreBadLines" : True,
        "outputDataset": {
            "id": "airline"
        },
        "runOnCreation": True
    }
})
mldb.log(datetime.datetime.now() - start)

start = datetime.datetime.now();
#mldb.put('/v1/procedures/benchmark', {
#    "type": "classifier.experiment",
#    "params": {
#        "experimentName": "airline",
#        "trainingData": """
#            select
#                {* EXCLUDING(IsArrDelayed, IsDepDelayed)} as features,
#                IsDepDelayed = 'YES' as label
#            from airline
#            """,
#        "configuration": {
#            "type": "bagging",
#            "num_bags": 100,
#            "validation_split": 0,
#            "weak_learner": {
#                "type": "decision_tree",
#                "max_depth": 20,
#                "random_feature_propn": 0.3
#            }
#        },
#        "modelFileUrlPattern": "file://tmp/models/airline_$runid.cls",       
#        "mode": "boolean",
#        "runOnCreation": True
#    }
#})
mldb.put('/v1/procedures/benchmark', {
    "type": "prototype",
    "params": {
        "trainingData": """
            select
                {* EXCLUDING(dep_delayed_15min)} as features,
                dep_delayed_15min = 'Y' as label
            from airline
            """,
        #"configuration": {
        #    "type": "bagging",
        #    "num_bags": 100,
        #    "validation_split": 0,
        #    "weak_learner": {
        #        "type": "decision_tree",
        #        "max_depth": 20,
        #        "random_feature_propn": 0.3
        #    }
        #},
        #"modelFileUrlPattern": "file://tmp/models/airline_$runid.cls",       
        #"mode": "boolean",
        "runOnCreation": True
    }
})
mldb.log(datetime.datetime.now() - start)

mldb.script.set_return('success')
