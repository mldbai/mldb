

import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

start = datetime.datetime.now();
mldb.put("/v1/datasets/airline", {
    "type":"text.csv.tabular",
    "params": {
        "dataFileUrl": "file://allyears.1987.2013.csv",
        "limit" : 1000000,
        "offset" : 0,
       "ignoreBadLines" : True
    }
})
mldb.log(datetime.datetime.now() - start)

res = mldb.query("select IsDepDelayed, min(DepDelay), max(DepDelay) from airline group by IsDepDelayed")

mldb.log(res)

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

start = datetime.datetime.now();
mldb.put('/v1/procedures/benchmark1', {
    "type": "classifier.train",
    "params": {
        "trainingData": """
            select
                {* EXCLUDING(IsArrDelayed, IsDepDelayed, DepDelay, ArrDelay)} as features,
                IsDepDelayed = 'YES' as label
            from airline
            """,
        "configuration": {
            "type": "decision_tree",
            "max_depth": 4,
            "random_feature_propn": 1,
            "verbosity": 3
        },
        "equalizationFactor": 0,
        # "configuration": {
        #     "type": "bagging",
        #     "num_bags": 1,
        #     "validation_split": 0,
        #     "weak_learner": {
        #         "type": "decision_tree",
        #         "max_depth": 4,
        #         "random_feature_propn": 0.3
        #     },
        #     "verbosity": 3
        # },
        "modelFileUrl": "file://tmp/airlines.cls",       
        "mode": "boolean",
        "runOnCreation": True
    }
})
mldb.log(datetime.datetime.now() - start)

start = datetime.datetime.now();
mldb.put('/v1/procedures/benchmark', {
    "type": "prototype",
    "params": {
        "trainingData": """
            select
                {* EXCLUDING(IsArrDelayed, IsDepDelayed, DepDelay, ArrDelay)} as features,
                IsDepDelayed = 'YES' as label
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
