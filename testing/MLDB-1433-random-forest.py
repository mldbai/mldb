import datetime

from mldb import mldb

start = datetime.datetime.now();
mldb.put("/v1/procedures/airline", {
    "type":"import.text",
    "params": {
        "dataFileUrl": "file://mldb_test_data/test.csv",
        "offset" : 0,
        "ignoreBadLines" : True,
        "outputDataset": {
            "id": "airline"
        },
        "limit" : 10000,
        "runOnCreation": True        
    }
})
mldb.log(datetime.datetime.now() - start)

start = datetime.datetime.now();
mldb.put("/v1/procedures/airline", {
    "type":"import.text",
    "params": {
        "dataFileUrl": "file://mldb_test_data/test.csv",
        "offset" : 10000,
        "ignoreBadLines" : True,
        "outputDataset": {
            "id": "airline_test"
        },
        # "limit" : 10,
        "runOnCreation": True        
    }
})
mldb.log(datetime.datetime.now() - start)

start = datetime.datetime.now();

mldb.put('/v1/procedures/benchmark', {
    "type": "randomforest.binary.train",
    "params": {
        "trainingData": """
            select
                {* EXCLUDING(dep_delayed_15min)} as features,
                dep_delayed_15min = 'Y' as label
            from airline
            """,
        "runOnCreation": True,
        "modelFileUrl": "file://tmp/MLDB-1433.cls",
        "functionName": "classifyme",
        "featureVectorSamplings" : 5,
        "featureSamplings" : 20,
        "maxDepth" : 20,
        "verbosity" : 0
    }
})
mldb.log(datetime.datetime.now() - start)
start = datetime.datetime.now();

accuracyConf = {
            "type": "classifier.test",
            "params": {
                "testingData": """
                    select classifyme({{* EXCLUDING(dep_delayed_15min)} as features})[score] as score, dep_delayed_15min = 'Y' as label from airline_test
                """,
                "runOnCreation": True
            }
        }

res = mldb.put("/v1/procedures/trainer3", accuracyConf);

mldb.log(res.json());

mldb.log(datetime.datetime.now() - start)

assert res.json()["status"]["firstRun"]["status"]["auc"] > 0.65

#### try with a non-tabular dataset

start = datetime.datetime.now();
mldb.put("/v1/procedures/airline", {
    "type": "transform",
    "params": {
        "inputData": "SELECT * FROM airline LIMIT 10000",
        "outputDataset": {
            "id": "airline_embedding",
            "type" : "sparse.mutable"
        },
        "runOnCreation": True        
    }
})
mldb.log(datetime.datetime.now() - start)

start = datetime.datetime.now();

mldb.put('/v1/procedures/benchmark', {
    "type": "randomforest.binary.train",
    "params": {
        "trainingData": """
            select
                {* EXCLUDING(dep_delayed_15min)} as features,
                dep_delayed_15min = 'Y' as label
            from airline_embedding
            """,
        "runOnCreation": True,
        "modelFileUrl": "file://tmp/MLDB-1433.cls",
        "functionName": "classifyme_embedding",
        "featureVectorSamplings" : 5,
        "featureSamplings" : 20,
        "maxDepth" : 20,
        "verbosity" : 0
    }
})
mldb.log(datetime.datetime.now() - start)
start = datetime.datetime.now();

accuracyConf = {
            "type": "classifier.test",
            "params": {
                "testingData": """
                    select classifyme_embedding({{* EXCLUDING(dep_delayed_15min)} as features})[score] as score, dep_delayed_15min = 'Y' as label from airline_test
                """,
                "runOnCreation": True
            }
        }

res = mldb.put("/v1/procedures/trainer4", accuracyConf);

mldb.log(res.json())

mldb.log(datetime.datetime.now() - start)

assert res.json()["status"]["firstRun"]["status"]["auc"] > 0.65

request.set_return('success')
