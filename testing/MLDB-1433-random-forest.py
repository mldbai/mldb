import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

start = datetime.datetime.now();
mldb.put("/v1/procedures/airline", {
    "type":"import.text",
    "params": {
        "dataFileUrl": "file://train-1m.csv",
        "offset" : 0,
        "ignoreBadLines" : True,
        "outputDataset": {
            "id": "airline"
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

accuracyConf = {
            "type": "classifier.test",
            "params": {
                "testingData": """
                    select classifyme({{* EXCLUDING(dep_delayed_15min)} as features})[score] as score, dep_delayed_15min = 'Y' as label from airline
                """,
                "runOnCreation": True
            }
        }

res = mldb.put("/v1/procedures/trainer3", accuracyConf);

mldb.log(res)

mldb.script.set_return('success')