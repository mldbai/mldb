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
        "functionName": "classifyme"
    }
})
mldb.log(datetime.datetime.now() - start)

res = mldb.query("select dep_delayed_15min = 'Y', classifyme({{* EXCLUDING(dep_delayed_15min)} as features}) from airline limit 20")

mldb.log(res)

mldb.script.set_return('success')