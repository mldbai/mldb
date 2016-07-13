function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

// Load the dataset
var resp = mldb.put("/v1/procedures/airline", {
    "type":"import.text",
    "params": {
        "dataFileUrl": "https://s3.amazonaws.com/benchm-ml--main/train-1m.csv",
        "offset" : 0,
        "ignoreBadLines" : true,
        "outputDataset": {
            "id": "airline"
        },
        "limit" : 10000,
        "runOnCreation": true        
    }
});

mldb.log(resp);

resp = mldb.put('/v1/procedures/benchmark', {
    "type": "randomforest.binary.train",
    "params": {
        "trainingData": "select {* EXCLUDING(dep_delayed_15min)} as features, dep_delayed_15min = 'Y' as label from airline",
        "runOnCreation": true,
        "modelFileUrl": "file://tmp/MLDB-1755.cls",
        "functionName": "classifyme",
        "featureVectorSamplings" : 1,
        "featureSamplings" : 1,
        "maxDepth" : 1,
        "verbosity" : 10,
        "featureSamplingProp": 1
    }
});

mldb.log(resp);

// Re-run but with all optimized paths turned off
// This causes us to not use the optimized column implementation
mldb.debugSetPathOptimizationLevel("never");

resp = mldb.put('/v1/procedures/benchmark2', {
    "type": "randomforest.binary.train",
    "params": {
        "trainingData": "select {* EXCLUDING(dep_delayed_15min)} as features, dep_delayed_15min = 'Y' as label from airline",
        "runOnCreation": true,
        "modelFileUrl": "file://tmp/MLDB-1433.cls",
        "functionName": "classifyme2",
        "featureVectorSamplings" : 1,
        "featureSamplings" : 1,
        "maxDepth" : 1,
        "verbosity" : 10,
        "featureSamplingProp": 1
    }
});

mldb.log(resp);

var cls1 = mldb.get("/v1/functions/classifyme/details");
var cls2 = mldb.get("/v1/functions/classifyme2/details");

mldb.log(cls1);
mldb.log(cls2);

assertEqual(cls1.json.model.params.classifiers[0].params.tree.root.pred,
            cls2.json.model.params.classifiers[0].params.tree.root.pred);

"success"


