var mldb = require('mldb')
var unittest = require('mldb/unittest');

var import_data_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "file://train-50m.csv.zst",
        outputDataset: {
            id: "airline",
        },
        runOnCreation: true,
        encoding: 'ascii',
        ignoreBadLines: true,
        limit: 10000000,//10000000,
    }
};

var start = new Date();
var res = mldb.put("/v1/procedures/import_data", import_data_conf);
var end = new Date();

mldb.log(res.json);
unittest.assertEqual(res.responseCode, 201);

mldb.log("importing training data took", (end - start), "milliseconds");

var import_test_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "file://test.csv.zst",
        outputDataset: {
            id: "airline_test",
        },
        runOnCreation: true,
        encoding: 'ascii',
        ignoreBadLines: true,
    }
};

start = new Date();
var res = mldb.put("/v1/procedures/import_test", import_test_conf)
end = new Date();
mldb.log(res.json);
unittest.assertEqual(res.responseCode, 201);

mldb.log("importing testing data took", (end - start), "milliseconds");

var benchmark_conf = {
    type: "randomforest.binary.train",
    params: {
        trainingData: "select {* EXCLUDING(dep_delayed_15min)} as features, dep_delayed_15min = 'Y' as label from airline",
        runOnCreation: true,
        modelFileUrl: "file://tmp/benchmark-airlines.cls",
        functionName: "classifyme",
        featureVectorSamplings : 1, //5,
        featureSamplings : 1, //20,
        maxDepth :19, // 0 based; maxDepth = 0 will train a depth 1 tree
        verbosity : 0,
    }
};

start = new Date();
res = mldb.put("/v1/procedures/benchmark", benchmark_conf);
end = new Date();
mldb.log(res.json);
unittest.assertEqual(res.responseCode, 201);

mldb.log("training classifier took", (end - start), "milliseconds");

var test_conf = {
    type: "classifier.test",
    params: {
        testingData: "select classifyme({{* EXCLUDING(dep_delayed_15min)} as features})[score] as score, dep_delayed_15min = 'Y' as label from airline_test",
        "runOnCreation": true,
    }
};

start = new Date();
res = mldb.put("/v1/procedures/test", test_conf);
end = new Date();
mldb.log(res.json);
unittest.assertEqual(res.responseCode, 201);

mldb.log("testing classifier took", (end - start), "milliseconds");

mldb.log("auc =", res.json.status.firstRun.status.auc)

"success"
