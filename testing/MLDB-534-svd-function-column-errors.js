// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
    plugin.log(process, response);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

function createAndTrainProcedure(config, name)
{
    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);
}

// Create a dataset with random numbers in it
var datasetConfig = {
    "type": "sparse.mutable",
    "id": "random_dataset"
}

var dataset = mldb.createDataset(datasetConfig)

var ts = new Date();

for (var i = 0;  i < 100;  ++i) {
    var tuples = [];
    for (var j = 0;  j < 26;  ++j) {
        tuples.push([j, Math.random() * 10 | 0, ts]);
    }
    for (var j = 0;  j < 26;  ++j) {
        tuples.push([j + 100, String.fromCharCode(Math.random() * 26 + 65), ts]);
    }
    dataset.recordRow(i + 1, tuples);
}
    
dataset.commit();

var svdConfig = {
    'type': 'svd.train',
    'params':
    {
        "trainingData": {"from" : {"id": "random_dataset"}},
        "columnOutputDataset": {
            "type": "embedding",
            "id": "svd_random_col"
        },
        "rowOutputDataset": {
            "id": "svd_random_row",
            'type': "embedding"
        },
        "numSingularValues": 1000,
        "numDenseBasisVectors": 20,
        "modelFileUrl": "file://tmp/MLDB-534-svd.json.gz"
    }
};

createAndTrainProcedure(svdConfig, "svd");

var cols = mldb.get("/v1/datasets/svd_random_col/query").json;

mldb.log(cols[0]);

//if (cols.length != 52) {
//    throw "Expected 52 columns, got " + cols.length;
//}

// Creating the function
var svdFunctionConfig = {
    "type": "svd.embedRow",
    "params": { "modelFileUrl": "file://tmp/MLDB-534-svd.json.gz", "maxSingularValues": 20 }
};

var r = mldb.put("/v1/functions/svd", svdFunctionConfig);

plugin.log(r);

assertSucceeded("creating SVD function", r);

// MLDB-536
var r = mldb.get("/v1/functions/svd/application", { input: { row: {'0': 1}}});
plugin.log(r);
assertEqual(r.responseCode, 200);
assertEqual(r.json.output.embedding.shape, [20]);

r = mldb.get("/v1/functions/svd/application", { input: { row: {'0': '1'}}});
plugin.log(r);
assertEqual(r.responseCode, 400);
assertContains(r.json.error, "only numbers were seen");

r = mldb.get("/v1/functions/svd/application", { input: { row: {'100': 1}}});
plugin.log(r);
assertEqual(r.responseCode, 400);
assertContains(r.json.error, "was a string in training");


"success"
