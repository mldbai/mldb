// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
    dataset.recordRow(i + 1, tuples);
}
    
dataset.commit();

var svdConfig = {
    'type': 'svd.train',
    'params':
    {
        "trainingData": "select * from random_dataset",
        "columnOutputDataset": {
            "type": "embedding",
            "id": "svd_random_col"
        },
        "rowOutputDataset": {
            "id": "svd_random_row",
            'type': "embedding"
        },
        "numSingularValues": 1000,
        "numDenseBasisVectors": 20
    }
};

createAndTrainProcedure(svdConfig, "svd");

var cols = mldb.get("/v1/query", {q : 'SELECT * from svd_random_col'}).json;

if (cols.length != 26) {
    throw "Expected 26 columns, got " + cols.length;
}

"success"
