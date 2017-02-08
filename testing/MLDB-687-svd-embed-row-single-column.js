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
    var start = new Date();

    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);

    var end = new Date();

    plugin.log("procedure " + name + " took " + (end - start) / 1000 + " seconds");
}

var dataset = mldb.createDataset({ id: "ds", type: 'sparse.mutable'});

var ts = new Date(2015,1,1);

dataset.recordRow("row1", [ [ "x", 1, ts ], [ "y", 1, ts ] ]);
dataset.recordRow("row2", [ [ "x", 1, ts ], [ "y", 2, ts ] ]);

dataset.commit();

var svdConfig = {
    type: "svd.train",
    params: {
        trainingData: "select * from ds",
        modelFileUrl: "file://tmp/MLDB-687.svd.json.gz",
        numSingularValues: 2
    }
};

createAndTrainProcedure(svdConfig, 'svd');

var svdFunctionConfig = {
    type: "svd.embedRow",
    params: {
        modelFileUrl: "file://tmp/MLDB-687.svd.json.gz"
    }
};

var createFunctionOutput = mldb.put("/v1/functions/svd", svdFunctionConfig);
assertSucceeded("creating SVD", createFunctionOutput);

var res = mldb.query("select svd({row: { x:2 }})");

mldb.log(res);

"success"
