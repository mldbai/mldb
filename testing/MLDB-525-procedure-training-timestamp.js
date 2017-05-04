// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Full Reddit example, as a serial procedure. */

function assertLessEqual(expr, val, msg)
{
    if (expr <= val)
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not less than " + JSON.stringify(val);
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

var numLines = 10000;

// Get a dataset with Reddit in it

var datasetId = "reddit_dataset_" + numLines;


// Now run the rest of the bits in a procedure

var loadDatasetConfig = {
    type: "createEntity",
    params: {
        kind: "plugin",
        type: "javascript",
        id: 'reddit',
        params: {
            address: "file://mldb/testing/reddit_dataset_plugin.js",
            args: { numLines: numLines, datasetId: datasetId }
        }
    }
};

var config = loadDatasetConfig;

var name = 'reddit';

var createOutput = mldb.put("/v1/procedures/" + name, config);
assertSucceeded("procedure " + name + " creation", createOutput);

// Run the training
var start = new Date(new Date() - 1.0);
var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
var end = new Date(new Date() + 1.0);
assertSucceeded("procedure " + name + " training", trainingOutput);

plugin.log(trainingOutput);

var trainingStarted = new Date(trainingOutput.json.runStarted);
var trainingFinished = new Date(trainingOutput.json.runFinished);

//assertLessEqual(start, trainingStarted, "start before training started");
assertLessEqual(trainingStarted, trainingFinished, "training start before training finish");
//assertLessEqual(trainingFinished, end, "training finish before finish");

plugin.log("procedure " + name + " took " + (end - start) / 1000 + " seconds");


"success"
