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

function createAndRunProcedure(config, name)
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


var importConfig = {
    type: 'import.git',
    params: {
        repository: 'file://./mldb_data2/docker',
        importStats: true,
        importTree: true,
        outputDataset: {
            id: 'git'
        }
    }
};

createAndRunProcedure(importConfig, "import");

var deriveConfig = {
    "type": "transform",
    "params": { 
        "inputData": {
            "select": "regex_replace(authorEmail, '.*@', '') as company, *",
            "from": "git",
            "where": "parentCount=1"
        },
        "outputDataset": { "id": "gitderived", "type": "sparse.mutable"},
        "runOnCreation": true
    }
};

createAndRunProcedure(deriveConfig, "derive");

var countConfig = {
    "type": "transform",
    "params": {
        "inputData": {
            "select": "count(*) as count",
            "from": "gitderived",
            "groupBy": "company",
            "named": "'company|' + company"
        },
        "outputDataset": { "id": "companycounts", "type": "sparse.mutable"},
        "runOnCreation": true
    }
};

createAndRunProcedure(countConfig, "count");

"success"

