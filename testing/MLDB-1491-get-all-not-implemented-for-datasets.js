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


var config = {
    type: 'import.git',
    params: {
        repository: 'file://.',
        importStats: true,
        importTree: true,
        outputDataset: {
            id: 'git'
        }
    }
};

createAndRunProcedure(config, "git");

var resp = mldb.get("/v1/query", { q: 
    "select count(*) as cnt, author, sum(filesChanged) as changes, sum(insertions) as insertions, sum(deletions) as deletions from git group by author", format: 'table', rowNames: false });

mldb.log(resp.json);
assertEqual(resp.responseCode, 200);

var resp = mldb.get("/v1/query", { q: 
    "select count(*) as cnt, author, min(earliest_timestamp({*})) as earliest, max(latest_timestamp({*})) as latest, sum(filesChanged) as changes, sum(insertions) as insertions, sum(deletions) as deletions from git group by author", format: 'table', rowNames: false });

mldb.log(resp.json);
assertEqual(resp.responseCode, 200);

var resp = mldb.get("/v1/query", { q: 
    "select count(*) as cnt, author, temporal_earliest({*}), sum(filesChanged) as changes, sum(insertions) as insertions, sum(deletions) as deletions from git group by author", format: 'table', rowNames: false });
mldb.log(resp.json);
assertEqual(resp.responseCode, 400);
assertEqual(resp.json.error, "Non-aggregator 'temporal_earliest({*})' with GROUP BY clause is not allowed");

"success"
