// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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

var resp = mldb.get("/v1/datasets/git").json;

mldb.log(resp);

var resp = mldb.get("/v1/query", { q: "select * from git limit 10",
                                   format: "sparse" }).json;

mldb.log(resp);

var svdConfig = {
    type: "svd.train",
    params: {
        trainingDataset: "git",
        columnOutputDataset: { "id": "git_svd_embedding", type: "embedding" },
        select: "* excluding (message, parent)"
    }
};

createAndRunProcedure(svdConfig, "svd");

var resp = mldb.get("/v1/query", { q: "select rowName() from git_svd_embedding limit 10",
                                   format: "table" }).json;

mldb.log(resp);

var res3 = mldb.get("/v1/datasets/git_svd_embedding/routes/rowNeighbours", {row: "file|sql/sql_expression.h", numNeighbours: 50 }).json;

mldb.log(res3);


var tsneConfig = {
    type: "tsne.train",
    params: {
        trainingDataset: "git_svd_embedding",
        rowOutputDataset: { "id": "git_tsne_embedding", "type": "embedding" }
    }
};

//createAndRunProcedure(tsneConfig, 'tsne');


var resp = mldb.get("/v1/query", { q: "select count(*) as cnt, author, min(when({*})) as earliest, max(when({*})) as latest, sum(filesChanged) as changes, sum(insertions) as insertions, sum(deletions) as deletions from git group by author order by cnt desc limit 5", format: 'table', rowNames: false }).json;

mldb.log(resp);

"success"
