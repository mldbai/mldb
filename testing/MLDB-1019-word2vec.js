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

function assertNearlyEqual(expr, val, msg, percent, abs)
{
    percent = percent || 1.0;
    abs = abs || 1e-10;
    
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    var diff = Math.abs(val - expr);

    if (diff < abs)
        return;

    var mag = Math.max(Math.abs(val), Math.abs(expr));

    var relativeErrorPercent = diff / mag * 100.0;

    if (relativeErrorPercent <= percent)
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);
    plugin.log("diff", diff);
    plugin.log("abs", abs);
    plugin.log("relativeErrorPercent", relativeErrorPercent);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not approximately equal to " + JSON.stringify(val);
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


var config = {
    type: 'import.word2vec',
    params: {
        dataFileUrl: 'file:///home/jeremy/GoogleNews-vectors-negative300.bin',
        output: {
            type: 'embedding',
            id: 'w2v'
        },
        limit: 100000
    }
};

createAndTrainProcedure(config, "w2v");

var res3 = mldb.get("/v1/datasets/w2v/routes/rowNeighbours", {row: "France"}).json;

mldb.log(res3);

var expected = [
   [ "France", "831e552f87fd6717", 0 ],
   [ "Belgium", "c62d860abed63cdd", 2.110022783279419 ],
   [ "French", "4a917df790d78d44", 2.111140489578247 ],
   [ "Germany", "23b23b4204547855", 2.321765184402466 ],
   [ "Paris", "30acad9c6b45cf9c", 2.366143226623535 ],
   [ "Spain", "e044f19832a6ddc9", 2.4046993255615234 ],
   [ "Italy", "01c2c9320702ac05", 2.4250826835632324 ],
   [ "Europe", "3d4c11e2fb4a8ed6", 2.558151960372925 ],
   [ "Morocco", "3f5fa5676bb7fb61", 2.567964553833008 ],
   [ "Switzerland", "e782a5ae091644c9", 2.5763208866119385 ]
];

assertEqual(res3, expected);

// MLDB-1020 check that we can record both 'null' and '0' which hash
// to the same value.
var res4 = mldb.get("/v1/query", { q: "select * from w2v where rowName() = '0' or rowName() = 'null'", format: 'table'}).json;

mldb.log(res4);

"success"
