// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

// MLDB-481
// Vantage point tree on corners of a high dimensional cube is a pathological
// case.

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

var dataset_config = {
    'type'    : 'embedding',
    'id'      : 'test1'
};

var dataset = mldb.createDataset(dataset_config)

var numDims = 200;

var now = new Date();

for (var i = 0;  i < numDims;  ++i) {

    var tuples = [];
    for (var j = 0;  j < numDims;  ++j) {
        tuples.push(["dim" + j, i == j ? 1 : 0, now]);
    }
    dataset.recordRow("row" + i, tuples);
    dataset.recordRow("row" + i + "_a", tuples);
}

dataset.commit();

// Check we can find the points properly in the VP tree

var res = mldb.get("/v1/datasets/test1/routes/neighbours", {dim0:1, numNeighbours:5}).json;

plugin.log(res);

// First two should have distance 0, the rest should have distance sqrt(2) as a 32 bit float
assertEqual(res.length, 5, "length");
assertEqual(res[0][0], "row0", "name0");
assertEqual(res[1][0], "row0_a", "name1");
assertEqual(res[0][2], 0, "dist0");
assertEqual(res[1][2], 0, "dist1");
assertEqual(res[2][2], 1.4142135381698608, "dist2");
assertEqual(res[3][2], 1.4142135381698608, "dist3");
assertEqual(res[4][2], 1.4142135381698608, "dist4");


"success"
