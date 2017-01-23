// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
        tuples.push(["" + j, i == j ? 1 : 0, now]);
    }
    dataset.recordRow("row" + i, tuples);
    dataset.recordRow("row" + i + "_a", tuples);
}

dataset.commit();

// Check we can find the points properly in the VP tree

var sql_func_res = mldb.put("/v1/functions/nn", {
    type: 'embedding.neighbors',
    params: {
        'dataset': {id: 'test1', type: "embedding"}
    }
});

mldb.log(sql_func_res);

coords = [];
for(var i=0; i<numDims; i++) coords.push(0);
coords[0] = 1;

var res = mldb.query("select nn({coords: "+JSON.stringify(coords)+", numNeighbors:5})[distances] as *");

plugin.log(res);

gres = res[0]["columns"];

// First two should have distance 0, the rest should have distance sqrt(2) as a 32 bit float
assertEqual(gres.length, 5, "length");
assertEqual(gres[1][0], "row0_a", "name0");
assertEqual(gres[0][0], "row0", "name1");
assertEqual(gres[1][1], 0, "dist0");
assertEqual(gres[0][1], 0, "dist1");
assertEqual(gres[2][1], 1.4142135381698608, "dist2");
assertEqual(gres[3][1], 1.4142135381698608, "dist3");
assertEqual(gres[4][1], 1.4142135381698608, "dist4");


"success"
