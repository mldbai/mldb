// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* MLDB-283 Test of nearest neighbours. */

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
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

function recordExample(row, x, y)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0);
recordExample("ex2", 0, 1);
recordExample("ex3", 1, 0);
recordExample("ex4", 1, 1);

dataset.commit()

var res1 = mldb.get("/v1/datasets/test/routes/neighbours", {x:0.5,y:0.5}).json;

var expected1 = [
   [ "ex1", "397de880d5f0376e", 0.7071067690849304 ],
   [ "ex2", "ed64a202cef7ccf1", 0.7071067690849304 ],
   [ "ex3", "418b8ce19e0de7a3", 0.7071067690849304 ],
   [ "ex4", "213ca5902e95224e", 0.7071067690849304 ]
];

assertEqual(mldb.diff(expected1, res1, false /* strict */), {},
            "Output was not the same as expected output");

var res2 = mldb.get("/v1/datasets/test/routes/neighbours", {x:0.1,y:0.2}).json;

if (res2[0][0] != "ex1"
    || res2[1][0] != "ex2"
    || res2[2][0] != "ex3"
    || res2[3][0] != "ex4")
    throw "Row names are wrong";

if (res2[0][2] >= res2[1][2]
    || res2[1][2] >= res2[2][2]
    || res2[2][2] >= res2[3][2])
    throw "Row order is wrong";




var expected2 = [
   [ "ex1", "397de880d5f0376e", 0.22360680997371674 ],
   [ "ex2", "ed64a202cef7ccf1", 0.8062257766723633 ],
   [ "ex3", "418b8ce19e0de7a3", 0.9219543933868408 ],
   [ "ex4", "213ca5902e95224e", 1.2041594982147217 ]
];

if (JSON.stringify(expected2) != JSON.stringify(res2)) {
    plugin.log(JSON.stringify(expected2));
    plugin.log(JSON.stringify(res2));
    assertEqual(mldb.diff(expected2, res2, false /* strict */), {},
                "Output was not the same as expected output");
}

plugin.log("result of query 2", res2);

// MLDB-509
var res3 = mldb.get("/v1/datasets/test/routes/rowNeighbours", {row: "ex1"}).json;

var expected3 = [
   [ "ex1", "397de880d5f0376e", 0 ],
   [ "ex2", "ed64a202cef7ccf1", 1 ],
   [ "ex3", "418b8ce19e0de7a3", 1 ],
   [ "ex4", "213ca5902e95224e", 1.4142135381698608 ]
]

plugin.log("result of query 3", res3);

if (JSON.stringify(expected3) != JSON.stringify(res3)) {
    plugin.log(JSON.stringify(expected3));
    plugin.log(JSON.stringify(res3));
    assertEqual(mldb.diff(expected3, res3, false /* strict */), {},
                "Output was not the same as expected output");
}

"success"
