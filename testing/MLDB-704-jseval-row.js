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

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");

function recordExample(row, x, y, z)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
    if (z)
        dataset.recordRow(row, [ [ "z", z, ts ] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 3);
recordExample("ex2", 1, 2, "yes");
recordExample("ex3", 2, 1);
recordExample("ex4", 3, 0, "no");

dataset.commit()

var res1 = mldb.get("/v1/datasets/test/query",
                    {
                        select: "jseval('mldb.log(''Hello '' + x.toJs());  return { x: x, y: ''yes''}', '!x', x) AS *",
                        format: 'table',
                        orderBy: 'rowName()'
                    });

plugin.log(res1);

var expected = [
      [ "_rowName", "x", "y" ],
      [ "ex1", 0, "yes" ],
      [ "ex2", 1, "yes" ],
      [ "ex3", 2, "yes" ],
      [ "ex4", 3, "yes" ]
];

assertEqual(res1.json, expected, "row output from JS function");

var res1 = mldb.get("/v1/datasets/test/query",
                    {
                        select: "jseval('mldb.log(''Hello '' + x);  return { x: x, y: ''yes''}', 'x', x) AS *",
                        format: 'table',
                        orderBy: 'rowName()'
                    });

plugin.log(res1);

assertEqual(res1.json, expected, "row output from JS function");

// MLDB-757

var res2 = mldb.get("/v1/datasets/test/query",
                    {
                        select: "jseval('return Object.keys(x.columns()).length', '!x', {*}) AS nvals",
                        format: 'table',
                        orderBy: 'rowName()'
                    });

plugin.log(res2);

var expected2 = [
   [ "_rowName", "nvals" ],
   [ "ex1", 2 ],
   [ "ex2", 3 ],
   [ "ex3", 2 ],
   [ "ex4", 3 ]
];

assertEqual(res2.json, expected2, "row input to JS function");

var res2 = mldb.get("/v1/datasets/test/query",
                    {
                        select: "jseval('return Object.keys(x).length', 'x', {*}) AS nvals",
                        format: 'table',
                        orderBy: 'rowName()'
                    });

plugin.log(res2);

var expected2 = [
   [ "_rowName", "nvals" ],
   [ "ex1", 2 ],
   [ "ex2", 3 ],
   [ "ex3", 2 ],
   [ "ex4", 3 ]
];

assertEqual(res2.json, expected2, "row input to JS function");

// MLDB-758

var res3 = mldb.get("/v1/datasets/test/query",
                    {
                        select: "jseval('', '') AS nulls",
                        format: 'table',
                        orderBy: 'rowName()'
                    });

plugin.log(res3);

var expected3 = [
   [ "_rowName", "nulls" ],
   [ "ex1", null ],
   [ "ex2", null ],
   [ "ex3", null ],
   [ "ex4", null ]
];

assertEqual(res3.json, expected3, "undefined output of JS function");


var res4 = mldb.query("SELECT jseval('return row;', '!row', {*}) AS * NAMED 'res' FROM (SELECT x:1, y:2)");

expected4 = [
   {
      "columns" : [
         [ "x", 1, "-Inf" ],
         [ "y", 2, "-Inf" ]
      ],
      "rowHash" : "4ba25cf9b5244b88",
      "rowName" : "res"
   }
];

assertEqual(res4, expected4);

"success"
