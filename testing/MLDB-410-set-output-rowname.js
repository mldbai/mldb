// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

var dataset = mldb.createDataset({type:'sparse.mutable',id:'test'});

var ts = new Date("2015-01-01");

function recordExample(row, x, y, label)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts] ]);
}

recordExample("ex1", 0, 0, "cat");
recordExample("ex2", 1, 1, "dog");
recordExample("ex3", 1, 2, "cat");

dataset.commit()

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var resp = mldb.get("/v1/datasets/test/query", { select: 'x,y,label', format: 'table' });
assertEqual(resp.responseCode, 200);

plugin.log(resp.json);

var expected = [
   [ "_rowName", "x", "y", "label" ],   
   [ "ex3", 1, 2, "cat" ],
   [ "ex2", 1, 1, "dog" ],
   [ "ex1", 0, 0, "cat" ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");


// Now with a transformed row name
var resp = mldb.get("/v1/datasets/test/query",
                    {select:'x,y,label', rowName:"rowName() + '_transformed'",
                     format: 'table'});
assertEqual(resp.responseCode, 200);

plugin.log(resp);

expected = [
    [ "_rowName", "x", "y", "label" ],
    [ "ex3_transformed", 1, 2, "cat" ],
    [ "ex2_transformed", 1, 1, "dog" ],
    [ "ex1_transformed", 0, 0, "cat" ],
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");

"success"
