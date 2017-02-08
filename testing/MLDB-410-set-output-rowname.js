// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

var resp = mldb.get("/v1/query", { q: 'SELECT x,y,label from test', format: 'table' });
assertEqual(resp.responseCode, 200);

plugin.log(resp.json);

var expected = [
   [ "_rowName", "label", "x", "y" ],
   [ "ex3", "cat", 1, 2 ],
   [ "ex2", "dog", 1, 1 ],
   [ "ex1", "cat", 0, 0 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");


// Now with a transformed row name
var resp = mldb.get("/v1/query",
                    {q:"select x,y,label NAMED rowName() + '_transformed' from test", 
                     format: 'table'});
plugin.log(resp);

assertEqual(resp.responseCode, 200);

expected = [
    [ "_rowName", "label", "x", "y" ],
    [ "ex3_transformed", "cat", 1, 2 ],
    [ "ex2_transformed", "dog", 1, 1 ],
    [ "ex1_transformed", "cat", 0, 0 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Output was not the same as expected output");

"success"
