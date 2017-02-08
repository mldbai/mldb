// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

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

// Testcase will fail here until issue is fixed
var resp = mldb.get("/v1/query", {q : 'SELECT * FROM test', format: 'table'});

plugin.log(resp.json);

var expected = [
   [ "_rowName", "label", "x", "y" ],
   [ "ex3", "cat", 1, 2 ],
   [ "ex2", "dog", 1, 1 ],
   [ "ex1", "cat", 0, 0 ]
];

assertEqual(resp.json, expected);

"success"
