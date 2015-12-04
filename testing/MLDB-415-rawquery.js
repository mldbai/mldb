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

    mldb.log("expr", expr);
    mldb.log("val", val);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}


var resp = mldb.get("/v1/query", {q:"SELECT y, label, x FROM test", format:'table'});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log("returned", resp.json);

var expected = [
   [ "_rowName", "y", "label", "x" ],
   [ "ex1", 0, "cat", 0 ],
   [ "ex3", 2, "cat", 1 ],
   [ "ex2", 1, "dog", 1 ]
];

plugin.log("expected", expected);

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 1 output was not the same as expected output");

// Test a group by

var resp = mldb.get("/v1/query", {q:"SELECT min(x), min(y), label FROM test GROUP BY label"});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

expected =  [
   {
      "columns" : [
         [ "min(x)", 0, "2015-01-01T00:00:00Z" ],
         [ "min(y)", 0, "2015-01-01T00:00:00Z" ],
         [ "label", "cat", "2015-01-01T00:00:00Z" ]
      ],
      "rowHash" : "554f96c80ea05ddb",
      "rowName" : "[\"cat\"]"
   },
   {
      "columns" : [
         [ "min(x)", 1, "2015-01-01T00:00:00Z" ],
         [ "min(y)", 1, "2015-01-01T00:00:00Z" ],
         [ "label", "dog", "2015-01-01T00:00:00Z" ]
      ],
      "rowHash" : "d55e0e284796f79e",
      "rowName" : "[\"dog\"]"
   }
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

"success"
