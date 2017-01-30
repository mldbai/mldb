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

    mldb.log("expr", expr);
    mldb.log("val", val);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}


var resp = mldb.get("/v1/query", {q:"SELECT y, label, x FROM test ORDER BY rowPath()", format:'table'});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log("returned", resp.json);

var expected = [
   [ "_rowName", "label", "x", "y" ],
   [ "ex1", "cat", 0, 0 ],
   [ "ex2", "dog", 1, 1 ],
   [ "ex3", "cat", 1, 2 ]
];

plugin.log("expected", expected);

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 1 output was not the same as expected output");

// Test a group by

var resp = mldb.get("/v1/query", {q:"SELECT min(x), min(y), label FROM test GROUP BY label"});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

expected = [
   {
      "columns" : [
         [ "label", "cat", "2015-01-01T00:00:00Z" ],
         [ "min(x)", 0, "2015-01-01T00:00:00Z" ],
         [ "min(y)", 0, "2015-01-01T00:00:00Z" ]
      ],
      "rowName" : "\"[\"\"cat\"\"]\""
   },
   {
      "columns" : [
         [ "label", "dog", "2015-01-01T00:00:00Z" ],
         [ "min(x)", 1, "2015-01-01T00:00:00Z" ],
         [ "min(y)", 1, "2015-01-01T00:00:00Z" ]
      ],
      "rowName" : "\"[\"\"dog\"\"]\""
   }
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

"success"
