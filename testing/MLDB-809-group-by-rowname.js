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

var resp = mldb.get("/v1/query", {q : 'SELECT * from test', format: 'table'});

plugin.log(resp.json);

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var resp = mldb.get("/v1/query", {q:"SELECT min({*}) AS min, max({*}) AS max NAMED label from test group by label", format: 'table'});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

expected = [
   [
      "_rowName",
      "max.label",
      "max.x",
      "max.y",
      "min.label",
      "min.x",
      "min.y"
   ],
   [ "cat", "cat", 1, 2, "cat", 0, 0 ],
   [ "dog", "dog", 1, 1, "dog", 1, 1 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

resp = mldb.get("/v1/query", {q:"SELECT min({*}) AS min, max({*}) AS max NAMED group_key_element(0) from test group by label", format: 'table'});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

"success"

