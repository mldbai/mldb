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

var resp = mldb.get("/v1/datasets/test/query", {format: 'table'});

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

var resp = mldb.get("/v1/datasets/test/query", {select:"min({*}) AS min, max({*}) AS max", groupBy: 'label', rowName: 'label', format: 'table'});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

expected = [
   [
      "_rowName",
      "min.label",
      "min.x",
      "min.y",
      "max.label",
      "max.x",
      "max.y"
   ],
   [ "cat", "cat", 0, 0, "cat", 1, 2 ],
   [ "dog", "dog", 1, 1, "dog", 1, 1 ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

resp = mldb.get("/v1/datasets/test/query", {select:"min({*}) AS min, max({*}) AS max", groupBy: 'label', rowName: 'group_key_element(0)', format: 'table'});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

"success"

