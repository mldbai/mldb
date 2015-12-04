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

var resp = mldb.get("/v1/query", {q:"SELECT min({*}) AS min, max({*}) AS max FROM test GROUP BY label"});

assertEqual(resp.responseCode, 200, "Error executing query");

plugin.log(resp.json);

expected =  [
   {
      "columns" : [
         [ "min.label", "cat", "2015-01-01T00:00:00Z" ],
         [ "min.x", 0, "2015-01-01T00:00:00Z" ],
         [ "min.y", 0, "2015-01-01T00:00:00Z" ],
         [ "max.label", "cat", "2015-01-01T00:00:00Z" ],
         [ "max.x", 1, "2015-01-01T00:00:00Z" ],
         [ "max.y", 2, "2015-01-01T00:00:00Z" ]
      ],
      "rowHash" : "554f96c80ea05ddb",
      "rowName" : "[\"cat\"]"
   },
   {
      "columns" : [
         [ "min.label", "dog", "2015-01-01T00:00:00Z" ],
         [ "min.x", 1, "2015-01-01T00:00:00Z" ],
         [ "min.y", 1, "2015-01-01T00:00:00Z" ],
         [ "max.label", "dog", "2015-01-01T00:00:00Z" ],
         [ "max.x", 1, "2015-01-01T00:00:00Z" ],
         [ "max.y", 1, "2015-01-01T00:00:00Z" ]
      ],
      "rowHash" : "d55e0e284796f79e",
      "rowName" : "[\"dog\"]"
   }
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

//MLDB-988

resp = mldb.get("/v1/query", {q:"SELECT sum(x) AS sum FROM test GROUP BY x"});
plugin.log(resp.json);

"success"
