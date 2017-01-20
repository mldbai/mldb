// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var dataset = mldb.createDataset({type:'sparse.mutable',id:'test'});

var ts = new Date("2015-01-01");

var row = 0;

function recordExample(who, what, how)
{
    dataset.recordRow(row++, [ [ "who", who, ts ], ["what", what, ts], ["how", how, ts] ]);
}

recordExample("mustard", "moved", "kitchen");
recordExample("plum", "moved", "kitchen");
recordExample("mustard", "stabbed", "plum");
recordExample("mustard", "killed", "plum");
recordExample("plum", "died", "stabbed");

dataset.commit()

var resp = mldb.put("/v1/functions/identity", {
    "type": "sql.expression",
    "params": {
        "expression": "input"
    }
});

mldb.log(resp);

// If we can't find the identity() function, we still have the bug
var resp = mldb.get("/v1/query", { q: 'SELECT * FROM test WHERE rowName() IN (KEYS OF identity({input: {"1": 1}})[input])' });

plugin.log(resp.json);

assertEqual(resp.responseCode, 200, "Error executing query");

expected = [
   {
      "columns" : [
         [ "how", "kitchen", "2015-01-01T00:00:00Z" ],
         [ "what", "moved", "2015-01-01T00:00:00Z" ],
         [ "who", "plum", "2015-01-01T00:00:00Z" ]
      ],
      "rowName" : "1"
   }
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

"success"
