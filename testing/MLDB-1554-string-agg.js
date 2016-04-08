// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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

var resp = mldb.get("/v1/datasets/test/query", {select: "string_agg(what, ', ') AS whats, string_agg(how, '') AS hows", groupBy: 'who', rowName: 'who', format: 'sparse', orderBy: 'who'});

plugin.log(resp.json);

assertEqual(resp.responseCode, 200, "Error executing query");

expected = [
   [
      [ "_rowName", "mustard" ],
      [ "hows", "kitchenplumplum" ],
      [ "whats", "moved, stabbed, killed" ]
   ],
   [
      [ "_rowName", "plum" ],
      [ "hows", "stabbedkitchen" ],
      [ "whats", "died, moved" ]
   ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

// test horizontal_agg

var resp = mldb.get("/v1/datasets/test/query", {select: "horizontal_string_agg({who, what, how}, ', ') AS aggs", format: 'table', orderBy: 'rowName()'});

plugin.log(resp.json);

assertEqual(resp.responseCode, 200, "Error executing query");

expected = [
   [ "_rowName", "aggs" ],
   [ "0", "mustard, moved, kitchen" ],
   [ "1", "plum, moved, kitchen" ],
   [ "2", "mustard, stabbed, plum" ],
   [ "3", "mustard, killed, plum" ],
   [ "4", "plum, died, stabbed" ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");


"success"
