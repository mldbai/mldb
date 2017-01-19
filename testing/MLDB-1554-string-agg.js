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

var resp = mldb.get("/v1/query", 
    {q: "SELECT string_agg(what, ', ', rowName()) AS whats, string_agg(how, '', rowName()) AS hows NAMED who FROM test GROUP BY who ORDER BY who",
    format: 'sparse'});

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
      [ "hows", "kitchenstabbed" ],
      [ "whats", "moved, died" ]
   ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");

// test horizontal_agg

var resp = mldb.get("/v1/query", {q: "SELECT horizontal_string_agg({who, what, how}, ', ') AS aggs FROM test ORDER BY rowName()", 
    format: 'table'});

plugin.log(resp.json);

assertEqual(resp.responseCode, 200, "Error executing query");

expected = [
   [ "_rowName", "aggs" ],
   [ "0", "kitchen, moved, mustard" ],
   [ "1", "kitchen, moved, plum" ],
   [ "2", "plum, stabbed, mustard" ],
   [ "3", "plum, killed, mustard" ],
   [ "4", "stabbed, died, plum" ]
];

assertEqual(mldb.diff(expected, resp.json, false /* strict */), {},
            "Query 2 output was not the same as expected output");


"success"
