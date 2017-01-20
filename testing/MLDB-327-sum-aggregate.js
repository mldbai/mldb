// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of sum aggregate (MLDB-327). */

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date();

function recordExample(row, x, y, label)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0, "cat");
recordExample("ex2", 1, 1, "dog");
recordExample("ex3", 1, 2, "cat");

dataset.commit()

var resp = mldb.get("/v1/query", {q: "select label,sum(x),vertical_sum(y) from test group by label order by label"});

plugin.log(resp);


/* expected output
[
      {
         "columns" : [
            [ "label", "cat", "1970-01-01T00:00:00.000Z" ],
            [ "sum(x)", 1, "1970-01-01T00:00:00.000Z" ],
            [ "sum(y)", 2, "1970-01-01T00:00:00.000Z" ]
         ],
         "rowHash" : "554f96c80ea05ddb",
         "rowName" : "[\"cat\"]"
      },
      {
         "columns" : [
            [ "label", "dog", "1970-01-01T00:00:00.000Z" ],
            [ "sum(x)", 1, "1970-01-01T00:00:00.000Z" ],
            [ "sum(y)", 1, "1970-01-01T00:00:00.000Z" ]
         ],
         "rowHash" : "d55e0e284796f79e",
         "rowName" : "[\"dog\"]"
      }
]
*/

function assertEqual(expr, val, msg)
{
    if (expr != val) {
        throw "Assertion failure: " + msg + ": " + expr + " not equal to " + val;
    }
}

assertEqual(resp.responseCode, 200, "response code");
assertEqual(resp.json[0].rowName, '"[""cat""]"', "rowName");  // MLDB-363
assertEqual(resp.json[0].columns[0][1], "cat", "label1");
assertEqual(resp.json[0].columns[1][1], 1, "sum of x for cat");
assertEqual(resp.json[0].columns[2][1], 2, "sum of y for cat");

"success"
