// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

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
    dataset.recordRow(["examples", row++], [ [ "who", who, ts ], ["what", what, ts], ["how", how, ts] ]);
}

recordExample("mustard", "moved", "kitchen");
recordExample("plum", "moved", "kitchen");
recordExample("mustard", "stabbed", "plum");
recordExample("mustard", "killed", "plum");
recordExample("plum", "died", "stabbed");

dataset.commit()

var resp = mldb.query('select rowPath(), rowPathElement(0), rowPathElement(1), rowPathElement(-1), * from test where rowPathElement(-1) = rowPathElement(1)');
mldb.log(resp);

assertEqual(resp.length, 5);


var resp = mldb.query('select rowPath(), rowPathElement(2) from test limit 1');
mldb.log(resp);

var expected = [
   {
      "columns" : [
         [
            "rowPath()",
            {
               "path" : [ "examples", "4" ]
            },
            "-Inf"
         ],
         [ "rowPathElement(2)", null, "-Inf" ]
      ],
      "rowName" : "examples.4"
   }
];
mldb.log(resp);
assertEqual(resp, expected);


var resp = mldb.query('select rowPath(), * from (select 1) as x join row_dataset({x:1}) as y');
mldb.log(resp);

var expected = [
   {
      "columns" : [
         [
            "rowPath()",
            {
               "path" : [ "[result]-[0]" ]
            },
            "-Inf"
         ],
         [ "x.1", 1, "-Inf" ],
         [ "y.column", { path: ["x"] }, "-Inf" ],
         [ "y.value", 1, "-Inf" ]
      ],
       "rowName" : "[result]-[0]"
   }
];

assertEqual(resp, expected);

"success"
