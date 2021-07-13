// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')


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

unittest.assertEqual(resp.length, 5);


var resp = mldb.query('select rowPath(), rowPathElement(2) from test order by rowPath() desc limit 1');
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
unittest.assertEqual(resp, expected);


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
            "NaD"
         ],
         [ "x.1", 1, "-Inf" ],
         [ "y.column", { path: ["x"] }, "-Inf" ],
         [ "y.value", 1, "-Inf" ]
      ],
       "rowName" : "[result]-[0]"
   }
];

unittest.assertEqual(resp, expected);

"success"
