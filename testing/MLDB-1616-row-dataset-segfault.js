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

var resp = mldb.query('select * from (select 1) as x join atom_dataset({x:1}) as y');
mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x.1", 1, "-Inf" ],
         [ "y.column", { path: ["x"]}, "-Inf" ],
         [ "y.value", 1, "-Inf" ]
      ],
       "rowName" : "[result]-[0]"
   }
];

assertEqual(resp, expected);

var resp = mldb.query('select * from (select 1) as x join atom_dataset({x:1}) as y join row_dataset({z:2}) as z');
mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x.1", 1, "-Inf" ],
         [ "y.column", { path: ["x"] }, "-Inf" ],
         [ "y.value", 1, "-Inf" ],
         [ "z.column", { path: ["z"] }, "-Inf" ],
         [ "z.value", 2, "-Inf" ]
      ],
       "rowName" : "[result]-[[0]-[0]]"
   }
];

assertEqual(resp, expected);

"success"
