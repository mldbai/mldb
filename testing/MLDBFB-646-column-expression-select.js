// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}


var resp = mldb.query("select column expr(select value() * 10) named 'res' from (select x:1, y:2)");

var expected = [
   {
      "columns" : [
         [ "x", 10, "-Inf" ],
         [ "y", 20, "-Inf" ]
      ],
      "rowHash" : "4ba25cf9b5244b88",
      "rowName" : "res"
   }
];

mldb.log(resp);

assertEqual(resp, expected);

resp = mldb.query("select column expr(select {a: value() * 10, b: value() * 20} ) named 'res' from (select x:1, y:2)");

mldb.log(resp);

expected = [
   {
      "columns" : [
         [ "x.a", 10, "-Inf" ],
         [ "x.b", 20, "-Inf" ],
         [ "y.a", 20, "-Inf" ],
         [ "y.b", 40, "-Inf" ]
      ],
      "rowHash" : "4ba25cf9b5244b88",
      "rowName" : "res"
   }
];

assertEqual(resp, expected);

"success"
