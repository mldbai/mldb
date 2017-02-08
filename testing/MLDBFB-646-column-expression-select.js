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


var resp = mldb.query("select column expr(select value() * 10) named 'res' from (select x:1, y:2)");

var expected = [
   {
      "columns" : [
         [ "x", 10, "-Inf" ],
         [ "y", 20, "-Inf" ]
      ],
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
      "rowName" : "res"
   }
];

assertEqual(resp, expected);

resp = mldb.query("select column expr(as parse_path(parse_path(columnName()))) named 'res' from (select \"x.y.z\":1, \"x.y.y\":2)");

mldb.log(resp);

expected = [
   {
      "columns" : [
         [ "x.y.y", 2, "-Inf" ],
         [ "x.y.z", 1, "-Inf" ]
      ],
      "rowName" : "res"
   }
];

assertEqual(resp, expected);

resp = mldb.query("select column expr(as columnPathElement(0)) named 'res' from (select \"x.y.z\":1, \"x.y.y\":2)");

mldb.log(resp);

resp = mldb.query("select column expr(as parse_path(columnPathElement(0))) named 'res' from (select \"x.y.z\":1, \"x.y.y\":2)");

mldb.log(resp);

assertEqual(resp, expected);

resp = mldb.query("select column expr(as unflatten_path(columnPath())) named 'res' from (select \"x.y.z\":1, \"x.y.y\":2)");

mldb.log(resp);

assertEqual(resp, expected);

"success"
