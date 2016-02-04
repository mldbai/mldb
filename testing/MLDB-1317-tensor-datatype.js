// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(expr, 'IS NOT EQUAL TO', val);

    throw "Assertion failure";
}

var resp = mldb.query("select [ [ 1, 2 ], [ 3, 4 ] ] as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "0.0", 1, "-Inf" ],
         [ "0.1", 2, "-Inf" ],
         [ "1.0", 3, "-Inf" ],
         [ "1.1", 4, "-Inf" ]
      ]
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select quantize(normalize([ [ 1, 2 ], [ 3, 4 ] ], 1), 0.1) as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "0.0", 0.1, "-Inf" ],
         [ "0.1", 0.2, "-Inf" ],
         [ "1.0", 0.3, "-Inf" ],
         [ "1.1", 0.4, "-Inf" ]
      ]
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select [ [1], [2] ] + [ [3], [4] ]");

mldb.log(resp);

"success"
