// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(JSON.stringify(expr), 'IS NOT EQUAL TO', JSON.stringify(val));

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
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select quantize(normalize([ [ 10, 20 ], [ 30, 40 ] ], 1), 0.1) as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "0.0", 0.1, "-Inf" ],
         [ "0.1", 0.2, "-Inf" ],
         [ "1.0", 0.30000000000000004, "-Inf" ],
         [ "1.1", 0.4, "-Inf" ]
      ]
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select [ [1], [2] ] + [ [3], [4] ] as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "0.0", 4, "-Inf" ],
         [ "1.0", 6, "-Inf" ]
      ]
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select [ [1], [2] ] + [ ['three'], ['four'] ] as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "0.0", "1three", "-Inf" ],
         [ "1.0", "2four", "-Inf" ]
      ]
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select { x: 1, y: 2} + 1 as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x", 2, "-Inf" ],
         [ "y", 3, "-Inf" ]
      ]
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select 1 + { x: 1, y: 2} as *");

mldb.log(resp);

assertEqual(resp, expected);

var resp = mldb.query("select { x: 1, y: 2} * { x: 3, y: 4} as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x", 3, "-Inf" ],
         [ "y", 8, "-Inf" ]
      ]
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

var resp = mldb.query("select { x: 1, y: 2} * { x: 3, y: 4, z: 5 } as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x", 3, "-Inf" ],
         [ "y", 8, "-Inf" ],
         [ "z", null, "-Inf" ]
      ]
      ,"rowHash":"d54892b736cac3ab","rowName":"result"
   }
];

assertEqual(resp, expected);

// MLDB-1564

var resp = mldb.query("select vector_sum([1,2], null) as *");

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ 0, 1, "1970-01-01T00:00:00Z" ],
         [ 1, 2, "1970-01-01T00:00:00Z" ]
      ],
      "rowHash" : "d54892b736cac3ab",
      "rowName" : "result"
   }
];

assertEqual(resp, expected);

"success"
