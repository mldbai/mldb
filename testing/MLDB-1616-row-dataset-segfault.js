// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

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

unittest.assertEqual(resp, expected);

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

unittest.assertEqual(resp, expected);

"success"
