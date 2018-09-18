// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var resp = mldb.query('select * ' + 
                      'from (select \'this is toy story time\' as title) as y ' +
                      'join transpose((select {"toy story": 1, "terminator": 5} as * ' +
                      'named \'rating\')) as x ' +
                      'where regex_match(y.title, \'.*\'+x.rowName()+\'.*\')');

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x.rating", 1, "-Inf" ],
         [ "y.title", "this is toy story time", "-Inf" ]
      ],
      "rowName" : "[result]-[toy story]"
   }
];

unittest.assertEqual(resp, expected);

"success"

