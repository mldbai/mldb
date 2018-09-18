// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var resp = mldb.query('SELECT replace_nan({*}, -1) AS res FROM (select x:0/0, x.y:0/0)');

mldb.log(resp);

var expected = [
    [ "res.x", -1, "-Inf" ],
    [ "res.x.y", -1, "-Inf" ]
];

unittest.assertEqual(resp[0].columns, expected);

var resp = mldb.query('SELECT replace_nan({*}, -1) AS res FROM (select x:0/0, x.y:0/0, y.z:0/0)');

mldb.log(resp);

var expected = [
    [ "res.x", -1, "-Inf" ],
    [ "res.x.y", -1, "-Inf" ],
    [ "res.y.z", -1, "-Inf" ]
];

unittest.assertEqual(resp[0].columns, expected);

"success"

