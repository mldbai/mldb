// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

// Make sure a valid character code works
var str = "string with embedded chars " + String.fromCharCode(17);

var resp = mldb.query("select '" + str + "' as res");

mldb.log(resp);

unittest.assertEqual(resp[0].columns[0][1], str);

// Make sure a null character doesn't work

var str = "string with embedded chars " + String.fromCharCode(0);

var resp = mldb.get('/v1/query', { q: "select '" + str + "' as res" });

mldb.log(resp);

unittest.assertEqual(resp.responseCode, 400);

"success"

