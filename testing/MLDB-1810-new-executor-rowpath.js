// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var resp = mldb.query("select * named ['hello', 'world'] from row_dataset({x:1})");

mldb.log(resp);

// Make sure that the row name is properly structured, and not a single string
unittest.assertEqual(resp[0].rowName, "hello.world");

"success"
