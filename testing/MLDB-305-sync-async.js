// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of sync / async functionality. */

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var config = { type: "sparse.mutable" };

var output = mldb.put("/v1/datasets/test1", config);

var output2 = mldb.putAsync("/v1/datasets/test2", config);

var output3 = mldb.put("/v1/datasets/test3", config, {async:true});

var output4 = mldb.put("/v1/datasets/test4", config, {Async:true});

unittest.assertEqual(output.json.state, "ok", "test1 " + JSON.stringify(output.json));
unittest.assertEqual(output2.json.state, "initializing", "test2 " + JSON.stringify(output2.json));
unittest.assertEqual(output3.json.state, "initializing", "test3 " + JSON.stringify(output3.json));
unittest.assertEqual(output4.json.state, "initializing", "test4 " + JSON.stringify(output4.json));

"success"
