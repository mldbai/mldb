// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
}

var config = {
    type: "merged",
    params: {
        "datasets": [
            { id: "doesntexist1" },
            { id: "doesntexist2" }
        ]
    }
};

var resp = mldb.put("/v1/datasets/test", config);

plugin.log(resp);

var resp2 = mldb.get("/v1/datasets/test");

plugin.log(resp2);

unittest.assertEqual(resp2.json.config.type, "merged", "checking merged");
unittest.assertEqual(resp2.json.type, "merged", "checking type");
unittest.assertEqual(resp2.json.config.id, "test", "checking test");

// MLDB-630

var resp3 = mldb.get("/v1/datasets/test/routes/hello");

plugin.log(resp3);

unittest.assertEqual(resp3.responseCode, 404);
assertContains(resp3.json.error, "not available due to error in creation");


"success"

