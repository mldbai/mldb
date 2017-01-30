// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of type routes. */

var config = { id: "test1", type: "sparse.mutable" };

var output = mldb.post("/v1/datasets", config);

plugin.log("output", output);

if (output.json.state != "ok")
    throw "Expected sync plugin to be in state 'ok', not '" + output.json.state + "'";

"success"
