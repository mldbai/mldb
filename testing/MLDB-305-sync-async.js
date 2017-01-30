// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of sync / async functionality. */

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var config = { type: "sparse.mutable" };

var output = mldb.put("/v1/datasets/test1", config);

var output2 = mldb.putAsync("/v1/datasets/test2", config);

var output3 = mldb.put("/v1/datasets/test3", config, {async:true});

var output4 = mldb.put("/v1/datasets/test4", config, {Async:true});

assertEqual(output.json.state, "ok", "test1 " + JSON.stringify(output.json));
assertEqual(output2.json.state, "initializing", "test2 " + JSON.stringify(output2.json));
assertEqual(output3.json.state, "initializing", "test3 " + JSON.stringify(output3.json));
assertEqual(output4.json.state, "initializing", "test4 " + JSON.stringify(output4.json));

"success"
