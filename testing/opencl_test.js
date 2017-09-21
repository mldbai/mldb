// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of SQL expression function. */

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var pluginConfig = {
    id: 'cl',
    type: 'opencl',
    params: {
    }
};

var resp = mldb.post('/v1/plugins', pluginConfig);

plugin.log(resp);

assertEqual(resp.responseCode, 201);

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
