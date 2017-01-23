// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

var scriptConfig = {
    address: "file://mldb/testing/MLDB-980-inner-script.js"
};

var res = mldb.post('/v1/types/plugins/javascript/routes/run', scriptConfig);

mldb.log(res);

assertEqual(res.responseCode, 400);
assertEqual(res.json.exception.message, "Uncaught SyntaxError: Invalid or unexpected token");

"success"
