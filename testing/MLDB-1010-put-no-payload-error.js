// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// MLDB-1010
// Check the error message for PUT and POST with an empty body

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

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
}

var resp = mldb.put("/v1/procedures/test");

mldb.log(resp.json);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.error, "empty payload");

resp = mldb.post("/v1/procedures");

mldb.log(resp.json);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.error, "empty payload");

"success"
