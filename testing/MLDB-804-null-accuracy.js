// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// MLDB-804
// Check we validate accuracy config

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

var config = {
    type: "classifier.test",
    params: {
        // no label, score, weight, dataset set here
    }
};

var resp = mldb.put("/v1/procedures/test1", config);

mldb.log(resp);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.error, "classifier.test expects a scalar named 'score' and a scalar named 'label'");

"success"
