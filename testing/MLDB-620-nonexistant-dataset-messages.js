// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of messages for nonexistant datasets */

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


var config1 = {
    id: "test1",
    type: "classifier.test",
    params: {
        testingData: { }
    }
};

var resp1 = mldb.post("/v1/procedures", config1);

plugin.log(resp1);

assertEqual(resp1.responseCode, 400);
assertContains(resp1.json.error, "classifier.test expects a scalar named 'score' and a scalar named 'label'");

"success"
