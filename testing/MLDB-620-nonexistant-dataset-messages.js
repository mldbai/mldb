// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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
        testingDataset: { },
        score: "1",
        where: "true",
        label: "1"
    }
};

var resp1 = mldb.post("/v1/procedures", config1);
var resp2 = mldb.put("/v1/procedures/test1/runs/1", {});

plugin.log(resp1);
plugin.log(resp2);

assertEqual(resp1.responseCode, 201);
assertContains(resp2.json.error, "Attempt to obtain dataset without setting 'id'");

"success"

