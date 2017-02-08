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

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");

function recordExample(row, x, y, z)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
    if (z)
        dataset.recordRow(row, [ [ "z", z, ts ] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 3);
recordExample("ex2", 1, 2, "yes");
recordExample("ex3", 2, 1);
recordExample("ex4", 3, 0, "no");

dataset.commit()

var res1 = mldb.get("/v1/query",
                    { q: "SELECT jseval('syntax error', 'x') from test" });

plugin.log(res1);

assertEqual(res1.responseCode, 400);
assertContains(res1.json.error, "Exception compiling");

var res2 = mldb.get("/v1/query",
                    { q: "SELECT jseval('throw 3', '') from test" });

plugin.log(res2);

assertEqual(res2.responseCode, 400);
assertContains(res2.json.error, "Exception running");

//MLDB-1714
var res3 = mldb.get("/v1/query",
                    { q: "select jseval('return 3;') FROM test" });

assertContains(res3.json.error, "jseval expected at least 2 arguments");

plugin.log(res3);

"success"
