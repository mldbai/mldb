// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw new Error("Assertion failure: " + msg + ": " + JSON.stringify(expr)
                    + " not equal to " + JSON.stringify(val));
}


var procConfig = {
    type: "createEntity",
    params: {
        kind: 'dataset',
        type: "sparse.mutable"
    }
};

var resp = mldb.put("/v1/procedures/test", procConfig);

assertEqual(resp.responseCode, 201);

resp = mldb.put("/v1/procedures/test/runs/1", {});

assertEqual(resp.responseCode, 201);

mldb.log(resp.json);

assertEqual(resp.json.status.config.type, "sparse.mutable");
assertEqual(resp.json.status.kind, "dataset");
assertEqual(resp.json.status.status.rowCount, 0);

"success"

