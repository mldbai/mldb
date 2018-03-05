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

var functionConfig = {
    id: "expr",
    type: "llvm.expression",
    params: {
        expression: "x + y + 10",
        raw: true
    }
};

var res = mldb.post("/v1/functions", functionConfig);

if (res.responseCode != 201)
    throw "Error creating function: " + res.response;

var resp = mldb.get("/v1/functions/expr/info").json;

plugin.log(resp);

resp = mldb.get("/v1/functions/expr/application",
                    {input:{x:1, y:2}});

plugin.log(resp);

assertEqual(resp.json.output, 13);

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
