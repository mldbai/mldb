// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(expr, 'IS NOT EQUAL TO', val);

    throw "Assertion failure";
}

var resp = mldb.get("/v1/plugins");

mldb.log(resp);

var resp = mldb.get("/v1/plugins/MLDB-1398-plugin");

mldb.log(resp);

assertEqual(resp.json.status, "Hello, world");

"success"

