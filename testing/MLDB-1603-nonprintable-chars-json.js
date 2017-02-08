// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

// Make sure a valid character code works
var str = "string with embedded chars " + String.fromCharCode(17);

var resp = mldb.query("select '" + str + "' as res");

mldb.log(resp);

assertEqual(resp[0].columns[0][1], str);

// Make sure a null character doesn't work

var str = "string with embedded chars " + String.fromCharCode(0);

var resp = mldb.get('/v1/query', { q: "select '" + str + "' as res" });

mldb.log(resp);

assertEqual(resp.responseCode, 400);

"success"

