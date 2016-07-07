// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var resp1 = mldb.query('SELECT column expr () from (select x.a:1, y.b:2)');
var resp2 = mldb.query('SELECT * from (select x.a:1, y.b:2)');

mldb.log(resp1);
mldb.log(resp2);

assertEqual(resp1, resp2);

"success"

