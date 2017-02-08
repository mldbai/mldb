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

var resp = mldb.query('SELECT replace_nan({*}, -1) AS res FROM (select x:0/0, x.y:0/0)');

mldb.log(resp);

var expected = [
    [ "res.x", -1, "-Inf" ],
    [ "res.x.y", -1, "-Inf" ]
];

assertEqual(resp[0].columns, expected);

var resp = mldb.query('SELECT replace_nan({*}, -1) AS res FROM (select x:0/0, x.y:0/0, y.z:0/0)');

mldb.log(resp);

var expected = [
    [ "res.x", -1, "-Inf" ],
    [ "res.x.y", -1, "-Inf" ],
    [ "res.y.z", -1, "-Inf" ]
];

assertEqual(resp[0].columns, expected);

"success"

