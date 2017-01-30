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

var res = mldb.get('/v1/query', {q: "select 'ç'", format: "aos"});

mldb.log(res);

assertEqual(res.responseCode, 200);

var expected = [
    {
        "'ç'" : "ç",
        "_rowName" : "result"
    }
];

assertEqual(res.json, expected);

"success"
