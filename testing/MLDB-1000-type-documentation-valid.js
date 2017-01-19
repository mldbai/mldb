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

var types = [ 'procedures', 'functions', 'datasets', 'plugins' ];

for (var i = 0;  i < types.length;  ++i) {
    var resp = mldb.get('/v1/types/' + types[i], { details: true });
    assertEqual(resp.responseCode, 200);
}

"success"
