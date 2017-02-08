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

var fnConfig = {
    type: 'sql.expression',
    params: {
        expression: 'x * 10 as y'
    }
};

var fn = mldb.createFunction(fnConfig);

assertEqual(fn.type(), 'sql.expression');

var res = fn.call({ x: 10 });

mldb.log(res);

assertEqual(res[0][0], [ "y", [ 100, 'NaD' ]]);

var procConfig = {
    type: "null",
    params: {
    }
}
 
var proc = mldb.createProcedure(procConfig);   

assertEqual(proc.type(), 'null');

var res = proc.run({});

assertEqual(res, {});

mldb.log(res);

"success"
