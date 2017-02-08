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


var resp = mldb.query("select * named ['hello', 'world'] from row_dataset({x:1})");

mldb.log(resp);

// Make sure that the row name is properly structured, and not a single string
assertEqual(resp[0].rowName, "hello.world");

"success"
