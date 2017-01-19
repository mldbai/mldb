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

var dir2 = mldb.ls("docker://quay.io/datacratic/datacratic-ubuntu:jeremy-test");
mldb.log(dir2);

assertEqual(dir2.objects["docker://quay.io/datacratic/datacratic-ubuntu:jeremy-test/etc/timezone"].exists, true);

"success"
