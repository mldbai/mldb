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


var config = { type: "sparse.mutable" };
var dataset = mldb.createDataset(config);

mldb.log(config);
mldb.log(dataset.id());

if (config.id.indexOf("auto_") != 0)
    throw "ID should start with 'auto_' : '" + config.id + "'";

assertEqual(config.id, dataset.id());

"success"

