// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* MLDB 434: check round-trip through beh dataset. */

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


var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2014-01-01");

function recordExample(row, x, y, label, test)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts]]);
}

recordExample("ex00", 0, 0, 0);
recordExample("ex10", 1, 0, 1);
recordExample("ex01", 0, 1, 1);
recordExample("ex111", 1, 1, 1);
recordExample("ex110", 1, 1, 0);
recordExample("ex112", 1, 1, null);

dataset.commit()

var data = mldb.get('/v1/query', {q: "SELECT x,y,label from test where rowName() = 'ex112'", format:'table'}).json;
plugin.log(data);

var expected = [
   [ "_rowName", "label", "x", "y" ],
   [ "ex112", null, 1, 1 ]
];

assertEqual(mldb.diff(expected, data, false /* strict */), {},
            "Output was not the same as expected output");

"success"
