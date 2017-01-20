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

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");
dataset.recordRow("0", [ [ "0", 0, ts ] ]);
dataset.recordRow(1, [ [ 0, 0, ts ] ]);

dataset.commit()

var expected = [
   [ "_rowName", "0" ],   
   [ "1", 0 ],
   [ "0", 0 ]
];

var resp = mldb.get("/v1/query", { q: 'select * from test', format: 'table' });

plugin.log(resp.json);

assertEqual(resp.json, expected);

var resp = mldb.get("/v1/query", { q: 'select "0" from test', format: 'table' });

plugin.log(resp.json);

assertEqual(resp.json, expected);

"success"
