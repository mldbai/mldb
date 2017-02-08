// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// Test for MLDB-605; timestamp queries

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

var ts1 = new Date("2015-01-01");
var ts2 = new Date("2015-01-02");
var ts3 = new Date("2015-01-03");

dataset.recordRow('row1', [ [ "x", 0, ts1 ], ["x", 1, ts2], ["x", 2, ts3] ]);

dataset.commit()

var query1 = mldb.get('/v1/query', { q: 'SELECT * from test' });

plugin.log(query1);

assertEqual(query1.json[0].columns.length, 3);

var query2 = mldb.get('/v1/query', { q: 'SELECT x from test' });

assertEqual(query2.json[0].columns.length, 3);

// Note that we return all tuples of a column for consistency (see MLDB-1370)
// assertEqual(query2.json, query1.json);

plugin.log(query2);

"success"
