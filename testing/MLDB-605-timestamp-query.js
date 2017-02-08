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

dataset.recordRow('row1_imp_then_click', [ [ "imp", 0, ts1 ], ["click", 0, ts2] ]);
dataset.recordRow('row2_click_then_imp', [ [ "click", 0, ts1 ], ["imp", 0, ts2] ]);
dataset.recordRow('row3_click_and_imp', [ [ "click", 0, ts1 ], ["imp", 0, ts1] ]);

dataset.commit()

var query1 = mldb.get('/v1/query',
                      { q: 'select * from test where latest_timestamp(imp) < latest_timestamp(click)',
                        format: 'table', headers: false });

plugin.log(query1);

assertEqual(query1.json.length, 1);
assertEqual(query1.json[0][0], "row1_imp_then_click");

var query2 = mldb.get('/v1/query',
                      { q: 'select * from test where latest_timestamp(click) < latest_timestamp(imp)',
                        format: 'table', headers: false });

plugin.log(query2);

assertEqual(query2.json.length, 1);
assertEqual(query2.json[0][0], "row2_click_then_imp");

var query3 = mldb.get('/v1/query',
                      { q: 'select * from test where latest_timestamp(click) = latest_timestamp(imp)',
                        format: 'table', headers: false });

plugin.log(query3);

assertEqual(query3.json.length, 1);
assertEqual(query3.json[0][0], "row3_click_and_imp");

"success"
