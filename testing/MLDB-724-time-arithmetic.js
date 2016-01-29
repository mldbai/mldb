// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

// Test for MLDB-724; timestamp arithmetics

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
var ts4 = new Date("2015-03-01");

dataset.recordRow('row1_imp_then_click', [ [ "imp", 0, ts1 ], ["click", 0, ts2] ]);
dataset.recordRow('row2_imp_then_click_later', [ [ "click", 0, ts3 ], ["imp", 1, ts1] ]);
dataset.recordRow('row3_click_and_imp', [ [ "click", 0, ts1 ], ["imp", 0, ts1] ]);

dataset.commit()

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'testlight',
};

var datasetlight = mldb.createDataset(dataset_config)
datasetlight.recordRow('uniquerow', [ [ "x", 0, ts1 ] ]);

datasetlight.commit();

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'testlong',
};

var datasetlong = mldb.createDataset(dataset_config)
datasetlong.recordRow('uniquerow', [ [ "imp", 0, ts3 ], ["click", 0, ts4] ]);

datasetlong.commit();

plugin.log("\nInterval Seconds\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'10 s\' = INTERVAL \'10second\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'22S\' = INTERVAL \'22 SECOND\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Minutes\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'10 minute\' = INTERVAL \'600second\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'60 MINUTE\' = INTERVAL \'1H\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Hours\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'2H\' = INTERVAL \'120m\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'2 hour\' = INTERVAL \'2 HOUR\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Days\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1 d\' = INTERVAL \'1day\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1D\' = INTERVAL \'1 DAY\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Weeks\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1 w\' = INTERVAL \'7day\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1week\' = INTERVAL \'1 WEEK\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Months\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1 month\' = INTERVAL \'30day\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1MONTH\' = INTERVAL \'1 month\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Year\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1 year\' = INTERVAL \'365day\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1YEAR\' = INTERVAL \'1 Y\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nInterval Mixed\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1Y2W\' = INTERVAL \'379day\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

query = mldb.get('/v1/datasets/testlight/query',
                      { select: 'INTERVAL \'1 day 5H\' = INTERVAL \'104400 second\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nTimestamp Integer addition\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: '(when(imp) + 1) < when(click)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "row2_imp_then_click_later");

plugin.log("\nTimestamp Integer addition commutative\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: '1 + when(imp) < when(click)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "row2_imp_then_click_later");

plugin.log("\n Timestamp Interval Addition\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: 'when(imp) + INTERVAL \'1d\' = when(click)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "row1_imp_then_click");

plugin.log("\n Timestamp Interval Addition Complex\n");

query = mldb.get('/v1/datasets/testlong/query',
                      { where: 'when(imp) + INTERVAL \'1month 29 day 33 minute \' > when(click)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Timestamp Interval Addition commutative\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: 'INTERVAL \'1d\' + when(imp) = when(click)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "row1_imp_then_click");

plugin.log("\n Interval Addition and compare\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { where: 'INTERVAL \'1d\' + INTERVAL \'2d\' < INTERVAL \'20d\'',
                        format: 'table', headers: false });


assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\nTimestamp substraction\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: 'when(click) - when(imp) < INTERVAL \'2d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 2);

plugin.log("\nTimestamp Integer substraction\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: 'when(click) - 1 > when(imp)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "row2_imp_then_click_later");

plugin.log("\nTimestamp Interval substraction\n");

query = mldb.get('/v1/datasets/test/query',
                      { where: 'when(click) - INTERVAL \'1d\' > when(imp)',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "row2_imp_then_click_later");

plugin.log("\n Interval unary substraction\n");

query = mldb.get('/v1/datasets/testlight/query',
                      { where: 'INTERVAL \'1d\' > - INTERVAL \'1d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Interval unary substraction\n");
query = mldb.get('/v1/datasets/testlight/query',
                      { where: '- INTERVAL \'1d\' < INTERVAL \'1d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Interval substraction\n");
query = mldb.get('/v1/datasets/testlight/query',
                      { where: 'INTERVAL \'3d\' - INTERVAL \'1d\' < INTERVAL \'3d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Interval multiplication\n");
query = mldb.get('/v1/datasets/testlight/query',
                      { where: 'INTERVAL \'1d\' * 3 = INTERVAL \'3d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Interval multiplication commutative\n");
query = mldb.get('/v1/datasets/testlight/query',
                      { where: '(3 * INTERVAL \'1d\') > (INTERVAL \'2d\')',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Interval multiplication float\n");
query = mldb.get('/v1/datasets/testlight/query',
                      { where: 'INTERVAL \'4d\' * 0.5 = INTERVAL \'2d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

plugin.log("\n Interval division\n");
query = mldb.get('/v1/datasets/testlight/query',
                      { where: 'INTERVAL \'4d\' / 2 = INTERVAL \'2d\'',
                        format: 'table', headers: false });

assertEqual(query.json.length, 1);
assertEqual(query.json[0][0], "uniquerow");

//MLDB-903

plugin.log("\n MLDB-903\n");

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test3',
};

var dataset3 = mldb.createDataset(dataset_config)
dataset3.recordRow('myrow', [ [ "a", 0, ts1 ], ["b", 0, ts2], ["c", 0, ts3] ]);

dataset3.commit();

query = mldb.get('/v1/datasets/test3/query',
                      { select: 'when({a,b}), min_timestamp({a,b}), max_timestamp({a,b})'});

plugin.log(query);
assertEqual(query.json[0].columns[0][1]['ts'], "2015-01-02T00:00:00Z");
assertEqual(query.json[0].columns[1][1]['ts'], "2015-01-01T00:00:00Z");
assertEqual(query.json[0].columns[2][1]['ts'], "2015-01-02T00:00:00Z");

query = mldb.get('/v1/datasets/test3/query',
                      { select: 'when({*}), min_timestamp({*}), max_timestamp({*})'});

plugin.log(query);
assertEqual(query.json[0].columns[0][1]['ts'], "2015-01-03T00:00:00Z");
assertEqual(query.json[0].columns[1][1]['ts'], "2015-01-01T00:00:00Z");
assertEqual(query.json[0].columns[2][1]['ts'], "2015-01-03T00:00:00Z");

query = mldb.get('/v1/datasets/test3/query',
                      { select: 'when({a, {b, c}}), min_timestamp({a, {b, c}}), max_timestamp({z, {b, c}})'});

plugin.log(query);
assertEqual(query.json[0].columns[0][1]['ts'], "2015-01-03T00:00:00Z");
assertEqual(query.json[0].columns[1][1]['ts'], "2015-01-01T00:00:00Z");
assertEqual(query.json[0].columns[2][1]['ts'], "2015-01-03T00:00:00Z");

//MLDB-1230

query = mldb.get('/v1/datasets/test3/query',
                      { select: 'CAST (INTERVAL \'1s\' as integer) as x'});
plugin.log(query);

"success"
