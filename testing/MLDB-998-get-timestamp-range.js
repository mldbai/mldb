// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

function testDataset(type)
{
    mldb.del('/v1/datasets/test');

    var dataset_config = {
        type    : type,
        id      : 'test',
    };

    var dataset = mldb.createDataset(dataset_config)

    var ts1 = new Date("2015-01-01");
    var ts2 = new Date("2015-01-02");
    var ts3 = new Date("2015-01-03");
    var ts4 = new Date("2015-03-01");

    dataset.recordRow('row1_imp_then_click', [ [ "imp", 0, ts1 ], ["click", 0, ts2] ]);
    dataset.recordRow('row2_imp_then_click_later', [ [ "click", 0, ts3 ], ["imp", 1, ts1] ]);
    dataset.recordRow('row3_click_and_imp', [ [ "click", 0, ts1 ], ["imp", 0, ts1] ]);

    dataset.commit();

    var range = dataset.getTimestampRange();

    mldb.log(range[0], range[1]);

    var resp = mldb.get('/v1/query', { q: 'select min(earliest_timestamp({*})) as earliest, max(latest_timestamp({*})) as latest from test'}).json;

    mldb.log(resp);

    unittest.assertEqual(range[0], ts1);
    unittest.assertEqual(range[1], ts3);

    unittest.assertEqual(new Date(resp[0].columns[0][1]['ts']), ts1);
    unittest.assertEqual(new Date(resp[0].columns[1][1]['ts']), ts3);
}

testDataset("beh.mutable");
testDataset("sparse.mutable");
testDataset("sqliteSparse");

"success"

