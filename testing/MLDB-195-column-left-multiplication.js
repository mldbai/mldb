// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// See MLDB-195
// Check we can left multiply by a columns

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var datasetConfig = { "id": "ds1", "type": "sparse.mutable" };

var dataset = mldb.createDataset(datasetConfig);

var ts = new Date();

dataset.recordRow("row1", [ [ "Weight", 1, ts ], ["col2", 2, ts] ]);

dataset.commit();

var resp = mldb.get('/v1/query', { q: 'select 2.2 * Weight from ds1', format: 'table' });

mldb.log(resp.json);

var expected = [
   [ "_rowName", "\"2.2 * Weight\"" ],
   [ "row1", 2.20 ]
];

assertEqual(resp.json, expected);

"success"

