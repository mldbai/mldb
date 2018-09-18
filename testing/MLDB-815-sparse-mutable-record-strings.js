// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var dataset = mldb.createDataset({type:'sparse.mutable',id:'test'});

var ts = new Date("2015-01-01");

function recordExample(row, x, y, label)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts], ["label", label, ts] ]);
}

recordExample("ex1", 0, 0, "cat");
recordExample("ex2", 1, 1, "dog");
recordExample("ex3", 1, 2, "cat");

dataset.commit()

// Testcase will fail here until issue is fixed
var resp = mldb.get("/v1/query", {q : 'SELECT * FROM test ORDER BY rowName() DESC', format: 'table'});

plugin.log(resp.json);

var expected = [
   [ "_rowName", "label", "x", "y" ],
   [ "ex3", "cat", 1, 2 ],
   [ "ex2", "dog", 1, 1 ],
   [ "ex1", "cat", 0, 0 ]
];

unittest.assertEqual(resp.json, expected);

"success"
