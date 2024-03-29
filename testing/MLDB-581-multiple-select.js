// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** Test case for MLDB-581 */

var mldb = require('mldb')
var unittest = require('mldb/unittest')

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        'type'    : 'sparse.mutable',
        'id'      : 'reddit_dataset',
    };
    
    var dataset = mldb.createDataset(dataset_config)

    var dataset_address = 'file://mldb/mldb_test_data/reddit.csv.zst';
    var now = new Date();

    var stream = mldb.openStream(dataset_address);

    var numLines = 1000;

    var lineNum = 0;
    while (!stream.eof() && lineNum < numLines) {
        ++lineNum;
        if (lineNum % 100000 == 0)
            plugin.log("loaded", lineNum, "lines");
        var line = stream.readLine();
        var fields = line.split(',');
        var tuples = [];
        for (var i = 1;  i < fields.length;  ++i) {
            tuples.push([fields[i], 1, now]);
        }

        dataset.recordRow(fields[0], tuples);
    }

    plugin.log("Committing dataset")
    dataset.commit()

    var end = new Date();
    
    plugin.log("creating dataset took " + (end - start) / 1000 + " seconds");

    return dataset;
}

var dataset = createDataset();

var res1 = mldb.get("/v1/query", {q: "select * from reddit_dataset limit 10", format:"sparse"}).json;
var res2 = mldb.get("/v1/query", {q: "select * from reddit_dataset limit 10", format:"sparse"}).json;

unittest.assertEqual(res1, res2);

"success"
