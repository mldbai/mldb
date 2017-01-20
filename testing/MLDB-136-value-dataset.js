// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type:    'sparse.mutable',
        id:      'reddit_dataset'
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
    var now = new Date();

    var stream = mldb.openStream(dataset_address);

    var numLines = 20000; //875000;

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

function loadDataset()
{
    var dataset_config = {
        type:    'sparse',
        id:      'reddit_dataset',
        address: 'tmp/reddit.3sm'
    };
    
    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    return dataset;
}

var dataset = createDataset();

/*
var dataset;
try {
    dataset = loadDataset();
} catch (e) {
    mldb.del("/v1/datasets/reddit_dataset");
    dataset = createDataset();
}
*/

plugin.log(mldb.get("/v1/datasets/reddit_dataset/query", {limit:10}));

"success"
