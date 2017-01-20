// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

function createDataset(numLines, datasetId)
{
    var start = new Date("2015-01-01");

    var dataset_config = {
        type:    'beh.mutable',
        id:      datasetId,
        params: { dataFileUrl: 'file://tmp/' + datasetId + '.beh' }
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz';
    var now = new Date();

    var stream = mldb.openStream(dataset_address);

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

function loadDataset(numLines, datasetId)
{
    var dataset_config = {
        type: 'beh',
        id: datasetId,
        params: {
            dataFileUrl: 'file://tmp/' + datasetId + '.beh'
        }
    };
    
    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    return dataset;
}

plugin.log(this);

var numLines = plugin.args.numLines;
var datasetId = plugin.args.datasetId;

if (!numLines)
    throw "numLines needs to be set for Reddit dataset";

if (!datasetId)
    throw "datasetId needs to be set for Reddit dataset";

var dataset;
try {
    dataset = loadDataset(numLines, datasetId);
} catch (e) {
    mldb.del("/v1/datasets/" + datasetId);
    dataset = createDataset(numLines, datasetId);
}
