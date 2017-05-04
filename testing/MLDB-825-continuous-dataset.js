// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


function createDataset()
{
    var uri = "hello";
    uri;
}

mldb.log(createDataset.toString());

var createDatasetSource = '\
var uri = "file://tmp/MLDB-825-data/" + new Date().toISOString() + ".beh"; \
var config = { type: "beh.binary.mutable", params: { dataFileUrl: uri } }; \
var dataset = mldb.createDataset(config); \
mldb.log(config); \
mldb.log(dataset); \
var output = { config: dataset.config() }; \
output; \
';

var saveDatasetSource = '\
var uri = "file://tmp/MLDB-825-data/" + new Date().toISOString() + ".beh"; \
var addr = "/v1/datasets/" + args.datasetId; \
var res = mldb.post(addr + "/routes/saves", { dataFileUrl: uri });  \
mldb.log(res);  \
res = { metadata: mldb.get(addr).json.status, config: res.json }; \
res;'

var datasetConfig = {
    id: 'recorder',
    type: 'continuous',
    params: {
        commitInterval: "1s",
        metadataDataset: {
            type: 'sqliteSparse',
            id: 'metadata-db',
            params: {
                dataFileUrl: 'file://tmp/MLDB-825-metadata.sqlite'
            }
        },
        createStorageDataset: {
            type: 'script.run',
            params: {
                language: 'javascript',
                scriptConfig: {
                    source: createDatasetSource
                }
            }
        },
        /*
        createStorageDataset: {
            type: 'createEntity',
            params: {
                kind: 'dataset',
                type: 'beh.binary.mutable'
                dataFileUrl: '{ "file://" + args.when + ".beh" }'
            }
        },
*/
        saveStorageDataset: {
            type: 'script.run',
            params: {
                language: 'javascript',
                scriptConfig: {
                    source: saveDatasetSource
                }
            }
        }
    }
};

var numLines = 1000000;
//numLines = 200000;
//numLines = 10000;

var dataset = mldb.createDataset(datasetConfig);  

var now = new Date("2015-01-01");

var dataset_address = 'https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/1310438/reddituserpostingbehavior.csv.gz'
//var dataset_address = 'file://reddit_user_posting_behavior.csv';
var start = new Date();

var stream = mldb.openStream(dataset_address);

var lineNum = 0;

var rows = [];

while (!stream.eof() && lineNum < numLines) {
    ++lineNum;
    try {
        var line = stream.readLine();
    } catch (e) {
        if (stream.eof())
            break;
        throw e;
    }
    var fields = line.split(',');
    var tuples = [];
    for (var i = 1;  i < fields.length;  ++i) {
        tuples.push(["\"" + fields[i] + "\"", 1, new Date()]);
    }

    rows.push([fields[0], tuples]);

    if (rows.length == 1000) {
        dataset.recordRows(rows);
        rows = [];
    }

    if (lineNum % 100000 == 0) {
        plugin.log("loaded", lineNum, "lines");
        plugin.log(dataset.status());

        plugin.log(mldb.get('/v1/query', { q: 'select count(*) from recorder',
                                           format: 'table' }));
    }
}

dataset.recordRows(rows);

plugin.log("Committing dataset")
dataset.commit()

var end = new Date();

plugin.log("creating dataset took " + (end - start) / 1000 + " seconds");

plugin.log(mldb.get('/v1/query', { q: 'select * from "metadata-db"',
                                   format: 'table' }));

// Now create a window view on the dataset.  This is an immutable view that
// only gives us things that are available within a certain time-frame.

var windowConfig = {
    id: 'window',
    type: 'continuous.window',
    params: {
        metadataDataset: {
            id: 'metadata-db',
        },
        from: new Date(new Date() - 5000).toISOString(),  // 5 seconds ago
        to: new Date(new Date() - 0).toISOString()
    }
};

var windowDataset = mldb.createDataset(windowConfig);

mldb.log(windowDataset.status());

plugin.log(mldb.get('/v1/query', { q: 'select * from window limit 5',
                                   format: 'table' }));

"success"
