// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type: 'sqliteSparse',
        id: 'reddit_dataset',
        params: {
            //dataFileUrl: 'file:///mnt/s3cache/jeremy/MLDB-574-reddit.sqlite'
            dataFileUrl: 'file://tmp/MLDB-574-reddit.sqlite'
        }
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    //return dataset;

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
    //var dataset_address = 'file://reddit_user_posting_behavior.csv';
    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var numLines = 5000;

    var lineNum = 0;

    var rows = [];

    while (!stream.eof() && lineNum < numLines) {
        ++lineNum;
        if (lineNum % 1000 == 0)
            plugin.log("loaded", lineNum, "lines");
        var line = stream.readLine();
        var fields = line.split(',');
        var tuples = [];
        for (var i = 1;  i < fields.length;  ++i) {
            tuples.push([fields[i], 1, now]);
        }

        rows.push([fields[0], tuples]);

        // Commit every 5,000
        if (rows.length >= 5000) {
            dataset.recordRows(rows);
            rows = [];
        }
    }

    dataset.recordRows(rows);

    plugin.log("Committing dataset")
    dataset.commit()

    var end = new Date();
    
    plugin.log("creating dataset took " + (end - start) / 1000 + " seconds");

    return dataset;
}

var dataset = createDataset();

var res = mldb.get("/v1/query", {q: "select * from reddit_dataset limit 10"});

plugin.log(res);



function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
    plugin.log(process, response);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

function createAndTrainProcedure(config, name)
{
    var start = new Date();

    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);

    var end = new Date();

    plugin.log("procedure " + name + " took " + (end - start) / 1000 + " seconds");
}

// Create a dataset with just the column sums (number of users per row)
var userCountsConfig = {
    type: "transform",
    params: {
        inputData: { 
            select: 'columnCount() AS numUsers',
            from: { type: 'transposed',
                     params: { dataset: { id: 'reddit_dataset' } } },
            orderBy: 'columnCount() DESC, rowName()',
            named: "rowName() + '|1'",
            limit: 1000
        },
        outputDataset: { type: 'embedding', id: 'reddit_user_counts' }
    }
};

createAndTrainProcedure(userCountsConfig, "reddit_user_counts");

plugin.log(mldb.get("/v1/datasets/reddit_user_counts/query", {limit:100}));


// Create a SVD procedure.  We only want to embed the 1,000 columns with the
// highest row counts.

var svdConfig = {
    type: "svd.train",
    params: {
        trainingData: { "from" : {"id": "reddit_dataset" },
                        "select": "COLUMN EXPR (AS columnName() WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000)"             
                      },
        columnOutputDataset: { "id": "reddit_svd_embedding", type: "embedding" }
    }
};

createAndTrainProcedure(svdConfig, 'reddit_svd');

//plugin.log(mldb.get("/v1/datasets/svd_output/query", {select:'rowName()', limit:100}));

var kmeansConfig = {
    type: "kmeans.train",
    params: {
        trainingData: "select * from reddit_svd_embedding",
        outputDataset: {
            id: "reddit_kmeans_clusters", type: "embedding"
        }
    }
};

createAndTrainProcedure(kmeansConfig, 'reddit_kmeans');

var tsneConfig = {
    type: "tsne.train",
    params: {
        trainingData: { "from" : {"id": "reddit_svd_embedding" }},
        rowOutputDataset: { "id": "reddit_tsne_embedding", "type": "embedding" }
    }
};

createAndTrainProcedure(tsneConfig, 'reddit_tsne');

var mergedConfig = {
    type: "merged",
    id: "reddit_merged",
    params: {
        datasets: [
            { id: 'reddit_kmeans_clusters' },
            { id: 'reddit_tsne_embedding' },
            { id: 'reddit_user_counts' }
        ]
    }
};

var merged = mldb.createDataset(mergedConfig);

plugin.log(mldb.get("/v1/datasets/reddit_merged/query", {select:'*', limit:100}).json);

"success"
