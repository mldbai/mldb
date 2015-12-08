// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Example script to import a reddit dataset and run an example */

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

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type: 'text.line',
        id: 'reddit_text_file',
        params: {
            //dataFileUrl: 'file://reddit_user_posting_behavior.csv'
            dataFileUrl: 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
        }
    };

    var now = new Date();

    var dataset = mldb.createDataset(dataset_config);

    var end = new Date();
    
    plugin.log("creating text dataset took " + (end - start) / 1000 + " seconds");

    var transformConfig = {
        type: "transform",
        params: {
            inputDataset: { id: 'reddit_text_file' },
            outputDataset: { type: 'sparse.mutable', id: 'reddit_dataset' },
            //outputDataset: { type: 'beh.binary.mutable', id: 'reddit_dataset' },
            select: "parse_sparse_csv(lineText) as reddit",
            //rowName: "regex_replace(lineText, '([^,]\+).*', '\\1')",
            //rowName: "lineNumber"
            rowName: "jseval('return x.substr(0, x.indexOf('',''));', 'x', lineText)"
        }
    };

    createAndTrainProcedure(transformConfig, "dataset import");

    // Save the dataset for later
    //var saveResp = mldb.post("/v1/datasets/reddit_dataset/routes/saves",
    //                         {dataFileUrl: 'file://tmp/MLDB-499.beh'});
    //plugin.log(saveResp);
}

function createDatasetOld()
{
    var start = new Date();

    var dataset_config = {
        type: 'sparse.mutable',
        id: 'reddit_dataset',
        params: {
            dataFileUrl: 'file://tmp/MLDB-4660-reddit.beh'
        }
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    //var dataset_address = 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
    var dataset_address = 'file://reddit_user_posting_behavior.csv'

    var now = new Date();

    var stream = mldb.openStream(dataset_address);

    var numLines = 875000;

    var lineNum = 0;

    var rows = [];

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

        rows.push([fields[0], tuples]);

        // Commit every 100000
        if (rows.length >= 100000) {
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

function loadDataset()
{
    var dataset_config = {
        'type'    : 'beh.binary',
        'id'      : 'reddit_dataset',
        'address' : 'file://tmp/MLDB-466-reddit.beh'
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    return dataset;
}

//var dataset = createDataset();

var dataset;
try {
    throw "disabled"
    dataset = loadDataset();
} catch (e) {
    mldb.del("/v1/datasets/reddit_text_file");
    mldb.del("/v1/datasets/reddit_dataset");
    dataset = createDataset();
}


// Create a dataset with just the column sums (number of users per reddit)
var userCountsConfig = {
    type: "transform",
    params: {
        inputDataset: { type: 'transposed',
                        params: { dataset: { id: 'reddit_dataset' } } },
        outputDataset: { type: 'embedding', id: 'reddit_user_counts' },
        select: 'columnCount() AS numUsers',
        orderBy: 'columnCount() DESC, rowName()',
        limit: 1000,
        rowName: "rowName() + '|1'"
    }
};

createAndTrainProcedure(userCountsConfig, "reddit_user_counts");

plugin.log(mldb.get("/v1/datasets/reddit_user_counts/query", {limit:100}));


// Create a SVD procedure.  We only want to embed the 1,000 columns with the
// highest row counts.

var svdConfig = {
    type: "svd.train",
    params: {
        trainingDataset: { "id": "reddit_dataset" },
        columnOutputDataset: { "id": "reddit_svd_embedding", type: "embedding" },
        select: "COLUMN EXPR (AS columnName() WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000)"
    }
};

createAndTrainProcedure(svdConfig, 'reddit_svd');

plugin.log(mldb.get("/v1/datasets/svd_output/query", {select:'rowName()', limit:100}));

var kmeansConfig = {
    type: "kmeans.train",
    params: {
        trainingDataset: { "id": "reddit_svd_embedding" },
        select: "*",
        outputDataset: {
            id: "reddit_kmeans_clusters", type: "embedding"
        }
    }
};

createAndTrainProcedure(kmeansConfig, 'reddit_kmeans');

var tsneConfig = {
    type: "tsne.train",
    params: {
        trainingDataset: { "id": "reddit_svd_embedding" },
        rowOutputDataset: { "id": "reddit_tsne_embedding", "type": "embedding" },
        select: "*",
        where: "true"
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

//MLDB-1176 merge function in FROM expression

var expected = mldb.get("/v1/query", {q:"SELECT * FROM reddit_merged LIMIT 10", format:'table'});
plugin.log(expected)

var resp = mldb.get("/v1/query", {q:"SELECT * FROM merge(reddit_kmeans_clusters, reddit_tsne_embedding, reddit_user_counts) LIMIT 10", format:'table'});
plugin.log(resp)

assertEqual(resp.json, expected.json);

"success"
