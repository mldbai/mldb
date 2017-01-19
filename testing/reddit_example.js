// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Example script to import a reddit dataset and run an example */

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'reddit_dataset'
};

var dataset = mldb.createDataset(dataset_config)
plugin.log("Reddit data loader created dataset")

var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
var now = new Date();

var stream = mldb.openStream(dataset_address);

var numLines = 500;

var lineNum = 0;
while (!stream.eof() && lineNum < numLines) {
    ++lineNum;
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


function checkSuccess(response)
{
    if (response.responseCode > 200 && response.responseCode < 400)
        return;
    throw "Error: " + JSON.stringify(response);
}


// Create a SVD procedure

var trainSvd = true;

if (trainSvd) {

    var svdConfig = {
        type: "svd.train",
        params: {
            trainingData: { "from" : {"id": "reddit_dataset" }},
            columnOutputDataset: { "id": "svd_output", type: "embedding" },
            rowOutputDataset: { "id": "svd_embedding", type: "embedding" }
        }
    };

    var svdOutput = mldb.put("/v1/procedures/reddit_svd", svdConfig);

    plugin.log("svd output", svdOutput);


    // Run the training

    var trainingOutput = mldb.put("/v1/procedures/reddit_svd/runs/1", {});

    plugin.log("training output", trainingOutput);

    checkSuccess(trainingOutput);
}

//plugin.log("datasets", JSON.stringify(mldb.perform("GET", "/v1/datasets")));
//plugin.log(JSON.stringify(mldb.perform("GET", "/v1/datasets/svd_embedding")));

var trainKmeans = true;

if (trainKmeans) {

    var kmeansConfig = {
        type: "kmeans.train",
        params: {
            trainingData: "select embedding* from svd_embedding",
            outputDataset: {
                id: "kmeans_output", type: "embedding"
            }
        }
    };

    plugin.log(mldb.get("/v1/datasets/svd_embedding/query", {limit:10}));

    var kmeansOutput = mldb.put("/v1/procedures/reddit_kmeans", kmeansConfig);

    plugin.log("kmeans output", kmeansOutput);


    // Run the training

    var trainingOutput = mldb.put("/v1/procedures/reddit_kmeans/runs/1", {});
    plugin.log("kmeans training output", trainingOutput);

    checkSuccess(trainingOutput);
}

var trainTsne = true;

if (trainTsne) {

    var tsneConfig = {
        type: "tsne.train",
        params: {
            trainingData: { "select" : "embedding*",
                            "from" : { "id": "svd_embedding" }},
            rowOutputDataset: { "id": "tsne_output", "type": "embedding" },
        }
    };

    var tsneOutput = mldb.put("/v1/procedures/reddit_tsne", tsneConfig);

    plugin.log("tsne output", tsneOutput);


    // Run the training

    var trainingOutput = mldb.put("/v1/procedures/reddit_tsne/runs/1", {});

    plugin.log("tsne training output", trainingOutput);

    checkSuccess(trainingOutput);
}

var mergedConfig = {
    type: "merged",
    id: "reddit_embeddings",
    params: {
        "datasets": [
            { "id": "svd_embedding" },
            { "id": "tsne_output" },
            { "id": "kmeans_output" }
        ]
    }
};

var mergedDataset = mldb.createDataset(mergedConfig);

plugin.log(mldb.get("/v1/datasets/reddit_embeddings"));

plugin.log(mldb.get("/v1/datasets/reddit_embeddings/query",
                    {select:"*",limit:10}));

// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success!"
