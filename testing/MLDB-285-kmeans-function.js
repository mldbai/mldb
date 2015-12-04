# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Example script to import a reddit dataset and run an example */

function assert(expr, msg)
{
    if (expr)
        return;

    throw "Assertion failure: " + msg;
}

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'gaussian',
};

var dataset = mldb.createDataset(dataset_config)
plugin.log("Reddit data loader created dataset")

var rng = mldb.createRandomNumberGenerator(1 /* seed */);

var now = new Date();

var names = [ "x", "y" ];

// Create an example with gaussians to train the k-means opn

function addExample(rowName, centroid, stddev)
{
    var row = [];
    for (var i = 0;  i < centroid.length;  ++i) {
        row.push([names[i], centroid[i] + rng.normal(0, stddev), now]);
    }

    //plugin.log("normal dist = ", rng.normal());
    
    dataset.recordRow(rowName, row)
}

for (var i = 0;  i < 200;  ++i)
    addExample("row0_" + i, [-1, -1], 0.2);

for (var i = 0;  i < 200;  ++i)
    addExample("row1_" + i, [1, 1], 0.2);

plugin.log("Committing gaussian dataset")
dataset.commit()


//plugin.log(mldb.perform("GET", "/v1/datasets/gaussian/query"));


// Train the k-means algorithm

var kmeansConfig = {
    type: "kmeans.train",
    params: {
        numClusters: 2,
        metric: "euclidean",
        trainingData: "select * from gaussian",
        outputDataset: { "id": "kmeans_output", type: "embedding" },
        centroidsDataset: { "id": "kmeans_centroids", type: "embedding" }
    }
};

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

var kmeansOutput = mldb.put("/v1/procedures/kmeans", kmeansConfig);
    
assertSucceeded("kmeans creation", kmeansOutput);

// Run the training

var trainingOutput = mldb.put("/v1/procedures/kmeans/runs/1", {});

assertSucceeded("kmeans training", trainingOutput);

// Dump the centroids.  There should be two, one around (1,1) and the other
// around (0,0).
var centroids = mldb.perform("GET", "/v1/datasets/kmeans_centroids/query");

//assert(centroids.length == 2, "Centroids should have length 2");


plugin.log(mldb.perform("GET", "/v1/datasets/kmeans_centroids/query"));

// Create a function to re-run the kmeans
var functionConfig = {
    type: "kmeans",
    params: {
        centroids: { id: "kmeans_centroids" },
        metric: "euclidean"
    }
};

var functionOutput = mldb.put("/v1/functions/kmeans", functionConfig);

assertSucceeded("function creation", functionOutput);


var functionApplicationOutput = mldb.get("/v1/functions/kmeans/application", { input: { embedding: {x:1,y:1 }}});
assertSucceeded("function application", functionApplicationOutput);

var select = "{*} as embedding";

var queryOutput = mldb.get("/v1/datasets/gaussian/query",
                           { select: select, limit: 10 });

plugin.log(queryOutput, queryOutput.json);

assertSucceeded("query application", queryOutput);

var select = "kmeans({{*} as embedding})[cluster]";

var queryOutput = mldb.get("/v1/datasets/gaussian/query",
                           { select: select, limit: 10 });
assertSucceeded("query application", queryOutput);

plugin.log(queryOutput, queryOutput);

"success"
