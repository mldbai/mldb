// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

// Train the k-means algorithm
var kmeansConfig = {
    type: "kmeans.train",
    params: {
        numClusters: 2,
        metric: "euclidean",
        trainingData: "select * from gaussian",
        modelFileUrl: "file://tmp/MLDB-285.kms",
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
    plugin.log(process, "succeeded");

    if (!succeeded(response)) {
        plugin.log(process, "failed", response);
        throw process + " failed: " + JSON.stringify(response);
    }
}

var kmeansOutput = mldb.put("/v1/procedures/kmeans", kmeansConfig);
    
assertSucceeded("kmeans creation", kmeansOutput);

// Run the training
var trainingOutput = mldb.put("/v1/procedures/kmeans/runs/1", {});

assertSucceeded("kmeans training", trainingOutput);

// Dump the centroids.  There should be two, one around (1,1) and the other around (-1,-1).
var centroids = mldb.perform("GET", "/v1/query", {q : 'select * from kmeans_centroids'});
assertSucceeded("getting centroids", centroids);

assert(centroids.json.length == 2, "Centroids should have length 2");
var diff1 = centroids.json[0].columns[0][1] - centroids.json[0].columns[1][1];
var diff2 = centroids.json[1].columns[0][1] - centroids.json[1].columns[1][1];

// Centroids are along the diagonale
assert(-0.1 < diff1 && diff1 < 0.1, "first point not along the diagonale");
assert(-0.1 < diff2 && diff2 < 0.1, "second point not along the diagonale");

// Create a function to re-run the kmeans
var functionConfig = {
    type: "kmeans",
    params: {
        modelFileUrl : "file://tmp/MLDB-285.kms"
    }
};

var functionOutput = mldb.put("/v1/functions/kmeans", functionConfig);
assertSucceeded("function creation", functionOutput);

var functionApplicationOutput = mldb.get("/v1/functions/kmeans/application", { input: { embedding: {x:1,y:1 }}});
assertSucceeded("function application", functionApplicationOutput);
var clusterId = functionApplicationOutput.json.output.cluster;

functionApplicationOutput = mldb.get("/v1/functions/kmeans/application", { input: { embedding: {x:-1,y:-1 }}});
assertSucceeded("function application", functionApplicationOutput);
assert(clusterId != functionApplicationOutput.json.output.cluster, "opposite points should be in different clusters");

function applyFunction(regex) {
    var select = "select kmeans({{*} as embedding})[cluster] as cluster";
    var query = select + " from gaussian where regex_match(rowName(), '" + regex + "')" + "limit 10";
    var queryOutput = mldb.get("/v1/query", { q : query });

    var prevCluster = queryOutput.json[0].columns[0][1];
    assertSucceeded("kmeans application", queryOutput);
    for (i = 0; i < queryOutput.json.length; i++) {
        var cluster = queryOutput.json[i].columns[0][1];
        assert(prevCluster == cluster, "all embeddings on rows " + regex + " should belong to the same cluster");
    }
}

applyFunction("row0_.*");
applyFunction("row1_.*");


/* pass centroids in kmeans function ane make sure they each get themselves gack */
rez = mldb.query("select kmeans({embedding: {*}})[cluster] as cluster from kmeans_centroids");
mldb.log(rez)
for(var i=0; i<2; i++)
    assert(rez[i]["rowName"] == rez[i]["columns"][0][1])



"success"
