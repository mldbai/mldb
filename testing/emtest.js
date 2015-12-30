/* Example script to import a reddit dataset and run an example */

function assert(expr, msg)
{
    if (expr)
        return;

    throw "Assertion failure: " + msg;
}

var dataset_config = {
    'type'    : 'embedding',
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

function addExampleN(rowName, centroid, stddev)
{
    var row = [];
    var point = [];
    for (var i = 0;  i < centroid.length;  ++i) {
        point[i] =  centroid[i] + rng.normal(0, stddev[i])
        row.push([names[i], point[i], now]);
       
    }

    plugin.log(point);

    //plugin.log("normal dist = ", rng.normal());
    
    dataset.recordRow(rowName, row);

    return point;
}

var sum = [0,0];

for (var i = 0;  i < 20;  ++i)
{
    var point = addExampleN("row0_" + i, [0, 20], [5, 1]);

    for (var j = 0; j < point.length; ++j)
    {
        sum[j] += point[j] / 20.0;
    }
}

 //sum /= 20;
plugin.log("sum");
plugin.log(sum);

sum = [0,0];

for (var i = 0;  i < 20;  ++i)
{
    var point = addExampleN("row1_" + i, [0, 0], [5, 1]);
 
     for (var j = 0; j < point.length; ++j)
    {
        sum[j] += point[j] / 20.0;
    }
    

}

//sum /= 20;
plugin.log("sum");
plugin.log(sum);

plugin.log("Committing gaussian dataset")
dataset.commit()


//plugin.log(mldb.perform("GET", "/v1/datasets/gaussian/query"));


// Train the k-means algorithm

var emConfig = {
    type: "EM.train",
    params: {
        numClusters: 2,
        trainingData: "select * from gaussian",
        outputDataset: { "id": "em_output", type: "embedding" },
        centroidsDataset: { "id": "em_centroids", type: "embedding" }
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

var emOutput = mldb.put("/v1/procedures/em", emConfig);
    
assertSucceeded("em creation", emOutput);

// Run the training

plugin.log("Training")
var trainingOutput = mldb.put("/v1/procedures/em/runs/1", {});

assertSucceeded("em training", trainingOutput);

// Dump the centroids.  There should be two, one around (1,1) and the other
// around (0,0).
var centroids = mldb.perform("GET", "/v1/datasets/em_centroids/query");

//assert(centroids.length == 2, "Centroids should have length 2");


plugin.log(mldb.perform("GET", "/v1/datasets/em_centroids/query"));

// Create a function to re-run the em
var functionConfig = {
    type: "EM",
    params: {
        centroids: { id: "em_centroids" },
    }
};

var functionOutput = mldb.put("/v1/functions/em", functionConfig);

assertSucceeded("function creation", functionOutput);


var functionApplicationOutput = mldb.get("/v1/functions/em/application", { input: { embedding: {x:1,y:1}}});
assertSucceeded("function application", functionApplicationOutput);

var select = "em({{*} as embedding})[cluster]";

var queryOutput = mldb.get("/v1/datasets/gaussian/query",
                           { select: select, limit: 40 });
assertSucceeded("query application", queryOutput);

plugin.log(queryOutput, queryOutput);

"success"
