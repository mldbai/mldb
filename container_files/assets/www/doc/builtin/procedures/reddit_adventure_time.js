/* Example script to import a reddit dataset and run an example */

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type: 'beh.mutable',
        id: 'reddit_dataset'
    };

    var dataset = mldb.createDataset(dataset_config)

    var dataset_address = 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var numLines = 100000;

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

    dataset.commit()

    var end = new Date();
    
    plugin.log("Created Reddit dataset for " + lineNum + " users in "
               + (end - start) / 1000 + " seconds");
    
    return dataset;
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{

    if (!succeeded(response)) {
        plugin.log(process, response);
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

// Create a SVD procedure.  We only want to embed the 1,000 columns with the
// highest row counts.

var svdConfig = {
    type: "svd",
    params: {
        dataset: { "id": "reddit_dataset" },
        output: { "id": "reddit_svd_embedding", type: "embedding" },
        select: "COLUMN EXPR (AS columnName() WHERE rowCount() > 100 ORDER BY rowCount() DESC, columnName() LIMIT 1000)"
    }
};

// 1.  Create our Reddit dataset, by importing actions of 100,000 users.
createDataset();

// 2.  Train our SVD on the Reddit dataset, embedding each of the subreddits
// in a 100 dimensional space.
createAndTrainProcedure(svdConfig, 'reddit_svd');

// 3.  Query MLDB to show what the Adventure Time embedding looks like
plugin.log(mldb.get("/v1/datasets/reddit_svd_embedding/query", {select: "svd000*", where:"rowName()='adventuretime|1'"}).json);

// 4.  Query the embedding to find similar subreddits to Adventure Time
var similar = mldb.get("/v1/datasets/reddit_svd_embedding/routes/rowNeighbours",
                       {row: 'adventuretime|1', numNeighbours:10}).json;

plugin.log(similar);

// Set the output of the script
"success"
