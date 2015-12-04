/* Example script to import a recipe dataset and run an example */

function createDataset()
{
    var dataset_config = {
        type    : 'beh.mutable',
        id      : 'recipes',
        address : 'tmp/recipes.beh'
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Recipe data loader created dataset")

    //https://raw.githubusercontent.com/jmcohen/taste/master/data/favorites.csv
    var dataset_address = "/home/mailletf/workspace/platform/mldb_data/receipes/favorites.csv.gz";

    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var last_id = -1;
    var tuples = [];

    function commit()
    {
        if (tuples.length == 0)
            return;
        dataset.recordRow(user, tuples);
        last_id = -1;
        tuples = [];
    }

    function record(user, recipe)
    {
        if (user != last_id) {
            commit();
            last_id = user;
        }

        tuples.push([recipe, 1, now]);
    }

    var numLines = 10000000;

    var lineNum = 0;
    while (!stream.eof() && lineNum < numLines) {
        ++lineNum;
        if (lineNum % 100000 == 0)
            plugin.log("loaded", lineNum, "lines");
        try {
            var line = stream.readLine();
            var fields = line.split(',');
            var user = fields[0];
            var recipe = fields[1];

            record(user, recipe);
        } catch (e) {
            break;
        }
    }

    commit();
    plugin.log("Committing dataset")
    dataset.commit()

    return dataset;
}

function loadDataset()
{
    var dataset_config = {
        'type'    : 'beh',
        'id'      : 'recipes',
        'address' : 'tmp/recipes.beh'
    };

    var dataset = mldb.createDataset(dataset_config)
    return dataset;
}

//var dataset = createDataset();

var dataset;
try {
    dataset = loadDataset();
} catch (e) {
    mldb.del("/v1/datasets/recipes");
    dataset = createDataset();
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
    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);
}

var svdConfig = {
    "type": "svd.train",
    "params": {
        "trainingDataset": { "id": "recipes" },
        "columnOutputDataset" : {"id" : "recipes_svd",
                    "type" : "embedding" },
        "rowOutputDataset" : {"id": "recipes_svd_embedding",
                       "type": "embedding" },
        "select" : "* EXCLUDING (label)"
    }
};

createAndTrainProcedure(svdConfig, 'recipes_svd');

var kmeansConfig = {
    "type": "kmeans.train",
    "params": {
        "trainingData": "select * from recipes_svd",
        "outputDataset": {"id" : "recipes_svd_kmeans",
                   "type" : "embedding" },
        "centroidsDataset": {"id" : "recipes_svd_kmeans_centroids",
                      "type" : "embedding" },
        "numClusters": 20
    }
};

createAndTrainProcedure(kmeansConfig, 'recipes_kmeans');

var tsneConfig = {
    "type": "tsne.train",
    "params": {
        "trainingDataset": {"id":"recipes_svd"},
        "rowOutputDataset": {"id" : "recipes_svd_tsne",
                   "type" : "embedding"},
        "numOutputDimensions": 2,
        "perplexity": 6,
        "select": "svd*",
        "where": "true"
    }
};

createAndTrainProcedure(tsneConfig, 'recipes_tsne');

mldb.del("/v1/procedures/recipes_tsne");
mldb.del("/v1/datasets/recipes_tsne_tsne");

createAndTrainProcedure(tsneConfig, 'recipes_tsne');

"success"
