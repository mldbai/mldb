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
    plugin.log(process, response.json);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

res = mldb.put('/v1/procedures/import_reddit', { 
    "type": "import.text",  
    "params": { 
        "dataFileUrl": "http://public.mldb.ai/reddit.csv.gz",
        'delimiter':'', 
        'quotechar':'',
        'outputDataset': 'reddit_raw',
        'runOnCreation': true,
        'limit': 10000
    } 
});

assertSucceeded("import text", res);

res = mldb.query("select * from reddit_raw limit 5");

mldb.log(res);

res = mldb.put('/v1/procedures/reddit_import', {
    "type": "transform",
    "params": {
        "inputData": "select tokenize(lineText, {offset: 1, value: 1}) as * from reddit_raw",
        "outputDataset": "reddit_dataset",
        "runOnCreation": true
    }
});

assertSucceeded("tokenize", res);

res = mldb.put('/v1/procedures/reddit_count_users', {
    "type": "transform",
    "params": {
        "inputData": "select columnCount() as numUsers named rowName() + '|1' from transpose(reddit_dataset)",
        "outputDataset": "reddit_user_counts",
        "runOnCreation": true
    }
});

assertSucceeded("user counts", res);

res = mldb.query("select * from reddit_dataset limit 5");

mldb.log(res);

res = mldb.put('/v1/procedures/reddit_svd', {
    "type" : "svd.train",
    "params" : {
        "trainingData" : "SELECT COLUMN EXPR (AS columnName() ORDER BY rowCount() DESC, columnName() LIMIT 1000) FROM reddit_dataset",
        "columnOutputDataset" : "reddit_svd_embedding",
        "runOnCreation": true
    }
});

assertSucceeded("svd", res);

res = mldb.query("select * from reddit_svd_embedding limit 5");

mldb.log(res);

res = mldb.put('/v1/procedures/reddit_kmeans', {
    "type" : "kmeans.train",
    "params" : {
        "trainingData" : "select * from reddit_svd_embedding",
        "outputDataset" : "reddit_kmeans_clusters",
        "numClusters" : 20,
        "runOnCreation": true
    }
});

assertSucceeded("kmeans", res);

res = mldb.query("select * from reddit_kmeans_clusters limit 5");

mldb.log(res);

res = mldb.put('/v1/procedures/reddit_tsne', {
    "type" : "tsne.train",
    "params" : {
        "trainingData" : "select * from reddit_svd_embedding",
        "rowOutputDataset" : "reddit_tsne_embedding",
        "runOnCreation": true
    }
});

assertSucceeded("tsne", res);

mldb.log(res.json);

res = mldb.query("select * from reddit_tsne_embedding limit 5");

mldb.log(res);

res = mldb.query("select * from reddit_user_counts limit 5");

mldb.log(res);

res = mldb.query(
    "select *, quantize(x, 7) as grid_x, quantize(y, 7) as grid_y " +
    "named regex_replace(rowName(), '\|1', '') " +
    "from merge(reddit_user_counts, reddit_tsne_embedding, reddit_kmeans_clusters) " +
    "where cluster is not null " +
    "order by numUsers desc limit 100"
);

mldb.log(res);

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

