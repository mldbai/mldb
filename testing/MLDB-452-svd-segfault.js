// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

var datasetConfig = {
    "type": "beh",
    "id": "tweets_dataset",
    "address": "/home/atremblay/workspace/platform/tweets_dataset.beh.gz"
};

var dataset = mldb.createDataset(datasetConfig);


svd_config = {
    'type' : 'svd.train',
    'params' :
    {
        "trainingDataset": {"id": "tweets_dataset"},
        "columnOutputDataset": {
            "type": "beh.mutable",
            "id": "svd_tweets_col",
            "address": "svd_tweets_col.beh.gz"
        },
        "rowOutputDataset": {
            "id": "svd_tweets_row",
            'type': "embedding",
            'address' : "svd_tweets_row.beh.gz"
        },
        "numSingularValues": 1000,
        "numDenseBasisVectors": 20,
        "select": "*"
    }
};

var resp = mldb.put('/v1/procedures/svd_tweets', svd_config);

var resp2 = mldb.put('/v1/procedures/svd_tweets/runs/1', {});

"success"

