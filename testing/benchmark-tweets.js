// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')

var load_tweets = {
    type: "import.text",
    params: {
        dataFileUrl : "file://mldb/mldb_test_data/tweets.gz",
        outputDataset: {
            id: "tweets",
        },
        runOnCreation: true,
        delimiter: "\t",
            headers: ["a", "b", "tweet", "date"],
        select: "*",
        ignoreBadLines: true
    }
};
var res = mldb.put("/v1/procedures/csv_proc", load_tweets)

mldb.log("tweets import: ", res);

mldb.log(mldb.post("/v1/datasets/tweets/routes/saves",
                     {dataFileUrl: 'file://tmp/tweets.mldbds'}));

"success"
