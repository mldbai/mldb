// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* Example script to import a reddit dataset and run an example */

var dataset_config = {
    type: 'beh.binary',
    id: 'bk',
    params: {
        dataFileUrl: "s3://prod.bluekai.datacratic.com/v2/summaries/2015/05/04/2015-05-04-00:00:00-14d-2535-362758e0a7d454a9-02_32.beh.lz4"
    }
};

var dataset = mldb.createDataset(dataset_config)

plugin.log("dataset status", mldb.get("/v1/datasets/bk"));

var resp = mldb.get("/v1/datasets/bk/query",
                    { limit: 20, format: 'full' });

plugin.log(resp.json);

var resp = mldb.get("/v1/datasets/bk/query",
                    { 'where': 'rowName() = "Qg0JzMk599eeq45D"',
                      'format': 'sparse' });

plugin.log(resp.json);

// NOTE: this should find the row...

var resp = mldb.get("/v1/datasets/bk/query",
                    { select: 'rowName()',
                      where: '"2535:419970:23765" OR "2535:419970:23766"',
                      format: 'sparse' });

plugin.log(resp.json.length);

// Should be 4105 here...

"success";