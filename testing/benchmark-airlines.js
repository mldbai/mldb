// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "file://airlines.csv.lz4",
        outputDataset: {
            id: "airlines",
        },
        runOnCreation: true,
        encoding: 'ascii',
        ignoreBadLines: true,
        limit: 50000000,
	    //timestamp: "CAST (Year + '-' + Month + '-' + DayofMonth) AS TIMESTAMP"
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

mldb.log("airlines import: ", res);

mldb.log(mldb.post("/v1/datasets/airlines/routes/saves",
                     {dataFileUrl: 'file://tmp/airlines.mldbds'}));

"success"

