// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

csv_conf = {
    type: "import.text",
    params: {
        //dataFileUrl : "file://yellow_tripdata_2020-01.csv",
        //dataFileUrl : "file://yellow_tripdata_2020-01.csv.lz4",
        dataFileUrl : "file://yellow_tripdata_2020-01.csv.zst",
        //dataFileUrl : "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv",
        //dataFileUrl : "s3://nyc-tlc/trip+data/yellow_tripdata_2020-01.csv",
        outputDataset: {
            id: "taxis",
        },
        runOnCreation: true,
        encoding: 'ascii',
	    timestamp: "tpep_pickup_datetime"
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

mldb.log("cities import: ", res);

mldb.log(mldb.post("/v1/datasets/taxis/routes/saves",
                     {dataFileUrl: 'file://tmp/taxis.mldbds'}));

"success"

