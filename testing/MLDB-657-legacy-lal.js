// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* test of legacy lal model loading */

// LATER - load as an external binary plugin
/*
var pluginConfig = {
    id: 'legacyLal',
    type: 'sharedLibrary',
    params: {
        address: "mldb_legacy_plugins",
        apiVersion: "1.0.0",
        allowInsecureLoading: true
    }
};

var resp = mldb.post("/v1/plugins", pluginConfig);

plugin.log(resp);


*/

if (false) {

    function createTestDataset()
    {
        try {
            var dataset_config = {
                type    : 'beh',
                id      : 'legacy_behs',
                params: { dataFileUrl: "file://tmp/MLDB-657.beh" }
            };

            return mldb.createDataset(dataset_config);
        } catch (e) {
        }
        
        var dataset_config = {
            type    : 'beh.mutable',
            id      : 'legacy_behs',
            params: { dataFileUrl: "file://tmp/MLDB-657.beh" }
        };

        var dataset = mldb.createDataset(dataset_config);

        //var dataset_address = 'file://mldb/testing/simons-test.tsv.gz';
        var dataset_address = 'file://simons-test-20000.tsv.gz';

        var stream = mldb.openStream(dataset_address);

        var lineNum = 0;

        var currentUid;

        var tuples = [];

        function flush()
        {
            if (tuples.length == 0)
                return;
            
            dataset.recordRow(currentUid, tuples);
            tuples = [];
        }

        try {
            while (!stream.eof()) {
                ++lineNum;
                if (lineNum % 100000 == 0)
                    plugin.log("loaded", lineNum, "lines");
                var line = stream.readLine();
                if (line.length == 0)
                    break;
                var fields = line.split('\t');

                var uid = fields[0];
                var site = fields[1];
                var ts = new Date(fields[2] * 1000);

                if (uid != currentUid)
                    flush();
                currentUid = uid;
                tuples.push([site, 1, ts]);
            }
        } catch (e) {
        }

        flush();

        dataset.commit()

        return dataset;
    }

    var dataset = createTestDataset();

    var functionConfig = {
        id: 'legacy_lal',
        type: 'legacy.lal',
        params: {
            modelUri: "file://mldb/testing/assets/simons-280_283.bin.lz4",
            modelFileUrl: "file://mldb/testing/assets/ss.svd.lz4"
        }
    };

    var resp = mldb.post("/v1/functions", functionConfig);

    plugin.log(resp);

    var resp2 = mldb.get("/v1/functions/legacy_lal/info");

    //plugin.log(resp2.json);


    for (var i = 0;  i < 10;  ++i) {

        var before = new Date();
        var resp3 = mldb.get("/v1/datasets/legacy_behs/query",
                             {select: 'APPLY FUNCTION legacy_lal WITH (object(SELECT *) AS behs, rowName() AS id, now() AS latest) EXTRACT (score,prob,index)',
                              //orderBy: 'score',
                              format: 'table'});
        var after = new Date();

        plugin.log("scored " + resp3.json.length + " in " + (after - before) + " at "
                   + resp3.json.length / (after - before) * 1000 + " per second");
    }

    //var resp3 = mldb.get("/v1/functions/legacy_lal/application",
    //                     {input: {id: 'hello', behs: { }, latest: new Date() }});

    //plugin.log(resp3);

    "success"
}



var functionConfig = {
    "type": "legacy.lal",
    "params": {
        "svdModelFileUrl": "s3://xxx/yyy.svd.lz4",
        "classifierModelFileUrl": "s3://xxx/102-27587.bin.lz4"
    }
};



var resp = mldb.put("/v1/functions/score27587", functionConfig);

mldb.log(resp);

var datasetConfig = {
    id: 'toScore',
    "type" : "merged",
    "params" : {
        "datasets" : [
            {
                "id" : "toScorePart1",
                "type": "beh.binary",
                "params": { 
                    "dataFileUrl": "s3://xxx/yyy-14d-0-3414994d7ab46d54-00_32.beh.lz4" 
                }              
            },
            {
                "id" : "toScorePart2",
                "type": "beh.binary",
                "params": { 
                    "dataFileUrl": "s3://xxx/yyy-14d-2716-8dd7fa45e8f1621f-00_32.beh.lz4" 
                }              
            }
        ]
    }
};

var ds = mldb.createDataset(datasetConfig);

var resp = mldb.get('/v1/query', { q: "SELECT score27587({behs: {*}, id: rowName(), latest: now()})[ {score, prob, index} ] as * FROM toScore WHERE rowHash() < 1000000 LIMIT 10" });

mldb.log(resp);
