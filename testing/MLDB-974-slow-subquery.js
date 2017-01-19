// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

csv_conf = {
    type: "import.text",
    params: {
        dataFileUrl : "https://s3.amazonaws.com/benchm-ml--main/test.csv",
        outputDataset: {
            id: "test",
        },
        runOnCreation: true,     
    }
}

var res = mldb.put("/v1/procedures/csv_proc", csv_conf)

var before = new Date();

var resp = mldb.get('/v1/query', { q: "select min(cnt), max(cnt) from (select count(*) as cnt from test group by cast (rowName() as number)) limit 10",
                                   format: 'table' });

var after = new Date();

mldb.log(resp.json);

var timeTaken = (after - before) / 1000.0;

mldb.log("query took", timeTaken, "seconds");

if (timeTaken > 15.0)
    throw "Query took too long";

"success"



