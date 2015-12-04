// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

function succeeded(response)
{
        return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
        plugin.log(process, response);

        if (!succeeded(response)){
            throw process + " failed: " + JSON.stringify(response);
        }
}


mldb.createDataset({"id": "hellô", "type":"embedding"})

assertSucceeded("", mldb.get("/v1/query", {"q": 'select * from "hellô"'}))

assertSucceeded("", mldb.get("/v1/datasets"))

assertSucceeded("", mldb.put("/v1/datasets/hôwdy", {"type": 'embedding'}))

assertSucceeded("", mldb.get("/v1/datasets"))

assertSucceeded("", mldb.post("/v1/datasets", {"type": 'embedding', "id": "hî" }))

assertSucceeded("", mldb.perform("GET", "/v1/datasets"))

"success"
