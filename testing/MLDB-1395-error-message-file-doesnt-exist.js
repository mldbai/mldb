// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var config = {
    type: "import.text",
    params: {
        dataFileUrl : "file://thisfiledoesnotexist",
        outputDataset: {
            id: "broken_fail",
        },
        runOnCreation: true,        
    }
}

var resp = mldb.put("/v1/procedures/csv_proc", config)

mldb.log(resp);

unittest.assertEqual(resp.json.details.runError.error.indexOf("No such file or directory") >= 0, true);
unittest.assertEqual(resp.json.details.runError.error.indexOf("thisfiledoesnotexist") >= 0, true);

"success"

