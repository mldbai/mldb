// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')


var procConfig = {
    type: "createEntity",
    params: {
        kind: 'dataset',
        type: "sparse.mutable"
    }
};

var resp = mldb.put("/v1/procedures/test", procConfig);

unittest.assertEqual(resp.responseCode, 201);

resp = mldb.put("/v1/procedures/test/runs/1", {});

unittest.assertEqual(resp.responseCode, 201);

mldb.log(resp.json);

unittest.assertEqual(resp.json.status.config.type, "sparse.mutable");
unittest.assertEqual(resp.json.status.kind, "dataset");
unittest.assertEqual(resp.json.status.status.rowCount, 0);

"success"

