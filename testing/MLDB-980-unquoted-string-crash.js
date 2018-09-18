// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var scriptConfig = {
    address: "file://mldb/testing/MLDB-980-inner-script.js"
};

var res = mldb.post('/v1/types/plugins/javascript/routes/run', scriptConfig);

mldb.log(res);

unittest.assertEqual(res.responseCode, 400);
unittest.assertEqual(res.json.exception.message, "Uncaught SyntaxError: Invalid or unexpected token");

"success"
