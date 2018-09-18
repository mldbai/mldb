// This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.
// Check the require() functionality and module loading

var mldb = require('mldb')

var unittest = require('mldb/unittest')


// Verify we can list all of our modules
var modules = mldb.get('/v1/types/plugins/javascript/routes/modules');

mldb.log(modules.json);

var expected = [ "mldb", "mldb/unittest" ];
unittest.assertEqual(modules.json, expected);

// Verify we can get a documentation page about the unittest module
var response = mldb.get('/v1/types/plugins/javascript/routes/modules/mldb/unittest');

mldb.log(response);

unittest.assertEqual(typeof(response.json.assertEqual.markdown), "string");

// Verify it can be rendered to HTML

var response = mldb.get('/v1/types/plugins/javascript/routes/modules/mldb/unittest/doc.html');

mldb.log(response);

unittest.assertEqual(response.contentType, "text/html");

"success"
