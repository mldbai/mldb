// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// MLDB-684
// Test of external plugin functionality

var mldb = require('mldb')
var unittest = require('mldb/unittest')
var python_executable = mldb.getPythonExecutable();

var pluginConfig = {
    id: 'external1',
    type: 'experimental.external',
    params: {
        startup: {
            type: 'subprocess',
            params: {
                command: {
                    commandLine: [
                        /*'/usr/bin/strace', '-ftttT', '-o', 'test_server.strace', */
                        python_executable, 'mldb/testing/MLDB-684-test-server.py'
                    ]
                }
            }
        }
    }
};

var resp = mldb.post('/v1/plugins', pluginConfig);

plugin.log(resp);

unittest.assertEqual(resp.responseCode, 201);
unittest.assertEqual(resp.json.status["a-ok"], true);

// Make sure we can call a route on the plugin

var resp2 = mldb.get('/v1/plugins/external1/routes/goodbye');

plugin.log(resp2);

unittest.assertEqual(resp2.responseCode, 200);
unittest.assertEqual(resp2.response, "goodbye");


// Make sure we can delete the plugin, which will kill the plugin

var resp3 = mldb.perform("DELETE", "/v1/plugins/external1");

plugin.log(resp3);

unittest.assertEqual(resp3.responseCode, 204);

var resp4 = mldb.get('/v1/plugins/external1/routes/goodbye');

plugin.log(resp4);

unittest.assertEqual(resp4.responseCode, 404);

plugin.log('exiting');
"success"


