// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// MLDB-684
// Test of external plugin functionality

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}


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
                        './virtualenv/bin/python', 'mldb/testing/MLDB-684-test-server.py'
                    ]
                }
            }
        }
    }
};

var resp = mldb.post('/v1/plugins', pluginConfig);

plugin.log(resp);

assertEqual(resp.responseCode, 201);
assertEqual(resp.json.status["a-ok"], true);

// Make sure we can call a route on the plugin

var resp2 = mldb.get('/v1/plugins/external1/routes/goodbye');

plugin.log(resp2);

assertEqual(resp2.responseCode, 200);
assertEqual(resp2.response, "goodbye");


// Make sure we can delete the plugin, which will kill the plugin

var resp3 = mldb.perform("DELETE", "/v1/plugins/external1");

plugin.log(resp3);

assertEqual(resp3.responseCode, 204);

var resp4 = mldb.get('/v1/plugins/external1/routes/goodbye');

plugin.log(resp4);

assertEqual(resp4.responseCode, 404);

plugin.log('exiting');
"success"


