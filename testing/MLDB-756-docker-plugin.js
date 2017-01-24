// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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

/*
var pluginConfig = {
    id: 'external1',
    type: 'experimental.external',
    params: {
        setup: {
            type: 'install',
            params: {
                copy: [
                    {
                        archive: 'docker://quay.io/mldb/mldb_sample_plugin:latest',
                        match: '.*',
                        dest: '/\0'
                    }
                ]
            }
        },
        startup: {
            type: 'loadLibrary',
            params: {
                path: 'mldb_plugin.so'
            }
        }
    }
};
*/

var pluginConfig = {
    id: 'external1',
    type: 'experimental.external',
    params: {
        startup: {
            type: 'experimental.docker',
            params: {
                repo: 'quay.io/mldb/mldb_sample_plugin:latest',
                sharedLibrary: 'build/plugin.so'
            }
        }
    }
};


var resp = mldb.post('/v1/plugins', pluginConfig);

plugin.log(resp);

assertEqual(resp.responseCode, 201);
assertEqual(resp.json.status, "SamplePlugin is loaded");


var functionConfig = {
    id: 'hello',
    type: 'hello.world'
};

var resp = mldb.post('/v1/functions', functionConfig);

mldb.log(resp);

assertEqual(resp.responseCode, 201);

resp = mldb.get('/v1/query', { q: 'select hello()' , format: 'table' });

mldb.log(resp);

"success"
