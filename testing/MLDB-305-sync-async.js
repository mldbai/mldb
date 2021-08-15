// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of sync / async functionality. */

var mldb = require('mldb')
var unittest = require('mldb/unittest')

// Do non-trivial work as we don't want trival creation to race with returning
// the first status call

var config = {
    "type": "python",
    "params":{
        "source":{
            "routes": "from mldb import mldb\nmldb.log(str(request.rest_params))\nmldb.log(str(request.payload))\nrequest.set_return({'args': request.rest_params, 'payload': request.payload})"
        }
    }
}

var output = mldb.put("/v1/plugins/test1", config);

var output2 = mldb.putAsync("/v1/plugins/test2", config);

var output3 = mldb.put("/v1/plugins/test3", config, {async:true});

var output4 = mldb.put("/v1/plugins/test4", config, {Async:true});

unittest.assertEqual(output.json.state, "ok", "test1 " + JSON.stringify(output.json));
unittest.assertEqual(output2.json.state, "initializing", "test2 " + JSON.stringify(output2.json));
unittest.assertEqual(output3.json.state, "initializing", "test3 " + JSON.stringify(output3.json));
unittest.assertEqual(output4.json.state, "initializing", "test4 " + JSON.stringify(output4.json));

// TODO if we remove these following lines, the test fails sometimes as a race between
// shutdown and initialization

while (mldb.get("/v1/plugins/test2").json.state == "initializing") ;
while (mldb.get("/v1/plugins/test3").json.state == "initializing") ;
while (mldb.get("/v1/plugins/test4").json.state == "initializing") ;


"success"
