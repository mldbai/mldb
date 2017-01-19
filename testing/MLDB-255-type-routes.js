// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Test of type routes. */

var script = { source: "'hello, world from javascript'" };

var output = mldb.perform("POST", "/v1/types/plugins/javascript/routes/run",
                          [], script);

plugin.log("output", output);

if (output.json.result != "hello, world from javascript")
    throw "Expected 'hello, world from javascript' from javascript run";

// Try one with a syntax error

script = { source: "this has a syntax error" };

var output = mldb.perform("POST", "/v1/types/plugins/javascript/routes/run",
                          [], script);

plugin.log("output", output);

if (output.responseCode < 400)
    throw "Expected 400 return code from script with syntax error";


script = { source: "this.has.a.runtime.error" };

var output = mldb.perform("POST", "/v1/types/plugins/javascript/routes/run",
                          [], script);

plugin.log("output", output);

if (output.responseCode < 400)
    throw "Expected 400 return code from script with runtime error";

// Try it in Python
var script2 = { source: "mldb.script.set_return('hello, world from python')" };

var output = mldb.perform("POST", "/v1/types/plugins/python/routes/run",
                          [], script2);

plugin.log("output", output);

if (output.json.result != "hello, world from python")
    throw "Expected 'hello, world from python' from python run; got '" + output.json.result + "'";

"success"




