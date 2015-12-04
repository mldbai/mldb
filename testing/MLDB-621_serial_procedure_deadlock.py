# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

mldb.perform("PUT", "/v1/procedures/q", [], {
    "type":"serial",
    "params": { "steps": [ {"id":"q", "type": "null"} ] }
})


mldb.script.set_return("success")