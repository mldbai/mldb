// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// Ensures that URL errors are not ignored when loading python scripts.

var output = mldb.post(
    "/v1/types/plugins/python/routes/run",
    { address: "http://bob.bob" });

plugin.log("output", output);
if (output.responseCode != 400)
    throw "Expected host failure when loading pluging"

"success"
