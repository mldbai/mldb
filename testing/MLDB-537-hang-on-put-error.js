// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// MLDB-537 test case

// Create a classifier function with an error

var classifierFunctionConfig = {
    id: "classifier",
    type: "classifier",
    params: {
        modelFileUrl: "this.file.does.not.exist.and.will.cause.an.error.cls"
    }
};

var createFunctionOutput
    = mldb.put("/v1/functions/classifier", classifierFunctionConfig);
plugin.log("classifier output", createFunctionOutput);



var probabilizerFunctionConfig = {
    id: "probabilizer",
    type: "serial",
    params: {
        steps: [
            {
                id: "classifier"
            },
            {
                id: "apply_probabilizer",
                type: "probabilizer",
                params: {
                    modelFileUrl: "file://probabilizer.json"
                }
            }
        ]
    }
};

var createFunctionOutput
    = mldb.put("/v1/functions/probabilizer", probabilizerFunctionConfig);
plugin.log("probabilizer output", createFunctionOutput);

// If the problem wasn't fixed, it would hang here


// The output of the last line of the script is returned as the result of the script,
// just like in Javscript eval
"success"
