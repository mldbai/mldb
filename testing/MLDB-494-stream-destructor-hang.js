// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* Check that we can transpose a dataset and it returns the identity
   function.
*/

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

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{
    plugin.log(process, response);

    if (!succeeded(response)) {
        throw process + " failed: " + JSON.stringify(response);
    }
}

function createAndTrainProcedure(config, name)
{
    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);
}

function createDataset()
{
    var dataset_config = {
        type: 'sparse.mutable',
        id: 'test'
    };

    var dataset = mldb.createDataset(dataset_config)
    plugin.log("Reddit data loader created dataset")

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var numLines = 5000;

    var lineNum = 0;
    while (!stream.eof() && lineNum < numLines) {
        ++lineNum;
        if (lineNum % 100000 == 0)
            plugin.log("loaded", lineNum, "lines");
        var line = stream.readLine();
        var fields = line.split(',');
        var tuples = [];
        for (var i = 1;  i < fields.length;  ++i) {
            tuples.push([fields[i], 1, now]);
        }

        dataset.recordRow(fields[0], tuples);
    }

    plugin.log("Committing dataset")
    dataset.commit()

    return dataset;
}

var dataset = createDataset();

// Double transposition should be the identity function

var dataset2_config = {
    type: 'transposed',
    id:   'test2',
    params: {
        dataset: {
            type: 'transposed',
            params: {
                dataset: { id: 'test' }
            }
        }
    }
};

var dataset2 = mldb.createDataset(dataset2_config);

assertEqual(mldb.get('/v1/query', {q : 'select * from test order by rowHash() limit 10'}).json,
            mldb.get('/v1/query', {q : 'select * from test2 order by rowHash() limit 10'}).json,
           "query diff");

"success"
