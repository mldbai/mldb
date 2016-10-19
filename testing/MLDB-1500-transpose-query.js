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
    var start = new Date();

    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);

    var end = new Date();

    plugin.log("procedure " + name + " took " + (end - start) / 1000 + " seconds");
}

function createDataset()
{
    var start = new Date();

    var datasetConfig = {
        type: 'import.text',
        params: {
            dataFileUrl: 'http://public.mldb.ai/reddit.csv.gz',
            outputDataset: { id: 'reddit_text_file' },
            limit: 1000,
            delimiter: "",
            quoteChar: ""
        }
    };

    var now = new Date();

    createAndTrainProcedure(datasetConfig, "dataset load");

    var end = new Date();
    
    plugin.log("creating text dataset took " + (end - start) / 1000 + " seconds");

    var transformConfig = {
        type: "transform",
        params: {
            inputData: { 
                select: "tokenize(lineText) AS *",
                from: 'reddit_text_file'
            },
            outputDataset: { type: 'sparse.mutable', id: 'reddit' }
        }
    };

    createAndTrainProcedure(transformConfig, "dataset import");
}

createDataset();

res = mldb.get('/v1/query', { q: 'select sum(horizontal_count({*})) as width from transpose(reddit) group by rowName() order by sum(horizontal_count({*})) desc, rowName() limit 2' });

//Check that we get largest value first and second largest second
expected = [

      {
         "columns" : [
            [ "width", 780, "2016-08-09T16:46:52Z" ]
         ],
         "rowName" : "\"[\"\"AskReddit\"\"]\""
      },
      {
         "columns" : [
            [ "width", 757, "2016-08-09T16:46:52Z" ]
         ],
         "rowName" : "\"[\"\"funny\"\"]\""
      }
   ];

mldb.log(res)

assertEqual(mldb.diff(expected, res.json, false /* strict */), {},
            "output was not the same as expected output in batch executor desc");

res = mldb.get('/v1/query', { q: 'select sum(horizontal_count({*})) as width from transpose(reddit) group by rowName() order by sum(horizontal_count({*})) asc, rowName() limit 2' });

//Check that we get smallest value first and second smallest second
expected = [
    {
        "columns" : [
            [ "width", 1, "2016-08-09T16:46:52Z" ]
        ],
        "rowName" : "\"[\"\"1000\"\"]\""
    },
    {
        "columns" : [
            [ "width", 1, "2016-08-09T16:46:52Z" ]
        ],
        "rowName" : "\"[\"\"1000words\"\"]\""
    }
];

mldb.log(res)

assertEqual(mldb.diff(expected, res.json, false /* strict */), {},
            "output was not the same as expected output in batch executor asc");

// now with the pipeline executor
mldb.put('/v1/functions/bop', {
    'type': 'sql.query',
    'params': {
        'query': 'select rowName(), sum(horizontal_count({*})) as width from transpose(reddit) group by rowName() order by sum(horizontal_count({*})) desc, rowName() limit 2'
    }
})

res = mldb.get('/v1/query', {q: 'select bop()', format: 'table'});

//check that we get biggest value, should be the same as in the batch executor
expected = [
      [ "_rowName", "bop().rowName()", "bop().width" ],
      [ "result", "[\"AskReddit\"]", 780 ]
   ]


mldb.log(res)

assertEqual(mldb.diff(expected, res.json, false /* strict */), {},
            "output was not the same as expected output in pipeline executor");

// now with the pipeline executor
mldb.put('/v1/functions/bop2', {
    'type': 'sql.query',
    'params': {
        'query': 'select rowName(), sum(horizontal_count({*})) as width from transpose(reddit) group by rowName() order by sum(horizontal_count({*})) asc, rowName() limit 2'
    }
})

res = mldb.get('/v1/query', {q: 'select bop2()', format: 'table'});

//check that we get smallest value, should be the same (value) as in the batch executor
expected = [
    [ "_rowName", "bop2().rowName()", "bop2().width" ],
    [ "result", "[\"1000\"]", 1 ]
];

mldb.log(res)

assertEqual(mldb.diff(expected, res.json, false /* strict */), {},
            "output was not the same as expected output in pipeline executor");

//check with non aggregator expression in order by in the presence of a group by
//should return an error
res = mldb.get('/v1/query', { q: 'select sum(horizontal_count({*})) as width from transpose(reddit) group by rowName() order by horizontal_count({*}) asc limit 2' });

mldb.log(res.json.error)

assertEqual(res.json.error, "Non-aggregator 'horizontal_count({*})' with GROUP BY clause is not allowed",
            "Did not get the expected error");

"success"

