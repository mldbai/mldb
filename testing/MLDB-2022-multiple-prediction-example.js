// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var functionConfig = {
    type: 'sql.expression',
    params: {
        expression: 'horizontal_sum(input) AS result',
        prepared: true,
        raw: true,
        autoInput: true
    }
};

var fn = mldb.put('/v1/functions/score_one', functionConfig);

assertEqual(fn.responseCode, 201);

mldb.log(fn);

var res = mldb.get('/v1/functions/score_one/batch',
                   { input: [[1,2,3],[4,5],[6],[]] });

mldb.log(res);

var expected = [ 6, 9, 6, 0 ];

assertEqual(res.json, expected);

var functionConfig = {
    type: 'sql.query',
    params: {
        query: 'select horizontal_sum(value) as value, column FROM row_dataset($input)',
        output: 'NAMED_COLUMNS'
    }
};

var fn = mldb.put('/v1/functions/score_many', functionConfig);

mldb.log(fn);

assertEqual(fn.responseCode, 201);

var functionConfig2 = {
    type: 'sql.expression',
    params: {
        expression: 'score_many({input: rowsToScore})[output] AS *',
        prepared: true
    }
};

var fn2 = mldb.put('/v1/functions/scorer', functionConfig2);

mldb.log(fn2);

assertEqual(fn2.responseCode, 201);

var input = { rowsToScore: [ { x: 1, y: 2}, {a: 2, b: 3, c: 4} ] };

var res = mldb.get('/v1/functions/scorer/application',
                   { input: input, outputFormat: 'json' });

var expected = [ 3, 9 ];

assertEqual(res.json, expected);

mldb.log(res);

var functionSource = `
var fnconfig = {
    type: "sql.expression",
    params: {
        expression: "horizontal_sum({*}) AS result",
        prepared: true
   }
};
var predictfn = mldb.createFunction(fnconfig);

function handleRequest(relpath, verb, resource, params, payload, contentType, contentLength,
                       headers)
{
    if (verb == "GET" && relpath == "/predict") {
        mldb.log(params);
        if (params[0][0] != "rowsToScore")
            throw "Unknown parameter name " + params[0][0];
        var allParams = JSON.parse(params[0][1]);
        for (p in allParams) {
            allParams[p] = predictfn.callJson(allParams[p])['result'];
        }
        return allParams;
    }
    throw "Unknown route " + verb + " " + relpath;
}

plugin.setRequestHandler(handleRequest);
`;

var pluginConfig = {
    type: 'javascript',
    params: {
        source: {
            main: functionSource
        }
    }
};

var pl = mldb.put('/v1/plugins/myapi', pluginConfig);

mldb.log(pl);

var res = mldb.get('/v1/plugins/myapi/routes/predict',
                   input);

mldb.log(res);

assertEqual(res.json, expected);

"success"
