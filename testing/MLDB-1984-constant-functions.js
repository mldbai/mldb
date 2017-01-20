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

var query = "SELECT tokenize('a,b,c') AS *";
var tokQuery = "SELECT tokenize('a,b,c') AS tok";

var analysis = mldb.get("/v1/query", { q: "SELECT static_expression_info(pi())[\"info\"][isConstant] as isRow", format: 'table', headers: false, rowNames: false });

mldb.log(analysis.json);

assertEqual(analysis.json[0][0], 1);

"success"

