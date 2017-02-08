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

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
}

var query = ""
    + "SELECT 'this is an unclosed string \n"
    + "        with lots of lines \n"
    + "        but no closing quote";

var resp = mldb.get('/v1/query', { q: query });

mldb.log(resp);

assertEqual(resp.responseCode, 400);
assertContains(resp.json.error, "1:9", "Error message did not contain correct location");

var query2 = ""
    + 'SELECT "this is an unclosed identifier \n'
    + "        with lots of lines \n"
    + "        but no closing quote";

var resp2 = mldb.get('/v1/query', { q: query2 });

mldb.log(resp2);

assertEqual(resp2.responseCode, 400);
assertContains(resp2.json.error, "1:9", "Error message did not contain correct location");

"success"

