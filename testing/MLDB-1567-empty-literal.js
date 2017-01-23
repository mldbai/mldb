/** MLDB-1562-join-with-in.js
    Jeremy Barnes, 7 April 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var expected = [
    [ "x", null, "NaD" ]
];

var resp = mldb.query('select [] as x');

mldb.log(resp);

assertEqual(resp[0].columns, expected);

resp = mldb.query('select {} as x')

mldb.log(resp);

assertEqual(resp[0].columns, undefined);

resp = mldb.get('/v1/query', {q: 'select * from transpose(select 1)'});

mldb.log(resp);

assertEqual(resp.responseCode, 400);

assertEqual(resp.json.error.indexOf("Expected to find a ')' parsing a table expression.  This is normally because of not putting a sub-SELECT within '()' characters") != -1, true, resp.json.error);

"success"
