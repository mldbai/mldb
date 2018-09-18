/** MLDB-1562-join-with-in.js
    Jeremy Barnes, 7 April 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var expected = [
    [ "x", null, "NaD" ]
];

var resp = mldb.query('select [] as x');

mldb.log(resp);

unittest.assertEqual(resp[0].columns, expected);

resp = mldb.query('select {} as x')

mldb.log(resp);

unittest.assertEqual(resp[0].columns, undefined);

resp = mldb.get('/v1/query', {q: 'select * from transpose(select 1)'});

mldb.log(resp);

unittest.assertEqual(resp.responseCode, 400);

unittest.assertEqual(resp.json.error.indexOf("Expected to find a ')' parsing a table expression.  This is normally because of not putting a sub-SELECT within '()' characters") != -1, true, resp.json.error);

"success"
