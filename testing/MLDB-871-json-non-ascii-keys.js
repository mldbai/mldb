// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var res = mldb.get('/v1/query', {q: "select 'ç'", format: "aos"});

mldb.log(res);

unittest.assertEqual(res.responseCode, 200);

var expected = [
    {
        "'ç'" : "ç",
        "_rowName" : "result"
    }
];

unittest.assertEqual(res.json, expected);

"success"
