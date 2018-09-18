// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var types = [ 'procedures', 'functions', 'datasets', 'plugins' ];

for (var i = 0;  i < types.length;  ++i) {
    var resp = mldb.get('/v1/types/' + types[i], { details: true });
    unittest.assertEqual(resp.responseCode, 200);
}

"success"
