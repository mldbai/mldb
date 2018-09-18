// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

function runQuery(q, expected)
{
    var resp = mldb.query(q);
    mldb.log(resp);
    unittest.assertEqual(resp[0].columns[0][1], expected);
}

runQuery("select cast ([''] as path)", {path: [""]});
runQuery("select cast (['1'] as path)", {path: ["1"]});
runQuery("select cast ([1] as path)", {path: ["1"]});
runQuery("select cast ([1,2,3] as path)", {path: ["1","2","3"]});
runQuery("select cast ('1.2.3' as path)", {path: ["1.2.3"]});
runQuery("select cast ('\"hello.world\"' as path)", {path: ["\"hello.world\""]});

runQuery("select stringify_path([1,2,3])", "1.2.3");
runQuery("select parse_path(stringify_path([1,2,3]))", {path: ["1","2","3"]});

"success"
