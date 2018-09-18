// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var fnConfig = {
    type: 'sql.expression',
    params: {
        expression: 'x * 10 as y'
    }
};

var fn = mldb.createFunction(fnConfig);

unittest.assertEqual(fn.type(), 'sql.expression');

var res = fn.call({ x: 10 });

mldb.log(res);

unittest.assertEqual(res[0][0], [ "y", [ 100, '-Inf' ]]);

var procConfig = {
    type: "null",
    params: {
    }
}
 
var proc = mldb.createProcedure(procConfig);   

unittest.assertEqual(proc.type(), 'null');

var res = proc.run({});

unittest.assertEqual(res, {});

mldb.log(res);

"success"
