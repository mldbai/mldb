// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var query = "SELECT tokenize('a,b,c') AS *";
var tokQuery = "SELECT tokenize('a,b,c') AS tok";

var analysis = mldb.get("/v1/query", { q: "SELECT static_expression_info(pi())[\"info\"][isConstant] as isRow", format: 'table', headers: false, rowNames: false });

mldb.log(analysis.json);

unittest.assertEqual(analysis.json[0][0], 1);

"success"

