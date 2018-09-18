// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var config = { type: "sparse.mutable" };
var dataset = mldb.createDataset(config);

mldb.log(config);
mldb.log(dataset.id());

if (config.id.indexOf("auto_") != 0)
    throw "ID should start with 'auto_' : '" + config.id + "'";

unittest.assertEqual(config.id, dataset.id());

"success"

