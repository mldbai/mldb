// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

plugin.log(mldb.get("/v1/typeInfo", {type:"std::string"}).json);

var vkInfo = mldb.get("/v1/typeInfo", {type:"MLDB::ValueKind"}).json;

unittest.assertEqual(vkInfo.typeName, "MLDB::ValueKind");
unittest.assertEqual(vkInfo.kind, "ENUM");

var svdInfo = mldb.get("/v1/types/procedures/svd.train/info").json;

plugin.log(svdInfo);

unittest.assertEqual(svdInfo.configType.typeName, "MLDB::SvdConfig");
unittest.assertEqual(svdInfo.configType.kind, "STRUCTURE");


"success"

