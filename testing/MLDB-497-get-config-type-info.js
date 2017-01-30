// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

plugin.log(mldb.get("/v1/typeInfo", {type:"std::string"}).json);

var vkInfo = mldb.get("/v1/typeInfo", {type:"MLDB::ValueKind"}).json;

assertEqual(vkInfo.typeName, "MLDB::ValueKind");
assertEqual(vkInfo.kind, "ENUM");

var svdInfo = mldb.get("/v1/types/procedures/svd.train/info").json;

plugin.log(svdInfo);

assertEqual(svdInfo.configType.typeName, "MLDB::SvdConfig");
assertEqual(svdInfo.configType.kind, "STRUCTURE");


"success"

