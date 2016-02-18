// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(expr, 'IS NOT EQUAL TO', val);

    throw "Assertion failure";
}

var resp = mldb.post("/v1/datasets", { type: "text.csv.tabular", params: { dataFileUrl: "file://thisfiledoesnotexist" } });

mldb.log(resp);

assertEqual(resp.json.error, "Opening file ./thisfiledoesnotexist: failed opening file: No such file or directory");

"success"

