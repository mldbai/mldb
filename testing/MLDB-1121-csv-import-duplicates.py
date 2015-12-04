# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

if False:
    mldb = None

res = mldb.perform("PUT", "/v1/datasets/mldb_1121", [], {
    "type": "text.csv.tabular",
    "params": {
        "dataFileUrl": "file://mldb/testing/MLDB-1121_test_set.csv",
        "headers": ["error_msg", "count"]
    }
})
assert res['statusCode'] == 201, str(res)

res = mldb.perform("GET", "/v1/query", [["q", "select * from mldb_1121"]])
assert res['statusCode'] == 200, str(res)

import json
response = json.loads(res['response'])
lines = set()

for row in response:
    line = row['columns'][0][1]
    assert line not in lines, 'Found duplicate line: ' + line
    lines.add(line)

line0 = response[0]["columns"][0][1]

mldb.script.set_return("success")
