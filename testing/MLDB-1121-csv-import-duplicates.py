#
# MLDB-1121-csv-import-duplicates.py
# Mich, 2016-01-25
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

res = mldb.put("/v1/datasets/mldb_1121", {
    "type": "text.csv.tabular",
    "params": {
        "dataFileUrl": "file://mldb/testing/MLDB-1121_test_set.csv",
        "headers": ["error_msg", "count"]
    }
})

res = mldb.get("/v1/query", q="select * from mldb_1121")

response = res.json()
lines = set()

for row in response:
    line = row['columns'][0][1]
    assert line not in lines, 'Found duplicate line: ' + line
    lines.add(line)

line0 = response[0]["columns"][0][1]

mldb.script.set_return("success")
