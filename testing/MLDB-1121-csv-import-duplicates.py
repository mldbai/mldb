#
# MLDB-1121-csv-import-duplicates.py
# Mich, 2016-01-25
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://mldb/testing/MLDB-1121_test_set.csv',
                "outputDataset": {
                    "id": "mldb_1121",
                },
                "runOnCreation": True,
                "headers": ["error_msg", "count"]
            }
        }
mldb.put("/v1/procedures/csv_proc", csv_conf) 

res = mldb.get("/v1/query", q="select * from mldb_1121")

response = res.json()

mldb.log(response)

lines = set()

for row in response:
    line = row['columns'][1][1]
    assert line not in lines, 'Found duplicate line: ' + line
    lines.add(line)

line0 = response[0]["columns"][0][1]

mldb.script.set_return("success")
