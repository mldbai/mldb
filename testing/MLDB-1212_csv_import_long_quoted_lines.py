#
# MLDB-1212_csv_import_long_quoted_lines.py
# 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

with open("tmp/broken_csv.csv", 'wb') as f:
    f.write("a,b\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(50)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(100)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(1000)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(10000)])+"\"\n")

csv_conf = {
    "type": "import.text",
    "params": {
        'dataFileUrl' : 'file://tmp/broken_csv.csv',
        "outputDataset": {
            "id": "x",
        },
        "runOnCreation": True,
        "ignoreBadLines": False
    }
}
mldb.put("/v1/procedures/csv_proc", csv_conf) 

result = mldb.get(
    "/v1/query",
    q="select tokenize(b, {splitchars: ' '}) as cnt "
    "from x order by rowName() ASC")
js_rez = result.json()
mldb.log(js_rez)

answers = {"2": 50, "3": 100, "4": 1000, "5": 10000}
for row in js_rez:
    assert answers[row["rowName"]] == row["columns"][0][1]

mldb.script.set_return("success")
