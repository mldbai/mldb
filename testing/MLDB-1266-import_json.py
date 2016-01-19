
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

import json

conf = {
    "id": "json_importer",
    "type": "import.json",
    "params": {
        "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
        "output": {"id": "my_json_dataset", "type": "sparse.mutable" },
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/json_importer", [], conf)
mldb.log(res)

res = mldb.perform("GET", "/v1/query", [["q", "select * from my_json_dataset order by rowName()"]])
jsRes = json.loads(res["response"])
mldb.log(jsRes)


def assertVal(res, rowName, colName, value):
    for row in res:
        if row["rowName"] != rowName: continue

        for col in row["columns"]:
            if col[0] == colName:
                assert col[1] == value
                return True

        # did not find col
        assert False

    # did not find row
    assert False


assertVal(jsRes, "row1", "colA", 1)
assertVal(jsRes, "row1", "colB", "pwet pwet")
assertVal(jsRes, "row2", "colB", "pwet pwet 2")

assertVal(jsRes, "row3", "colC_a", 1)
assertVal(jsRes, "row3", "colC_b", 2)

assertVal(jsRes, "row4", "colD", "[{\"a\":1},{\"b\":2}]")

assertVal(jsRes, "row5", "colD_1", 1)
assertVal(jsRes, "row5", "colD_abc", 1)
assertVal(jsRes, "row5", "colD_true", 1)


mldb.script.set_return("success")
