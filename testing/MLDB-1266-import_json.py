
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

import json


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

conf = {
    "id": "json_importer",
    "type": "import.json",
    "params": {
        "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
        "outputDataset": {"id": "my_json_dataset", "type": "sparse.mutable" },
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/json_importer", [], conf)
mldb.log(res)

res = mldb.perform("GET", "/v1/query", [["q", "select * from my_json_dataset order by rowName()"]])
jsRes = json.loads(res["response"])
mldb.log(jsRes)



assertVal(jsRes, "row1", "colA", 1)
assertVal(jsRes, "row1", "colB", "pwet pwet")
assertVal(jsRes, "row2", "colB", "pwet pwet 2")

assertVal(jsRes, "row3", "colC.a", 1)
assertVal(jsRes, "row3", "colC.b", 2)

assertVal(jsRes, "row4", "colD", "[{\"a\":1},{\"b\":2}]")

assertVal(jsRes, "row5", "colD.1", 1)
assertVal(jsRes, "row5", "colD.abc", 1)
assertVal(jsRes, "row5", "colD.true", 1)





conf = {
    "id": "json_importer",
    "type": "import.json",
    "params": {
        "dataFileUrl": "file://mldb/testing/dataset/json_dataset_invalid.json",
        "outputDataset": {"id": "my_json_dataset", "type": "sparse.mutable" },
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/json_importer", [], conf)
mldb.log(res)
assert res["statusCode"] == 400


conf = {
    "id": "json_importer",
    "type": "import.json",
    "params": {
        "dataFileUrl": "file://mldb/testing/dataset/json_dataset_invalid.json",
        "outputDataset": {"id": "my_json_dataset2", "type": "sparse.mutable" },
        "runOnCreation": True,
        "ignoreBadLines": True
    }
}

res = mldb.perform("PUT", "/v1/procedures/json_importer", [], conf)
mldb.log(res)
assert res["statusCode"] == 201

res = mldb.perform("GET", "/v1/query", [["q", "select * from my_json_dataset2 order by rowName()"]])
jsRes = json.loads(res["response"])
mldb.log(jsRes)

assertVal(jsRes, "row1", "colA", 1)
assertVal(jsRes, "row3", "colB", "pwet pwet 2")

mldb.script.set_return("success")
