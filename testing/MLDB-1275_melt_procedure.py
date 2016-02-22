
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

import json, datetime


def assertVal(res, rowName, colName, value):
    for row in res:
        if str(row["rowName"]) != rowName: continue

        for col in row["columns"]:
            if col[0] == colName:
                assert col[1] == value
                return True

        # did not find col
        assert False

    # did not find row
    assert False


#####
# test
####

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : "my_json_dataset"
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

row1 = {
    "name": "bill",
    "age": 25,
    "friends": [{"name": "mich", "age": 20}, {"name": "jean", "age": 18}]
}
dataset.record_row("row1" , [["data", json.dumps(row1), now]])

row2 = {
    "name": "alexis",
    "age": 22,
    "friends": [{"name": "cross", "age": 20},
                {"name": "fit", "age": 18},
                {"name": "foot", "region": "south"}]
}
dataset.record_row("row2" , [["data", json.dumps(row2), now]])

dataset.commit()



res = mldb.perform("GET", "/v1/query", [["q", "select parse_json(data, {arrays: 'encode'}) as * from my_json_dataset"]])
jsRes = json.loads(res["response"])
mldb.log(jsRes)


assertVal(jsRes, "row1", "age", 25)
assertVal(jsRes, "row1", "friends.1", "{\"age\":18,\"name\":\"jean\"}")


conf = {
    "id": "melter",
    "type": "melt",
    "params": {
        "inputData": """
                        SELECT {name, age} as to_fix,
                               {friends*} as to_melt
                        FROM (select parse_json(data, {arrays: 'encode'}) as * from my_json_dataset)
                        """,
        "outputDataset": {"id": "melted_dataset", "type": "sparse.mutable" },
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/melter", [], conf)
mldb.log(res)

res = mldb.perform("GET", "/v1/query", [["q", """SELECT 
                                                name, age, key, parse_json(value, {arrays: 'encode'}) as friends 
                                                FROM melted_dataset"""]])
jsRes = json.loads(res["response"])
mldb.log(jsRes)

assertVal(jsRes, "row1_friends.1", "name", "bill")
assertVal(jsRes, "row1_friends.1", "key", "friends.1")
assertVal(jsRes, "row1_friends.1", "friends.age", 18)



mldb.script.set_return("success")

