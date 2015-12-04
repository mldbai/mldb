# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

allGood = True

mldb.create_dataset({"id": "hellô", "type":"embedding"})

result = mldb.perform("GET", "/v1/query", [["q", "select * from \"hellô\""]])
mldb.log(result)
allGood = allGood and result["statusCode"] == 200

result = mldb.perform("GET", "/v1/datasets")
mldb.log(result)
allGood = allGood and result["statusCode"] == 200

result = mldb.perform("PUT", "/v1/datasets/hôwdy", [], 
    {"type": 'embedding'})
mldb.log(result)
allGood = allGood and result["statusCode"] == 201

result = mldb.perform("GET", "/v1/datasets")
mldb.log(result)
allGood = allGood and result["statusCode"] == 200

result = mldb.perform("POST", "/v1/datasets", [], {
    "type": 'embedding',
    "id": "hî"
})
mldb.log(result)
allGood = allGood and result["statusCode"] == 201

result = mldb.perform("GET", "/v1/datasets")
mldb.log(result)
allGood = allGood and result["statusCode"] == 200

if allGood:
    mldb.script.set_return("success")