# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

allGood = True

"""
there are two files with the same contents, differently encoded
mldb/testing/utf8.csv and mldb/testing/latin1.csv
contents:
Age,Nâme
12,Niçolâß
"""

result = mldb.perform("POST", "/v1/datasets", [], {
    "type": 'text.csv.tabular',
    "id": 'utf8',
    "params": {
        "dataFileUrl": 'file://mldb/testing/utf8.csv',
        "encoding": 'utf8'
    }
})
mldb.log(result)
allGood = allGood and result["statusCode"] == 201

result = mldb.perform("POST", "/v1/datasets", [], {
    "type": 'text.csv.tabular',
    "id": 'latin1',
    "params": {
        "dataFileUrl": 'file://mldb/testing/latin1.csv',
        "encoding": 'latin1'
    }
})
mldb.log(result)
allGood = allGood and  result["statusCode"] == 201

result = mldb.perform("GET", "/v1/query", 
    [["q", "select * from utf8"]])
mldb.log(result)
allGood = allGood and  result["statusCode"] == 200


result = mldb.perform("GET", "/v1/query", 
    [["q", "select * from latin1"]])
mldb.log(result)
allGood = allGood and  result["statusCode"] == 200

mldb.log(allGood)

if allGood:
    mldb.script.set_return("success")
