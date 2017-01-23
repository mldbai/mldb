
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

import json, random, datetime, os

now = datetime.datetime.now()

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'dataset1',
}
dataset = mldb.create_dataset(dataset_config)
dataset.record_row("row1", [["col1", "a", now]])
dataset.record_row("row2", [["col2", "b", now]])
dataset.commit()

dataset_config["id"] = "dataset2"
dataset = mldb.create_dataset(dataset_config)
dataset.record_row("row1", [["col1", "a", now]])
dataset.record_row("row2", [["col2", "b", now]])
dataset.commit()

dataset_config["id"] = "dataset3"
dataset = mldb.create_dataset(dataset_config)
dataset.record_row("row1", [["col1", "a", now], ["otherRow", "row1", now]])
dataset.record_row("row2", [["col2", "b", now], ["otherRow", "row2", now]])
dataset.commit()

#MLDB-1454 rownames in WHERE regular

mldb.log("MLDB-1454 rownames in WHERE regular")

expected = [["_rowName","col1"],["row1","a"]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT *
    FROM dataset1
    WHERE dataset1.col1 = 'a'
    """],['format', 'table']])

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected
##
expected = [["_rowName","col1"],["row1","a"]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT *
    FROM dataset1 as blah
    WHERE blah.col1 = 'a'
    """],['format', 'table']])

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

expected = [["_rowName","col1"],["row1","a"]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT *
    FROM dataset1
    WHERE dataset1.rowName() = 'row1'
    """],['format', 'table']])

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT *
    FROM dataset1 as blah
    WHERE blah.rowName() = 'row1'
    """],['format', 'table']])

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

expected = [[ "_rowName", '"dataset1.rowName()"' ],[ '"[""row1""]"', '"[""row1""]"' ],[ '"[""row2""]"', '"[""row2""]"' ]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT dataset1.rowName()
    FROM dataset1
    GROUP BY dataset1.rowName()
    """],['format', 'table']])

mldb.log(json.loads(res['response']))

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

expected = [[ "_rowName", "dataset1.col1" ],[ "[null]", None ],[ '"[""a""]"', "a" ]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT dataset1.col1
    FROM dataset1
    GROUP BY dataset1.col1
        """],['format', 'table']])

mldb.log(json.loads(res['response']))
mldb.log(expected)

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

#MLDB-1305 rownames in join

mldb.log("MLDB-1305 rownames in join")

expected = [["_rowName",'"dataset1.rowName()"','"dataset2.rowName()"'],["[row1]-[row1]","row1","row1"],["[row2]-[row2]","row2","row2"]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT dataset1.rowName(), dataset2.rowName()
    FROM dataset1
    JOIN dataset2 ON dataset2.rowName() = dataset1.rowName()
    """],['format', 'table']])

mldb.log(json.loads(res['response']))

expected = [["_rowName",'"dataset1.rowName()"','"dataset2.rowName()"','"dataset3.rowName()"'],
            [ "[row1]-[row1]-[row1]", "row1", "row1", "row1" ],
            [ "[row2]-[row2]-[row2]", "row2", "row2", "row2" ]]

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT dataset1.rowName(), dataset2.rowName(), dataset3.rowName()
    FROM dataset1
    JOIN dataset2 ON dataset2.rowName() = dataset1.rowName()
    JOIN dataset3 ON dataset3.rowName() = dataset1.rowName()
    """],['format', 'table']])

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

res = mldb.perform("GET", "/v1/query", [["q", """
    SELECT dataset1.rowName(), dataset2.rowName(), dataset3.rowName()
    FROM dataset1
    JOIN dataset2 ON dataset2.rowName() = dataset1.rowName()
    JOIN dataset3 ON dataset3.rowName() = dataset2.rowName()
    """],['format', 'table']])

assert res["statusCode"] == 200
assert json.loads(res['response']) == expected

conf = {
    "type": "transform",
    "params": {
        "inputData": """
    SELECT dataset1.rowName(), dataset2.rowName(), dataset3.rowName()
    NAMED dataset2.rowName()
    FROM dataset1
    JOIN dataset2 ON dataset2.rowName() = dataset1.rowName()
    JOIN dataset3 ON dataset3.rowName() = dataset1.rowName()
        """,
        "outputDataset": "output",
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/doit", [], conf)
assert res["statusCode"] == 201

mldb.script.set_return("success")
