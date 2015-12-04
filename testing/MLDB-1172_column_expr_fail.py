# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json, datetime


dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}


dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

dataset.record_row("rowA", [["feat1", 1, now],
                            ["feat2", 1, now],
                            ["feat3", 1, now]])
dataset.record_row("rowB", [["feat1", 1, now],
                            ["feat2", 1, now]]),
dataset.record_row("rowC", [["feat1", 1, now]])

dataset.commit()


res = mldb.perform("GET", "/v1/query", [["q", "select COLUMN EXPR (ORDER BY rowCount() DESC LIMIT 2) from toy"]])
mldb.log(res)

assert res["statusCode"] != 400

mldb.script.set_return("success")

