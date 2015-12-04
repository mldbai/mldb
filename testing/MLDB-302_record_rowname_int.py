# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import datetime
import json

# create a mutable beh dataset
datasetConfig = {
    "type": "sparse.mutable",
    "id": "recordWork"
}

dataset = mldb.create_dataset(datasetConfig)


dataset.record_row("a", [["b", 5, datetime.datetime.now()]])
dataset.record_row("#hashtag", [["spoon", 4, datetime.datetime.now()]])
dataset.record_row(1, [[5, 5, datetime.datetime.now()]])
dataset.record_row("1", [[5, 5, datetime.datetime.now()]])
dataset.record_row(0, [[5, 5, datetime.datetime.now()]])
dataset.record_row("0", [[6, 6, datetime.datetime.now()]])
dataset.commit()

resp = json.loads(mldb.perform("GET", "/v1/datasets/recordWork/query", [], {})["response"])

answers = [0, "a", 1]
good = True
for row, name in zip(resp, answers):
    if row["rowName"] != name:
        good = False

if good:
    mldb.script.set_return("success")
else:
    print resp

