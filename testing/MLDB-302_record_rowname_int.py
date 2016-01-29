#
# MLDB-302_record_rowname_int.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

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

resp = mldb.get("/v1/datasets/recordWork/query", ).json()

answers = [0, "a", 1]
good = True
for row, name in zip(resp, answers):
    if row["rowName"] != name:
        good = False

if good:
    mldb.script.set_return("success")
else:
    print resp
